"""Streaming N-Triples to Parquet conversion.

DBpedia ships its dumps as `.ttl.bz2`, but the content is one full-IRI triple per
line (N-Triples syntax) plus `#` comment lines, so a line parser is enough and is
an order of magnitude faster than rdflib at this volume. Lines are parsed in
chunks with vectorised polars regexes; only literals that contain a backslash
take the slow Python unescape path.

Output columns: s, p, o, o_is_iri, lang, datatype. IRIs are stored without angle
brackets and already normalised (see iri.normalize_iri). Blank nodes keep their
`_:` prefix.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import polars as pl
import pyarrow as pa

from .iri import normalize_iri
from .util import ParquetAppender, iter_line_chunks, log

SCHEMA = pa.schema(
    [
        ("s", pa.string()),
        ("p", pa.string()),
        ("o", pa.string()),
        ("o_is_iri", pa.bool_()),
        ("lang", pa.string()),
        ("datatype", pa.string()),
    ]
)

_TRIPLE_RE = r"^\s*(<[^>]*>|_:\S+)\s+<([^>]*)>\s+(.*?)\s*\.\s*$"
_LITERAL_RE = r'^"(.*)"(?:@([A-Za-z][A-Za-z0-9\-]*)|\^\^<([^>]*)>)?$'

_ESCAPE_RE = re.compile(r"\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|[tbnrf\"'\\])")
_SIMPLE = {"t": "\t", "b": "\b", "n": "\n", "r": "\r", "f": "\f", '"': '"', "'": "'", "\\": "\\"}


def unescape_literal(s: str) -> str:
    """Decode N-Triples string escapes."""
    if "\\" not in s:
        return s

    def repl(m: re.Match[str]) -> str:
        code = m.group(1)
        if code[0] in "uU":
            return chr(int(code[1:], 16))
        return _SIMPLE[code]

    return _ESCAPE_RE.sub(repl, s)


def _needs_norm(expr: pl.Expr) -> pl.Expr:
    return expr.str.contains("%") | expr.str.starts_with("https://") | expr.str.ends_with("/")


def _normalise_column(df: pl.DataFrame, col: str, mask: pl.Expr) -> pl.DataFrame:
    """Apply normalize_iri only to rows that can change; the rest are already canonical."""
    sel = mask & _needs_norm(pl.col(col))
    if df.select(sel.any()).item():
        return df.with_columns(
            pl.when(sel)
            .then(pl.col(col).map_elements(normalize_iri, return_dtype=pl.String))
            .otherwise(pl.col(col))
            .alias(col)
        )
    return df


def parse_lines(lines: list[str]) -> pl.DataFrame:
    """Parse a chunk of N-Triples lines into a DataFrame with the SCHEMA columns."""
    df = pl.DataFrame({"line": lines})
    df = df.filter(
        ~pl.col("line").str.strip_chars().str.starts_with("#") & (pl.col("line").str.strip_chars() != "")
    )
    if df.height == 0:
        return pl.DataFrame(
            schema={
                "s": pl.String,
                "p": pl.String,
                "o": pl.String,
                "o_is_iri": pl.Boolean,
                "lang": pl.String,
                "datatype": pl.String,
            }
        )

    parts = df.select(pl.col("line").str.extract_groups(_TRIPLE_RE).alias("g")).unnest("g")
    parts = parts.rename({"1": "s_raw", "2": "p", "3": "o_raw"})
    bad = parts.filter(pl.col("s_raw").is_null())
    if bad.height:
        first_bad = df.filter(
            pl.col("line").str.extract_groups(_TRIPLE_RE).struct.field("1").is_null()
        ).get_column("line")[0]
        log.warning("skipping %d unparsable lines (first: %r)", bad.height, first_bad[:120])
    parts = parts.filter(pl.col("s_raw").is_not_null())

    parts = parts.with_columns(
        pl.when(pl.col("s_raw").str.starts_with("<"))
        .then(pl.col("s_raw").str.slice(1, pl.col("s_raw").str.len_chars() - 2))
        .otherwise(pl.col("s_raw"))
        .alias("s"),
        pl.col("o_raw").str.starts_with("<").alias("o_is_iri"),
    )
    lit = parts.select(pl.col("o_raw").str.extract_groups(_LITERAL_RE).alias("g")).unnest("g")
    lit = lit.rename({"1": "lit_value", "2": "lang", "3": "datatype"})
    parts = parts.hstack(lit)

    parts = parts.with_columns(
        pl.when(pl.col("o_is_iri"))
        .then(pl.col("o_raw").str.slice(1, pl.col("o_raw").str.len_chars() - 2))
        .when(pl.col("lit_value").is_not_null())
        .then(pl.col("lit_value"))
        .otherwise(pl.col("o_raw"))  # blank node or unparsed literal: keep verbatim
        .alias("o"),
    )
    # Unescape literal values that contain escapes (slow path, minority of rows).
    esc = (~pl.col("o_is_iri")) & pl.col("o").str.contains("\\", literal=True)
    if parts.select(esc.any()).item():
        parts = parts.with_columns(
            pl.when(esc)
            .then(pl.col("o").map_elements(unescape_literal, return_dtype=pl.String))
            .otherwise(pl.col("o"))
            .alias("o")
        )
    # IRIs may carry \uXXXX escapes (N-Triples allows them inside <...>); decode before normalising.
    for col in ("s", "p", "o"):
        esc_iri = pl.col(col).str.contains("\\u", literal=True) | pl.col(col).str.contains(
            "\\U", literal=True
        )
        if col == "o":
            esc_iri = esc_iri & pl.col("o_is_iri")
        if parts.select(esc_iri.any()).item():
            parts = parts.with_columns(
                pl.when(esc_iri)
                .then(pl.col(col).map_elements(unescape_literal, return_dtype=pl.String))
                .otherwise(pl.col(col))
                .alias(col)
            )
    parts = _normalise_column(parts, "s", pl.col("s").str.starts_with("http"))
    parts = _normalise_column(parts, "p", pl.lit(True))
    parts = _normalise_column(parts, "o", pl.col("o_is_iri"))
    parts = parts.with_columns(
        pl.when(pl.col("o_is_iri"))
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(pl.col("lang"))
        .alias("lang"),
        pl.when(pl.col("o_is_iri"))
        .then(pl.lit(None, dtype=pl.String))
        .otherwise(pl.col("datatype"))
        .alias("datatype"),
    )
    return parts.select("s", "p", "o", "o_is_iri", "lang", "datatype")


def iter_triples(path: Path, chunk_lines: int = 500_000) -> Iterator[pl.DataFrame]:
    for lines in iter_line_chunks(path, chunk_lines):
        yield parse_lines(lines)


def convert_to_parquet(src: Path, dst: Path, chunk_lines: int = 500_000, sample_iris: int = 1000) -> dict:
    """Convert one N-Triples file to Parquet. Returns stats including a sample of subject IRIs for drift checks."""
    rows = 0
    sample: list[str] = []
    with ParquetAppender(dst, SCHEMA) as w:
        for df in iter_triples(src, chunk_lines):
            w.write(df)
            rows += df.height
            if len(sample) < sample_iris and df.height:
                sample.extend(df.get_column("s").head(sample_iris - len(sample)).to_list())
    log.info("converted %s -> %s (%d triples)", src.name, dst.name, rows)
    return {"triples": rows, "sample_iris": sample}


def format_iri(iri: str) -> str:
    return f"<{iri}>"


def format_literal(value: str, lang: str | None = None, datatype: str | None = None) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    if lang:
        return f'"{escaped}"@{lang}'
    if datatype:
        return f'"{escaped}"^^<{datatype}>'
    return f'"{escaped}"'
