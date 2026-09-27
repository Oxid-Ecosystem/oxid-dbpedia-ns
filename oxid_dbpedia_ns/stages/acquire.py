"""Stage 1: download every pinned input, verify checksums, convert to Parquet once.

Outputs (under cache/):
  downloads/<file>                   raw dumps (resumable downloads)
  parquet/<key>.parquet              triples: s, p, o, o_is_iri, lang, datatype
  parquet/sameas_all_wikis.parquet   q, lang, title  (one row per Wikipedia chapter article)
  pageviews/<YYYY-MM>.parquet        lang, title, views (user traffic, all access methods summed)
                                     (the sum over months happens in stage 4, restricted to the
                                     titles it needs; summing all ~1e9 rows here does not fit in RAM)
  iri_samples/<key>.txt              1,000 subject IRIs per file, for the normalisation drift check
  sources.lock.json                  url, size, sha256 per input as observed
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path
from urllib.parse import urlparse

import polars as pl
import pyarrow as pa
import requests

from ..config import Config
from ..iri import normalize_iri
from ..ntriples import convert_to_parquet
from ..util import (
    ParquetAppender,
    iter_line_chunks,
    log,
    read_json,
    sha256_file,
    timed,
    write_json,
    write_parquet_atomic,
)

SAMEAS_SCHEMA = pa.schema([("q", pa.string()), ("lang", pa.string()), ("title", pa.string())])
_SAMEAS_RE = (
    r"^<http://wikidata\.dbpedia\.org/resource/(Q\d+)> <http://www\.w3\.org/2002/07/owl#sameAs> "
    r"<http://(?:([a-z][a-z0-9\-]*)\.)?dbpedia\.org/resource/([^>]*)> \.\s*$"
)
NON_WIKIPEDIA_HOSTS = {"commons", "wikidata", "species", "meta", "global", "wikimedia"}


# --- downloads -----------------------------------------------------------------


# Wikimedia (and the Databus) refuse the default python-requests agent with a 403; their policy
# asks for a descriptive User-Agent with a contact URL. Override with [sources] user_agent.
DEFAULT_USER_AGENT = "oxid-dbpedia-ns/0.1 (+https://github.com/Oxid-Ecosystem/oxid-dbpedia-ns)"
_USER_AGENT = DEFAULT_USER_AGENT


def _download(url: str, dst: Path, expected_sha256: str = "", retries: int = 5) -> dict:
    """Download url to dst with HTTP range resume. Local file:// URLs are copied."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(url)
    if parsed.scheme == "file":
        src = Path(parsed.path)
        if not dst.exists() or dst.stat().st_size != src.stat().st_size:
            shutil.copyfile(src, dst)
    else:
        part = dst.with_suffix(dst.suffix + ".part")
        if not dst.exists():
            for attempt in range(retries):
                have = part.stat().st_size if part.exists() else 0
                headers = {"User-Agent": _USER_AGENT, **({"Range": f"bytes={have}-"} if have else {})}
                try:
                    with requests.get(
                        url, headers=headers, stream=True, timeout=120, allow_redirects=True
                    ) as r:
                        if r.status_code == 416:  # already complete
                            break
                        if have and r.status_code != 206:
                            have = 0  # server ignored the range: start over
                            mode = "wb"
                        else:
                            mode = "ab" if have else "wb"
                        r.raise_for_status()
                        total = int(r.headers.get("Content-Length", 0)) + have
                        log.info(
                            "downloading %s (%.1f MB)%s",
                            url,
                            total / 1e6,
                            f" from {have / 1e6:.1f} MB" if have else "",
                        )
                        with open(part, mode) as f:
                            f.writelines(r.iter_content(chunk_size=1 << 20))
                    break
                except (requests.RequestException, OSError) as e:
                    log.warning("download attempt %d failed: %s", attempt + 1, e)
                    time.sleep(min(60, 5 * (attempt + 1)))
            else:
                raise RuntimeError(f"could not download {url}")
            part.replace(dst)
    digest = sha256_file(dst)
    if expected_sha256 and digest != expected_sha256:
        raise RuntimeError(f"sha256 mismatch for {dst.name}: expected {expected_sha256}, got {digest}")
    return {"url": url, "size": dst.stat().st_size, "sha256": digest}


# --- the Wikidata sameAs file ----------------------------------------------------


def convert_sameas(src: Path, dst: Path, chunk_lines: int = 500_000) -> dict:
    rows = 0
    with ParquetAppender(dst, SAMEAS_SCHEMA) as w:
        for lines in iter_line_chunks(src, chunk_lines):
            df = pl.DataFrame({"line": lines})
            df = df.select(pl.col("line").str.extract_groups(_SAMEAS_RE).alias("g")).unnest("g")
            df = df.rename({"1": "q", "2": "lang", "3": "title"}).filter(pl.col("q").is_not_null())
            df = df.with_columns(pl.col("lang").fill_null("en"))
            df = df.filter(
                ~pl.col("lang").is_in(list(NON_WIKIPEDIA_HOSTS)) & ~pl.col("title").str.contains(":")
            )
            # Titles are the raw IRI local names; normalise the ones that carry percent-encoding.
            enc = pl.col("title").str.contains("%")
            if df.select(enc.any()).item():
                df = df.with_columns(
                    pl.when(enc)
                    .then(
                        pl.col("title").map_elements(
                            lambda t: normalize_iri("http://x.org/" + t)[13:], return_dtype=pl.String
                        )
                    )
                    .otherwise(pl.col("title"))
                    .alias("title")
                )
            w.write(df.select("q", "lang", "title"))
            rows += df.height
    log.info("converted %s -> %s (%d chapter links)", src.name, dst.name, rows)
    return {"rows": rows}


# --- pageviews ------------------------------------------------------------------


def _decompressor() -> list[str]:
    for exe in ("lbzip2", "pbzip2", "bzip2"):
        path = shutil.which(exe)
        if path:
            return [path, "-dc"]
    raise RuntimeError("no bzip2 decompressor found on PATH (install lbzip2 for speed, or bzip2)")


def aggregate_pageviews_month(raw: Path, dst: Path, languages: list[str], access_methods: list[str]) -> dict:
    """Filter one monthly pageview_complete file to the target wikis and sum views per (lang, title).

    The raw file is streamed through the system bzip2 decompressor and grep (both C, both fast) so
    Python only sees the target-language rows. Line format:
      wiki_code title page_id access_method monthly_total daily_string
    """
    filtered = dst.with_suffix(".tsv")
    pattern = "^(" + "|".join(languages) + r")\.wikipedia "
    with open(filtered, "wb") as out:
        env = dict(os.environ, LC_ALL="C")
        p1 = subprocess.Popen([*_decompressor(), str(raw)], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(["grep", "-E", pattern], stdin=p1.stdout, stdout=out, env=env)
        assert p1.stdout is not None
        p1.stdout.close()
        rc2 = p2.wait()
        rc1 = p1.wait()
    if rc1 != 0 or rc2 not in (0, 1):  # grep returns 1 when nothing matched
        raise RuntimeError(f"pageview filtering failed for {raw.name} (bzip2 rc={rc1}, grep rc={rc2})")

    if filtered.stat().st_size == 0:
        df = pl.DataFrame(schema={"lang": pl.String, "title": pl.String, "views": pl.Int64})
    else:
        lf = pl.scan_csv(
            filtered,
            separator=" ",
            has_header=False,
            new_columns=["wiki", "title", "page_id", "access", "views", "daily"],
            schema_overrides={
                "wiki": pl.String,
                "title": pl.String,
                "page_id": pl.String,
                "access": pl.String,
                "views": pl.Int64,
                "daily": pl.String,
            },
            quote_char=None,
            ignore_errors=True,
            truncate_ragged_lines=True,
        )
        df = (
            lf.filter(pl.col("access").is_in(access_methods) & pl.col("views").is_not_null())
            .with_columns(pl.col("wiki").str.split(".").list.first().alias("lang"))
            .group_by("lang", "title")
            .agg(pl.col("views").sum())
            .collect(engine="streaming")
        )
    write_parquet_atomic(df, dst)
    filtered.unlink(missing_ok=True)
    log.info("aggregated %s -> %s (%d rows)", raw.name, dst.name, df.height)
    return {"rows": df.height}


def _month_url(template: str, month: str) -> str:
    year, mm = month.split("-")
    return template.format(year=year, month=mm)


# --- stage entry point ------------------------------------------------------------


def run(cfg: Config, force: bool = False, skip_pageviews: bool = False) -> None:
    global _USER_AGENT
    _USER_AGENT = str(cfg["sources"].get("user_agent", DEFAULT_USER_AGENT))
    cache = cfg.cache
    downloads = cache / "downloads"
    parquet_dir = cache / "parquet"
    samples_dir = cache / "iri_samples"
    for d in (downloads, parquet_dir, samples_dir, cache / "pageviews"):
        d.mkdir(parents=True, exist_ok=True)
    lock_path = cache / "sources.lock.json"
    lock = read_json(lock_path, {}) or {}

    for spec in cfg["sources"]["files"]:
        key, url = spec["key"], spec["url"]
        raw = downloads / Path(urlparse(url).path).name
        out = parquet_dir / f"{key}.parquet"
        if out.exists() and key in lock and not force:
            continue
        with timed(f"[1] {key}: download"):
            info = _download(url, raw, spec.get("sha256", ""))
        with timed(f"[1] {key}: convert to parquet"):
            if key == "sameas_all_wikis":
                stats = convert_sameas(raw, out)
            else:
                stats = convert_to_parquet(raw, out)
                (samples_dir / f"{key}.txt").write_text(
                    "\n".join(stats.pop("sample_iris")) + "\n", encoding="utf-8"
                )
        lock[key] = {**info, **stats, "format": spec.get("format", "")}
        write_json(lock, lock_path)

    if skip_pageviews:
        return
    pv = cfg["pageviews"]
    months: list[str] = list(pv["months"])
    month_files = []
    for month in months:
        out = cache / "pageviews" / f"{month}.parquet"
        month_files.append(out)
        lock_key = f"pageviews:{month}"
        if out.exists() and lock_key in lock and not force:
            continue
        url = _month_url(pv["url_template"], month)
        raw = downloads / Path(urlparse(url).path).name
        with timed(f"[1] pageviews {month}: download"):
            info = _download(url, raw)
        with timed(f"[1] pageviews {month}: aggregate"):
            stats = aggregate_pageviews_month(raw, out, list(pv["languages"]), list(pv["access_methods"]))
        lock[lock_key] = {**info, **stats}
        write_json(lock, lock_path)
        if pv.get("delete_raw_after_aggregation", True) and urlparse(url).scheme != "file":
            raw.unlink(missing_ok=True)
