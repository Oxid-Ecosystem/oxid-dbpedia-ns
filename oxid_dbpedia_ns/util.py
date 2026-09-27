"""Shared helpers: logging, hashing, decompression, incremental Parquet writing, stage caching."""

from __future__ import annotations

import bz2
import hashlib
import io
import json
import logging
import shutil
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Self

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("oxid_dbpedia_ns")


def setup_logging(level: int = logging.INFO) -> None:
    if log.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    log.addHandler(handler)
    log.setLevel(level)


@contextmanager
def timed(what: str) -> Iterator[None]:
    t0 = time.time()
    log.info("%s ...", what)
    yield
    log.info("%s done in %.1fs", what, time.time() - t0)


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def open_text(path: Path) -> IO[str]:
    """Open a possibly bz2-compressed text file for line iteration. Uses lbzip2 when available."""
    if path.suffix == ".bz2":
        exe = shutil.which("lbzip2") or shutil.which("pbzip2")
        if exe:
            proc = subprocess.Popen([exe, "-dc", str(path)], stdout=subprocess.PIPE)
            assert proc.stdout is not None
            return io.TextIOWrapper(proc.stdout, encoding="utf-8", errors="replace")
        return io.TextIOWrapper(bz2.open(path, "rb"), encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def iter_line_chunks(path: Path, chunk_lines: int = 500_000) -> Iterator[list[str]]:
    buf: list[str] = []
    with open_text(path) as f:
        for line in f:
            buf.append(line)
            if len(buf) >= chunk_lines:
                yield buf
                buf = []
    if buf:
        yield buf


class ParquetAppender:
    """Write polars DataFrames chunk by chunk into one Parquet file with a fixed schema."""

    def __init__(self, path: Path, schema: pa.Schema | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        self.schema = schema
        self.writer: pq.ParquetWriter | None = None
        self.rows = 0

    def write(self, df: pl.DataFrame) -> None:
        if df.height == 0:
            return
        table = df.to_arrow()
        if self.writer is None:
            self.schema = self.schema or table.schema
            self.writer = pq.ParquetWriter(self.tmp, self.schema, compression="zstd")
        if table.schema != self.schema:
            table = table.cast(self.schema)
        self.writer.write_table(table)
        self.rows += table.num_rows

    def close(self) -> None:
        if self.writer is None:
            # Empty result: still write a valid, empty file when a schema is known.
            if self.schema is not None:
                pq.write_table(self.schema.empty_table(), self.tmp, compression="zstd")
            else:
                pq.write_table(pa.table({}), self.tmp)
        else:
            self.writer.close()
        self.tmp.replace(self.path)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        if exc[0] is None:
            self.close()
        elif self.writer is not None:
            self.writer.close()
            self.tmp.unlink(missing_ok=True)


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)


def write_json(obj: Any, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path, default: Any = None) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def stage_is_fresh(output: Path, inputs: list[Path], stamp: str) -> bool:
    """A stage output is fresh when it exists, is newer than every input and carries the same config stamp."""
    if not output.exists():
        return False
    stamp_file = output.with_suffix(output.suffix + ".stamp")
    if not stamp_file.exists() or stamp_file.read_text().strip() != stamp:
        return False
    out_mtime = output.stat().st_mtime
    return all((not p.exists()) or p.stat().st_mtime <= out_mtime for p in inputs)


def write_stamp(output: Path, stamp: str) -> None:
    output.with_suffix(output.suffix + ".stamp").write_text(stamp)
