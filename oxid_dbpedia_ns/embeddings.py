"""Embedding providers with per-part checkpointing.

- openai_batch: OpenAI Batch API, files of at most `batch_file_size` requests, all parts
  submitted up front and polled together. Resumable: batch ids are recorded in
  work/vectors/batches.json and completed parts are Parquet files.
- openai_sync: direct embeddings.create calls, for small runs and for filling gaps.
- fake: deterministic unit vectors seeded from the IRI, for tests and dry runs. Never
  use for a published tier; the manifest records the provider.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import polars as pl

from .util import log, read_json, write_json, write_parquet_atomic


def fake_vector(iri: str, dims: int) -> np.ndarray:
    seed = int.from_bytes(hashlib.sha256(iri.encode("utf-8")).digest()[:8], "little")
    v = np.random.default_rng(seed).standard_normal(dims).astype(np.float32)
    return v / np.linalg.norm(v)


def _vectors_frame(iris: list[str], vectors: list[np.ndarray], dims: int) -> pl.DataFrame:
    arr = np.stack(vectors).astype(np.float32)
    return pl.DataFrame({"iri": iris, "vector": pl.Series(arr).cast(pl.Array(pl.Float32, dims))})


class Embedder:
    def __init__(self, cfg: dict, work_dir: Path):
        self.cfg = cfg
        self.provider = cfg.get("provider", "openai_batch")
        self.model = cfg["model"]
        self.dims = int(cfg["dimensions"])
        self.dir = work_dir / "vectors"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.part_size = int(cfg.get("batch_file_size", 50000))
        self.total_tokens = 0
        self._client = None
        self.ledger_path = self.dir / "tokens.json"
        self.ledger: dict = read_json(self.ledger_path, {}) or {}

    # -- cost ledger --------------------------------------------------------------

    def price_per_token(self) -> float:
        key = (
            "batch_price_per_million_tokens"
            if self.provider == "openai_batch"
            else "price_per_million_tokens"
        )
        return float(self.cfg.get(key, 0.0)) / 1e6

    def record_part(self, name: str, tokens: int, count: int) -> None:
        """Persist tokens per completed part so `oxid-dbpedia-ns cost` can report mid-run."""
        self.ledger[name] = {
            "tokens": tokens,
            "count": count,
            "provider": self.provider,
            "model": self.model,
            "usd": round(tokens * self.price_per_token(), 4),
            "at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
        write_json(self.ledger, self.ledger_path)

    def cost_summary(self) -> dict:
        tokens = sum(int(v.get("tokens", 0)) for v in self.ledger.values())
        count = sum(int(v.get("count", 0)) for v in self.ledger.values())
        return {
            "parts": len(self.ledger),
            "entities": count,
            "total_tokens": tokens,
            "price_usd_per_million_tokens": self.price_per_token() * 1e6,
            "cost_usd": round(tokens * self.price_per_token(), 4),
        }

    @property
    def client(self):
        if self._client is None:
            from openai import OpenAI

            if not os.environ.get("OPENAI_API_KEY"):
                raise RuntimeError("OPENAI_API_KEY is not set (see .env.example)")
            self._client = OpenAI()
        return self._client

    # -- public ----------------------------------------------------------------

    def embed(self, iris: list[str], texts: list[str]) -> tuple[pl.DataFrame, dict]:
        """Embed texts (aligned with iris). Returns (DataFrame[iri, vector], manifest info)."""
        started = datetime.now(UTC).isoformat(timespec="seconds")
        parts = [
            (i, iris[i : i + self.part_size], texts[i : i + self.part_size])
            for i in range(0, len(iris), self.part_size)
        ]
        if self.provider == "fake":
            frames = [self._fake_part(n, p_iris) for n, p_iris, _ in parts]
        elif self.provider == "openai_sync":
            frames = [self._sync_part(n, p_iris, p_texts) for n, p_iris, p_texts in parts]
        elif self.provider == "openai_batch":
            frames = self._batch_parts(parts)
        else:
            raise ValueError(f"unknown embedding provider {self.provider!r}")
        df = (
            pl.concat(frames)
            if frames
            else pl.DataFrame(schema={"iri": pl.String, "vector": pl.Array(pl.Float32, self.dims)})
        )
        missing = set(iris) - set(df.get_column("iri").to_list())
        if missing:
            raise RuntimeError(
                f"{len(missing)} entities have no vector after embedding (first: {sorted(missing)[:3]})"
            )
        df = pl.DataFrame({"iri": iris}).join(df, on="iri", how="left")
        info = {
            "provider": self.provider,
            "model": self.model if self.provider != "fake" else "fake",
            "dimensions": self.dims,
            "requested_at": started,
            "completed_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "total_tokens": self.total_tokens,
            "parts": len(parts),
            "count": len(iris),
            **{
                k: v
                for k, v in self.cost_summary().items()
                if k in ("price_usd_per_million_tokens", "cost_usd")
            },
        }
        if self.provider != "fake":
            log.info(
                "embedding cost: %d tokens at $%.3f per 1M = $%.4f",
                self.total_tokens,
                info["price_usd_per_million_tokens"],
                info["cost_usd"],
            )
        return df, info

    # -- parts -----------------------------------------------------------------

    def _part_path(self, n: int) -> Path:
        return self.dir / f"part-{n // self.part_size:03d}.parquet"

    def _fake_part(self, n: int, iris: list[str]) -> pl.DataFrame:
        path = self._part_path(n)
        if path.exists():
            return pl.read_parquet(path)
        df = _vectors_frame(iris, [fake_vector(i, self.dims) for i in iris], self.dims)
        write_parquet_atomic(df, path)
        self.record_part(path.name, 0, len(iris))
        return df

    def _sync_embed(self, texts: list[str]) -> list[np.ndarray]:
        chunk = int(self.cfg.get("sync_chunk_size", 512))
        out: list[np.ndarray] = []
        for i in range(0, len(texts), chunk):
            batch = texts[i : i + chunk]
            for attempt in range(6):
                try:
                    resp = self.client.embeddings.create(model=self.model, input=batch, dimensions=self.dims)
                    break
                except Exception as e:  # rate limits, transient network errors
                    wait = min(60, 2**attempt)
                    log.warning("embeddings.create failed (%s); retrying in %ds", e, wait)
                    time.sleep(wait)
            else:
                raise RuntimeError("embeddings.create kept failing")
            self.total_tokens += resp.usage.total_tokens
            out.extend(
                np.asarray(d.embedding, dtype=np.float32) for d in sorted(resp.data, key=lambda d: d.index)
            )
        return out

    def _sync_part(self, n: int, iris: list[str], texts: list[str]) -> pl.DataFrame:
        path = self._part_path(n)
        if path.exists():
            self.total_tokens += int(self.ledger.get(path.name, {}).get("tokens", 0))
            return pl.read_parquet(path)
        log.info("embedding part %s: %d texts (sync)", path.name, len(iris))
        before = self.total_tokens
        df = _vectors_frame(iris, self._sync_embed(texts), self.dims)
        write_parquet_atomic(df, path)
        self.record_part(path.name, self.total_tokens - before, len(iris))
        return df

    def _batch_parts(self, parts: list[tuple[int, list[str], list[str]]]) -> list[pl.DataFrame]:
        state_path = self.dir / "batches.json"
        state: dict = read_json(state_path, {}) or {}
        pending: dict[str, tuple[int, list[str], list[str]]] = {}
        frames: dict[str, pl.DataFrame] = {}
        poll = int(self.cfg.get("poll_seconds", 30))

        # Submit every part that has neither a result file nor a recorded batch.
        for n, iris, texts in parts:
            path = self._part_path(n)
            if path.exists():
                frames[path.name] = pl.read_parquet(path)
                self.total_tokens += int(self.ledger.get(path.name, {}).get("tokens", 0))
                continue
            if path.name not in state:
                req = self.dir / path.name.replace(".parquet", ".jsonl")
                with open(req, "w", encoding="utf-8") as f:
                    f.writelines(
                        json.dumps(
                            {
                                "custom_id": iri,
                                "method": "POST",
                                "url": "/v1/embeddings",
                                "body": {"model": self.model, "input": text, "dimensions": self.dims},
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                        for iri, text in zip(iris, texts)
                    )
                with open(req, "rb") as fh:
                    up = self.client.files.create(file=fh, purpose="batch")
                batch = self.client.batches.create(
                    input_file_id=up.id, endpoint="/v1/embeddings", completion_window="24h"
                )
                state[path.name] = {
                    "batch_id": batch.id,
                    "input_file_id": up.id,
                    "submitted_at": datetime.now(UTC).isoformat(timespec="seconds"),
                }
                write_json(state, state_path)
                log.info("submitted batch %s for %s (%d requests)", batch.id, path.name, len(iris))
            pending[path.name] = (n, iris, texts)

        # Poll until every pending part is done.
        while pending:
            for name in list(pending):
                n, iris, texts = pending[name]
                b = self.client.batches.retrieve(state[name]["batch_id"])
                if b.status in ("validating", "in_progress", "finalizing"):
                    continue
                if b.status != "completed":
                    raise RuntimeError(f"batch {b.id} for {name} ended with status {b.status}: {b.errors}")
                vectors: dict[str, np.ndarray] = {}
                tokens_before = self.total_tokens
                content = self.client.files.content(b.output_file_id).text
                for line in content.splitlines():
                    if not line.strip():
                        continue
                    rec = json.loads(line)
                    body = rec.get("response", {}).get("body", {})
                    if rec.get("response", {}).get("status_code") == 200 and body.get("data"):
                        vectors[rec["custom_id"]] = np.asarray(body["data"][0]["embedding"], dtype=np.float32)
                        self.total_tokens += int(body.get("usage", {}).get("total_tokens", 0))
                missing = [(i, t) for i, t in zip(iris, texts) if i not in vectors]
                if missing:
                    log.warning(
                        "batch %s: %d requests failed; embedding them synchronously", b.id, len(missing)
                    )
                    for (i, _), v in zip(missing, self._sync_embed([t for _, t in missing])):
                        vectors[i] = v
                df = _vectors_frame(iris, [vectors[i] for i in iris], self.dims)
                write_parquet_atomic(df, self._part_path(n))
                self.record_part(name, self.total_tokens - tokens_before, len(iris))
                frames[name] = df
                state[name]["completed_at"] = datetime.now(UTC).isoformat(timespec="seconds")
                write_json(state, state_path)
                log.info("batch %s completed for %s", b.id, name)
                del pending[name]
            if pending:
                log.info("waiting on %d batch(es): %s", len(pending), ", ".join(sorted(pending)))
                time.sleep(poll)
        return [frames[k] for k in sorted(frames)]
