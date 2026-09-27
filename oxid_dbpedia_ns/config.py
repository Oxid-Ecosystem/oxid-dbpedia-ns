"""Configuration loading. One config.toml drives every stage."""

from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Bucket:
    name: str
    group: str
    roots: list[str]
    share: float  # renormalised so that all buckets sum to 1


@dataclass
class Config:
    path: Path
    raw: dict[str, Any]
    root: Path
    cache: Path
    work: Path
    out: Path
    tiers: list[int]
    buckets: list[Bucket] = field(default_factory=list)

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.raw.get(key, default)

    @property
    def config_hash(self) -> str:
        return hashlib.sha256(self.path.read_bytes()).hexdigest()

    @property
    def top_tier(self) -> int:
        return self.tiers[-1]

    def bucket_by_name(self, name: str) -> Bucket:
        for b in self.buckets:
            if b.name == name:
                return b
        raise KeyError(name)

    @property
    def groups(self) -> list[str]:
        seen: list[str] = []
        for b in self.buckets:
            if b.group not in seen:
                seen.append(b.group)
        return seen


def load_config(path: str | Path = "config.toml") -> Config:
    path = Path(path).resolve()
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    root = path.parent
    paths = raw.get("paths", {})

    tiers = list(raw["dataset"]["tiers"])
    if tiers != sorted(tiers) or len(set(tiers)) != len(tiers):
        raise ValueError("dataset.tiers must be strictly ascending")

    raw_buckets = raw.get("buckets", [])
    total = sum(float(b["share"]) for b in raw_buckets)
    if total <= 0:
        raise ValueError("bucket shares must sum to a positive number")
    buckets = [
        Bucket(name=b["name"], group=b["group"], roots=list(b["roots"]), share=float(b["share"]) / total)
        for b in raw_buckets
    ]
    names = [b.name for b in buckets]
    if len(names) != len(set(names)):
        raise ValueError("bucket names must be unique")

    return Config(
        path=path,
        raw=raw,
        root=root,
        cache=(root / paths.get("cache", "cache")).resolve(),
        work=(root / paths.get("work", "work")).resolve(),
        out=(root / paths.get("out", "out")).resolve(),
        tiers=tiers,
        buckets=buckets,
    )
