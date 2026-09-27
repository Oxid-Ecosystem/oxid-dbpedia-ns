"""Command line entry point: `oxid-dbpedia-ns <stage> [--config config.toml] [--force]`."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import load_config
from .util import setup_logging

STAGES = ["acquire", "candidates", "geography", "rank", "enrich", "emit", "validate"]
_CHARS_PER_TOKEN = 4.06  # measured on 20K real DBpedia abstracts with the cl100k tokenizer (2026-09-18)


def cost_report(cfg) -> dict:
    """Embedding cost: an estimate before Stage 5 runs, the running ledger during it, the final total after."""
    import json

    from .embeddings import Embedder

    emb = Embedder(dict(cfg["embedding"]), cfg.work)
    out: dict = {
        "provider": emb.provider,
        "model": emb.model,
        "price_usd_per_million_tokens": emb.price_per_token() * 1e6,
    }
    summary = emb.cost_summary()
    out["so_far"] = summary
    ranked = cfg.work / "ranked.parquet"
    cand = cfg.work / "candidates.parquet"
    if ranked.exists() and cand.exists():
        import polars as pl

        df = (
            pl.scan_parquet(ranked)
            .select("iri")
            .join(pl.scan_parquet(cand).select("iri", "title", "abstract"), on="iri")
            .select(
                (pl.col("title").str.len_chars() + 1 + pl.col("abstract").str.len_chars())
                .sum()
                .alias("chars"),
                pl.len().alias("n"),
            )
            .collect()
        )
        chars, n = df.row(0)
        est_tokens = int(chars / _CHARS_PER_TOKEN)
        out["estimate_for_top_tier"] = {
            "entities": n,
            "tokens": est_tokens,
            "cost_usd": round(est_tokens * emb.price_per_token(), 4),
            "basis": f"{_CHARS_PER_TOKEN} chars per token",
        }
    manifest = cfg.work / "embedding_manifest.json"
    if manifest.exists():
        out["final"] = json.loads(manifest.read_text())
    return out


def _load_dotenv(root: Path) -> None:
    env = root / ".env"
    if env.exists():
        import os

        for line in env.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def run_stage(name: str, cfg, args) -> bool:
    from .stages import acquire, candidates, emit, enrich_embed, geography, rank, validate

    if name == "acquire":
        acquire.run(cfg, force=args.force, skip_pageviews=args.skip_pageviews)
    elif name == "candidates":
        candidates.run(cfg, force=args.force)
    elif name == "geography":
        geography.run(cfg, force=args.force)
    elif name == "rank":
        rank.run(cfg, force=args.force, allow_short=args.allow_short)
    elif name == "enrich":
        enrich_embed.run(cfg, force=args.force)
    elif name == "emit":
        emit.run(cfg, force=args.force)
    elif name == "validate":
        return validate.run(cfg, force=args.force)
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="oxid-dbpedia-ns", description="Build the OxidDB neurosymbolic DBpedia dataset."
    )
    ap.add_argument(
        "stage",
        choices=[*STAGES, "all", "cost"],
        help="stage to run, all, or cost (embedding token/cost counter)",
    )
    ap.add_argument("--config", default="config.toml")
    ap.add_argument("--force", action="store_true", help="ignore cached results for this stage")
    ap.add_argument(
        "--skip-pageviews", action="store_true", help="acquire: skip the Wikimedia pageview dumps"
    )
    ap.add_argument(
        "--allow-short", action="store_true", help="rank: continue when the pool cannot fill the top tier"
    )
    ap.add_argument("--from", dest="from_stage", choices=STAGES, help="all: start at this stage")
    args = ap.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    _load_dotenv(cfg.root)
    if args.stage == "cost":
        import json

        print(json.dumps(cost_report(cfg), indent=2))
        return 0
    stages = STAGES if args.stage == "all" else [args.stage]
    if args.stage == "all" and args.from_stage:
        stages = STAGES[STAGES.index(args.from_stage) :]
    for s in stages:
        if not run_stage(s, cfg, args):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
