#!/usr/bin/env python3
"""Push a verified tier set to Hugging Face Datasets.

    python publish/to_huggingface.py out --repo Oxid-Ecosystem/oxid-dbpedia-ns        # plan only
    python publish/to_huggingface.py out --repo Oxid-Ecosystem/oxid-dbpedia-ns --yes  # actually upload

Nothing is uploaded without --yes: the default run prints the repo, the visibility and every file
it would push, then stops. `publish/preflight.py` runs first and a failure aborts the upload, so a
tier built with the fake embedder or a tier whose bytes drifted from its manifest cannot be pushed.

`out/README.md` is the dataset card: stage 6 writes it with the YAML frontmatter Hugging Face needs
(license, one config per tier), so it is uploaded as-is and becomes the repo landing page.

Re-running is safe. The Hub deduplicates by content hash, so an interrupted upload resumes rather
than re-sending what already arrived.

Needs `huggingface_hub` and a write token:  uv sync --extra publish && hf auth login
Environment: HF_DATASET_REPO, HF_TOKEN.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Tier payloads are far past the Hub's 10 MB inline limit; declare them LFS explicitly rather than
# relying on the default .gitattributes to cover .nt and .txt, which it does not.
GITATTRIBUTES = """\
*.parquet filter=lfs diff=lfs merge=lfs -text
*.nt filter=lfs diff=lfs merge=lfs -text
*.owl filter=lfs diff=lfs merge=lfs -text
*.ttl filter=lfs diff=lfs merge=lfs -text
*.txt filter=lfs diff=lfs merge=lfs -text
"""

IGNORE = ["dataset-metadata.json", ".DS_Store", "*.log", ".gitattributes"]


def plan(out: Path) -> tuple[list[Path], int]:
    files = [
        p
        for p in sorted(out.rglob("*"))
        if p.is_file() and p.name not in {".DS_Store", "dataset-metadata.json"}
    ]
    return files, sum(p.stat().st_size for p in files)


def main() -> int:
    ap = argparse.ArgumentParser(description="Upload a tier set to Hugging Face Datasets.")
    ap.add_argument("out", type=Path, nargs="?", default=Path("out"), help="directory holding the tiers")
    ap.add_argument(
        "--repo", default=os.environ.get("HF_DATASET_REPO"), help="e.g. Oxid-Ecosystem/oxid-dbpedia-ns"
    )
    ap.add_argument("--private", action="store_true", help="create the repo private instead of public")
    ap.add_argument(
        "--yes", action="store_true", help="actually upload; without it this only prints the plan"
    )
    ap.add_argument(
        "--skip-preflight", action="store_true", help="do not verify the tiers first (not advised)"
    )
    ap.add_argument("--message", default=None, help="commit message")
    args = ap.parse_args()

    if not args.repo:
        print("to_huggingface: --repo or HF_DATASET_REPO is required (owner/name)", file=sys.stderr)
        return 2
    if not args.out.is_dir():
        print(f"to_huggingface: {args.out} is not a directory", file=sys.stderr)
        return 2

    files, total = plan(args.out)
    tiers = sorted(p.name for p in args.out.iterdir() if p.is_dir() and (p / "manifest.json").exists())
    version = "unknown"
    if tiers:
        m = json.loads((args.out / tiers[0] / "manifest.json").read_text())
        version = str(m.get("dataset", {}).get("version") or m.get("pipeline_version") or "unknown")

    print(f"repo        https://huggingface.co/datasets/{args.repo}")
    print(f"visibility  {'private' if args.private else 'public'}")
    print(f"version     {version}")
    print(f"tiers       {', '.join(tiers) or 'none found'}")
    print(f"payload     {len(files)} files, {total / 1e9:.2f} GB")
    for p in files:
        print(f"  {p.relative_to(args.out)}  {p.stat().st_size / 1e6:.1f} MB")

    if not args.yes:
        print("\nplan only — re-run with --yes to upload")
        return 0

    if not args.skip_preflight:
        sys.path.insert(0, str(Path(__file__).parent))
        from preflight import check

        fails = check(args.out, quick=False)
        if fails:
            print(f"\naborted: preflight found {len(fails)} problem(s):", file=sys.stderr)
            for f in fails:
                print(f"  - {f}", file=sys.stderr)
            return 1
        print("\npreflight OK")

    try:
        from huggingface_hub import HfApi
    except ImportError:
        print("to_huggingface: huggingface_hub is missing — run `uv sync --extra publish`", file=sys.stderr)
        return 2

    api = HfApi(token=os.environ.get("HF_TOKEN") or None)
    api.create_repo(args.repo, repo_type="dataset", private=args.private, exist_ok=True)
    print(f"repo ready: {args.repo}")

    api.upload_file(
        path_or_fileobj=GITATTRIBUTES.encode(),
        path_in_repo=".gitattributes",
        repo_id=args.repo,
        repo_type="dataset",
        commit_message="Track tier payloads with LFS",
    )

    url = api.upload_folder(
        folder_path=str(args.out),
        repo_id=args.repo,
        repo_type="dataset",
        ignore_patterns=IGNORE,
        commit_message=args.message or f"oxid-dbpedia-ns {version}: {', '.join(tiers)}",
    )
    print(f"\nuploaded: {url}")
    print(f"dataset:  https://huggingface.co/datasets/{args.repo}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
