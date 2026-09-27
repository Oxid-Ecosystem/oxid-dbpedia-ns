#!/usr/bin/env python3
"""Push a verified tier set to Kaggle Datasets.

    python publish/to_kaggle.py out --id vincentbrand/oxid-dbpedia-ns              # plan only
    python publish/to_kaggle.py out --id vincentbrand/oxid-dbpedia-ns --yes        # create or version
    python publish/to_kaggle.py out --id vincentbrand/oxid-dbpedia-ns --yes --public

Nothing is uploaded without --yes, and `publish/preflight.py` must pass first.

Kaggle has no notion of dataset configs and no per-file metadata, so the layout differs from the
Hugging Face copy on purpose:

  - Each tier directory is uploaded as one zip (`t50.zip`, `t100.zip`, `t180.zip`), because the
    Kaggle API archives subdirectories rather than preserving them. Users unzip one tier.
  - The dataset card is generated here as `dataset-metadata.json` with the frontmatter stripped out
    of `out/README.md`, since Kaggle reads the description from that file, not from a README.
  - Keywords are left empty: Kaggle rejects tags outside its own vocabulary, so pick them in the web
    UI after the first upload.

A second run of --yes creates a new *version* of the dataset rather than a duplicate.

Needs the `kaggle` client and an API token:  uv sync --extra publish, then put kaggle.json in
~/.kaggle/ (Kaggle → Settings → API → Create New Token), or set KAGGLE_USERNAME and KAGGLE_KEY.
Environment: KAGGLE_DATASET_ID.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

TITLE = "OxidDB Neurosymbolic DBpedia Dataset"
SUBTITLE = "180K DBpedia entities with an OWL 2 EL ontology and 1536-d embeddings"
LICENSE = "CC-BY-SA-4.0"

CARD_SUFFIX = """

## Layout on Kaggle

Each tier is one zip (`t50.zip`, `t100.zip`, `t180.zip`) holding the files listed above; unzip the
tier you need. The Hugging Face copy keeps the Parquet files unzipped and exposes one loadable
config per tier, so prefer it if you want `load_dataset`.
"""


def strip_frontmatter(text: str) -> str:
    if not text.startswith("---"):
        return text
    parts = text.split("---", 2)
    return parts[2].lstrip("\n") if len(parts) == 3 else text


def credentials_present() -> bool:
    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return True
    return (Path.home() / ".kaggle" / "kaggle.json").exists()


def kaggle_cli() -> list[str] | None:
    if shutil.which("kaggle"):
        return ["kaggle"]
    venv = Path(".venv/bin/kaggle")
    if venv.exists():
        return [str(venv)]
    try:
        import kaggle  # noqa: F401
    except ImportError:
        return None
    return [sys.executable, "-m", "kaggle"]


def dataset_exists(cli: list[str], dataset_id: str) -> bool:
    _owner, _, slug = dataset_id.partition("/")
    r = subprocess.run([*cli, "datasets", "list", "-m", "-s", slug], capture_output=True, text=True)
    return dataset_id.lower() in r.stdout.lower()


def main() -> int:
    ap = argparse.ArgumentParser(description="Upload a tier set to Kaggle Datasets.")
    ap.add_argument("out", type=Path, nargs="?", default=Path("out"), help="directory holding the tiers")
    ap.add_argument(
        "--id", default=os.environ.get("KAGGLE_DATASET_ID"), help="e.g. vincentbrand/oxid-dbpedia-ns"
    )
    ap.add_argument("--public", action="store_true", help="publish publicly (Kaggle defaults to private)")
    ap.add_argument(
        "--yes", action="store_true", help="actually upload; without it this only prints the plan"
    )
    ap.add_argument(
        "--skip-preflight", action="store_true", help="do not verify the tiers first (not advised)"
    )
    ap.add_argument("--message", default=None, help="version notes for an update")
    args = ap.parse_args()

    if not args.id or "/" not in args.id:
        print("to_kaggle: --id or KAGGLE_DATASET_ID is required as owner/slug", file=sys.stderr)
        return 2
    if not args.out.is_dir():
        print(f"to_kaggle: {args.out} is not a directory", file=sys.stderr)
        return 2

    tiers = sorted(p.name for p in args.out.iterdir() if p.is_dir() and (p / "manifest.json").exists())
    version = "unknown"
    if tiers:
        m = json.loads((args.out / tiers[0] / "manifest.json").read_text())
        version = str(m.get("dataset", {}).get("version") or "unknown")

    card = args.out / "README.md"
    description = strip_frontmatter(card.read_text()) + CARD_SUFFIX if card.exists() else TITLE

    metadata = {
        "title": TITLE,
        "subtitle": SUBTITLE,
        "id": args.id,
        "licenses": [{"name": LICENSE}],
        "description": description,
    }

    print(f"dataset     https://www.kaggle.com/datasets/{args.id}")
    print(f"visibility  {'public' if args.public else 'private'}")
    print(f"version     {version}")
    print(
        f"tiers       {', '.join(tiers) or 'none found'} (uploaded as {', '.join(t + '.zip' for t in tiers)})"
    )
    print(f"title       {TITLE} ({len(TITLE)} chars, Kaggle allows 6-50)")
    print(f"subtitle    {SUBTITLE} ({len(SUBTITLE)} chars, Kaggle allows 20-80)")
    print(f"license     {LICENSE}")
    print(f"card        {len(description)} chars from {card}")

    if not args.yes:
        print(f"\nplan only — re-run with --yes to upload (would write {args.out / 'dataset-metadata.json'})")
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

    cli = kaggle_cli()
    if cli is None:
        print("to_kaggle: the kaggle client is missing — run `uv sync --extra publish`", file=sys.stderr)
        return 2
    if not credentials_present():
        print(
            "to_kaggle: no Kaggle credentials — put kaggle.json in ~/.kaggle/ "
            "(Kaggle → Settings → API → Create New Token) or set KAGGLE_USERNAME and KAGGLE_KEY",
            file=sys.stderr,
        )
        return 2

    meta_path = args.out / "dataset-metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"wrote {meta_path}")

    if dataset_exists(cli, args.id):
        cmd = [
            *cli,
            "datasets",
            "version",
            "-p",
            str(args.out),
            "-r",
            "zip",
            "-m",
            args.message or f"{version}",
        ]
        print(f"dataset exists — creating a new version: {' '.join(cmd)}")
    else:
        cmd = [*cli, "datasets", "create", "-p", str(args.out), "-r", "zip"]
        if args.public:
            cmd.append("-u")
        print(f"new dataset: {' '.join(cmd)}")

    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"\nkaggle exited {r.returncode}", file=sys.stderr)
        return r.returncode

    print(f"\ndataset: https://www.kaggle.com/datasets/{args.id}")
    if not args.public:
        print(
            "it is private — flip it to public in the Kaggle UI, and add keywords there while you are at it"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
