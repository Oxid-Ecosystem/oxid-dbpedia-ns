#!/usr/bin/env python3
"""Release gate: refuse to publish a tier set that is not what it claims to be.

    python publish/preflight.py out            # all tiers, full hash verification
    python publish/preflight.py out --quick    # sizes only, skip hashing 1.5 GB

Every check is a hard failure. What it proves:
  1. Stage 7 ran and passed        out/validation_report.json says ok: true
  2. The vectors are real          no manifest says embedding.provider == "fake"
  3. The dataset card is publishable   out/README.md exists with license frontmatter
  4. The tiers are complete        every file each manifest lists is present
  5. The bytes are the ones measured   size and SHA-256 match manifest.files
  6. The tiers agree with each other   same config hash, same embedding run, nested tier sizes

Exit code 0 means the directory is safe to push to Hugging Face or Kaggle.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

FAKE_PROVIDERS = {"fake", "test", "deterministic"}


def sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while block := fh.read(chunk):
            h.update(block)
    return h.hexdigest()


def check(out: Path, quick: bool) -> list[str]:
    fails: list[str] = []

    report = out / "validation_report.json"
    if not report.exists():
        fails.append(f"{report}: missing — run `make validate` before publishing")
    else:
        r = json.loads(report.read_text())
        if r.get("ok") is not True:
            hard = r.get("hard", {})
            bad = [k for k, v in hard.items() if v is not True] if isinstance(hard, dict) else []
            fails.append(f"{report}: ok is {r.get('ok')!r}, failing hard checks: {bad or 'see report'}")

    card = out / "README.md"
    if not card.exists():
        fails.append(f"{card}: missing — the dataset card is emitted by stage 6")
    else:
        text = card.read_text()
        if not text.startswith("---"):
            fails.append(f"{card}: no YAML frontmatter, Hugging Face needs it for the license and configs")
        elif "license:" not in text.split("---")[1]:
            fails.append(f"{card}: frontmatter has no license field")

    tiers = sorted(p for p in out.iterdir() if p.is_dir() and (p / "manifest.json").exists())
    if not tiers:
        fails.append(f"{out}: no tier directory with a manifest.json — run `make emit`")
        return fails

    seen: dict[str, set[str]] = {"config_sha256": set(), "embedding_run": set()}
    sizes: list[tuple[str, int]] = []

    for tier in tiers:
        m = json.loads((tier / "manifest.json").read_text())
        name = tier.name

        provider = str(m.get("embedding", {}).get("provider", "")).lower()
        if provider in FAKE_PROVIDERS or not provider:
            fails.append(
                f"{name}: embedding.provider is {provider!r} — that is the test embedder, never publish it"
            )

        seen["config_sha256"].add(str(m.get("config_sha256")))
        seen["embedding_run"].add(
            str(m.get("embedding", {}).get("batch_id") or m.get("embedding", {}).get("model"))
        )
        sizes.append((name, int(m.get("counts", {}).get("entities", 0))))

        files = m.get("files") or {}
        if not files:
            fails.append(f"{name}: manifest lists no files")
        for fname, meta in files.items():
            f = tier / fname
            if not f.exists():
                fails.append(f"{name}/{fname}: listed in the manifest but missing")
                continue
            actual_bytes = f.stat().st_size
            if actual_bytes != meta["bytes"]:
                fails.append(f"{name}/{fname}: {actual_bytes} bytes, manifest says {meta['bytes']}")
                continue
            if not quick:
                actual = sha256(f)
                if actual != meta["sha256"]:
                    fails.append(f"{name}/{fname}: sha256 {actual[:16]}… != manifest {meta['sha256'][:16]}…")

        for extra in ("manifest.json", "ATTRIBUTION.md"):
            if not (tier / extra).exists():
                fails.append(f"{name}/{extra}: missing")

    if len(seen["config_sha256"]) > 1:
        fails.append(f"tiers were built from different configs: {sorted(seen['config_sha256'])}")
    if len(seen["embedding_run"]) > 1:
        fails.append(f"tiers were built from different embedding runs: {sorted(seen['embedding_run'])}")

    counts = [n for _, n in sorted(sizes, key=lambda p: p[1])]
    if counts != sorted(counts) or len(set(counts)) != len(counts):
        fails.append(f"tier entity counts are not strictly increasing: {sizes}")

    return fails


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify a tier set before publishing it.")
    ap.add_argument("out", type=Path, nargs="?", default=Path("out"), help="directory holding the tiers")
    ap.add_argument("--quick", action="store_true", help="compare sizes only, do not hash every file")
    args = ap.parse_args()

    if not args.out.is_dir():
        print(f"preflight: {args.out} is not a directory", file=sys.stderr)
        return 2

    print(f"preflight: {args.out}{' (quick)' if args.quick else ''}")
    fails = check(args.out, args.quick)

    if fails:
        print(f"\nFAIL — {len(fails)} problem(s):", file=sys.stderr)
        for f in fails:
            print(f"  - {f}", file=sys.stderr)
        return 1

    tiers = sorted(p.name for p in args.out.iterdir() if p.is_dir() and (p / "manifest.json").exists())
    print(f"OK — {', '.join(tiers)} verified, safe to publish")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
