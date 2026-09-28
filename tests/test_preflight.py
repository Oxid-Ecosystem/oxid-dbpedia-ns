"""The release gate has to fail for the right reasons, so each failure mode gets a test.

A gate nobody tests is a gate that passes everything. These build a minimal tier directory on disk
and then break exactly one thing at a time.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "publish"))
from preflight import check


def write_tier(out: Path, name: str, entities: int, provider: str = "openai_batch") -> None:
    d = out / name
    d.mkdir(parents=True)
    payload = f"entities of {name}".encode()
    (d / "entities.parquet").write_bytes(payload)
    attribution = b"credits\n"
    (d / "ATTRIBUTION.md").write_bytes(attribution)
    (d / "manifest.json").write_text(
        json.dumps(
            {
                "config_sha256": "c0ffee",
                "embedding": {"provider": provider, "model": "text-embedding-3-small"},
                "counts": {"entities": entities},
                "files": {
                    "entities.parquet": {
                        "bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    },
                    "ATTRIBUTION.md": {
                        "bytes": len(attribution),
                        "sha256": hashlib.sha256(attribution).hexdigest(),
                    },
                },
            }
        )
    )


@pytest.fixture
def good(tmp_path: Path) -> Path:
    out = tmp_path / "out"
    out.mkdir()
    (out / "validation_report.json").write_text(json.dumps({"ok": True, "hard": {}}))
    (out / "README.md").write_text("---\nlicense: cc-by-sa-4.0\n---\n\n# card\n")
    write_tier(out, "t50", 50)
    write_tier(out, "t100", 100)
    return out


def test_clean_tier_set_passes(good: Path) -> None:
    assert check(good, quick=False) == []


def test_fake_embedder_is_refused(good: Path) -> None:
    write_tier(good, "t180", 180, provider="fake")
    fails = check(good, quick=False)
    assert any("test embedder" in f for f in fails)


def test_tampered_bytes_are_caught(good: Path) -> None:
    p = good / "t50" / "ATTRIBUTION.md"
    p.write_bytes(b"credit!\n")  # same length, different content
    assert any("sha256" in f for f in check(good, quick=False)), "hash drift must fail"
    assert check(good, quick=True) == [], "quick mode compares sizes only, by design"


def test_missing_file_is_caught(good: Path) -> None:
    (good / "t50" / "entities.parquet").unlink()
    assert any("missing" in f for f in check(good, quick=False))


def test_failed_validation_is_refused(good: Path) -> None:
    (good / "validation_report.json").write_text(json.dumps({"ok": False, "hard": {"nesting": False}}))
    fails = check(good, quick=False)
    assert any("nesting" in f for f in fails)


def test_card_without_licence_frontmatter_is_refused(good: Path) -> None:
    (good / "README.md").write_text("# no frontmatter\n")
    assert any("frontmatter" in f for f in check(good, quick=False))


def test_tiers_from_different_configs_are_refused(good: Path) -> None:
    m = json.loads((good / "t100" / "manifest.json").read_text())
    m["config_sha256"] = "decaf"
    (good / "t100" / "manifest.json").write_text(json.dumps(m))
    assert any("different configs" in f for f in check(good, quick=False))


def test_empty_directory_is_refused(tmp_path: Path) -> None:
    out = tmp_path / "out"
    out.mkdir()
    assert check(out, quick=False), "an empty out/ must never be publishable"
