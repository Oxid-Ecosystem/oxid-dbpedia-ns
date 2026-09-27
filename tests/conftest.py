from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from oxid_dbpedia_ns.config import load_config
from oxid_dbpedia_ns.util import setup_logging

from .fixtures.synthetic import Fixture, write_test_config

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def built(tmp_path_factory) -> dict:
    """Run the whole pipeline once on the synthetic fixture and hand out the config + paths."""
    setup_logging()
    tmp = tmp_path_factory.mktemp("build")
    files = Fixture(tmp / "fixture").write()
    shutil.copyfile(REPO / "LICENSE-DATA", tmp / "LICENSE-DATA")
    cfg_path = write_test_config(REPO, files, tmp)
    cfg = load_config(cfg_path)
    # emit copies LICENSE-DATA from cfg.root, which is tmp here.
    from oxid_dbpedia_ns.stages import acquire, candidates, emit, enrich_embed, geography, rank, validate

    acquire.run(cfg)
    candidates.run(cfg)
    geography.run(cfg)
    rank.run(cfg)
    enrich_embed.run(cfg)
    emit.run(cfg)
    ok = validate.run(cfg)
    return {"cfg": cfg, "tmp": tmp, "ok": ok, "fixture_files": files}
