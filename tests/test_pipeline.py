from __future__ import annotations

import json

import polars as pl
from rdflib import Graph

from oxid_dbpedia_ns.stages.emit import tier_dir


def test_validation_passed(built):
    report = json.loads((built["cfg"].out / "validation_report.json").read_text())
    failed = [h for h in report["hard"] if not h["ok"]]
    assert built["ok"], failed


def test_candidates_filter(built):
    df = pl.read_parquet(built["cfg"].work / "candidates.parquet")
    iris = set(df["iri"])
    for bad in (
        "List_of_scientists",
        "Foo_(disambiguation)",
        "Redirect_page",
        "Politician_1",
        "NoAbstract_1",
        "Short_1",
    ):
        assert f"http://dbpedia.org/resource/{bad}" not in iris, bad
    assert (
        "http://dbpedia.org/resource/Café_Scientist_3" in iris
    )  # percent-encoded abstract joined the decoded type row
    assert df.filter(pl.col("iri").str.ends_with("Café_Scientist_3"))["title"][0] == "Café Scientist 3"
    assert set(df["bucket"]) == {b.name for b in built["cfg"].buckets}


def test_geography_decisions(built):
    cfg = built["cfg"]
    geo = pl.read_parquet(cfg.work / "geo.parquet")
    rej = pl.read_parquet(cfg.work / "geo_rejected.parquet")
    passed = set(geo["iri"])
    rejected = dict(zip(rej["iri"], rej["reason"]))
    dbr = "http://dbpedia.org/resource/"
    assert dbr + "Mumbai" in rejected and rejected[dbr + "Mumbai"] == "non_target_country"
    assert dbr + "Amsterdam" in passed
    assert dbr + "New_York_City" in passed  # isPartOf -> New_York -> country -> United_States (two hops)
    assert dbr + "Kingdom_of_Prussia" in passed  # historic state mapped to Germany
    row = geo.filter(pl.col("iri") == dbr + "Kingdom_of_Prussia")
    assert row["countries"][0].to_list() == [dbr + "Germany"]
    # Works without a country inherit from their author.
    inherited = geo.filter(pl.col("resolved_via") == "author")
    assert inherited.height > 0
    # Entities with no geography pass only on editions.
    by_reason = dict(geo.group_by("pass_reason").len().iter_rows())
    assert by_reason.get("unresolved_editions", 0) > 0 and rejected  # both branches exercised
    assert "unresolved" in set(rejected.values())
    assert rejected[dbr + "India"] == "non_target_country"  # a country entity resolves to itself


def test_fixture_expectations_hold(built):
    from tests.fixtures.synthetic import Fixture

    # Rebuild the fixture spec deterministically to compare expectations (same seed, no writing).
    fx = Fixture(built["tmp"] / "unused")
    fx.pageviews = {"2025-01": [], "2025-02": []}
    fx.build_ontology()
    fx.build_places()
    fx.build_entities()
    geo = pl.read_parquet(built["cfg"].work / "geo.parquet")
    passed = set(geo["iri"])
    wrong = [
        e["name"]
        for e in fx.entities
        if (("http://dbpedia.org/resource/" + e["name"]) in passed) != e["expect_pass"]
    ]
    assert not wrong, wrong[:10]


def test_ranking_and_tiers(built):
    cfg = built["cfg"]
    ranked = pl.read_parquet(cfg.work / "ranked.parquet")
    assert ranked.height == cfg.top_tier
    assert ranked["rank"].to_list() == list(range(1, cfg.top_tier + 1))
    assert (ranked["views"] >= cfg["ranking"]["floor_views"]).all()
    assert (ranked["languages"] >= cfg["ranking"]["floor_languages"]).all()
    pre = json.loads((cfg.work / "preflight.json").read_text())
    assert {b["bucket"] for b in pre["buckets"]} == {b.name for b in cfg.buckets}
    rep = json.loads((cfg.work / "rank_report.json").read_text())
    assert "underflow_events" in rep


def test_tier_outputs(built):
    cfg = built["cfg"]
    for tier in cfg.tiers:
        d = tier_dir(cfg, tier)
        for name in (
            "entities.parquet",
            "edges.parquet",
            "abox.nt",
            "tbox.owl",
            "tbox.ttl",
            "tbox_removed.owl",
            "types_inferred.parquet",
            "oxid_tbox.txt",
            "oxid_abox.txt",
            "manifest.json",
            "ATTRIBUTION.md",
        ):
            assert (d / name).exists(), name
        ents = pl.read_parquet(d / "entities.parquet")
        assert ents.height == tier
        assert ents.schema["vector"] == pl.Array(pl.Float32, cfg["embedding"]["dimensions"])
        assert ents.schema["props"].__class__.__name__ == "Struct"
        abox = Graph().parse(str(d / "abox.nt"), format="nt")
        assert len(abox) == json.loads((d / "manifest.json").read_text())["counts"]["abox_triples"]
        lines = (d / "oxid_abox.txt").read_text().splitlines()
        assert any(line.startswith("INSTANCE http://dbpedia.org/resource/") for line in lines)
        tb = (d / "oxid_tbox.txt").read_text()
        assert "SUBCLASS http://dbpedia.org/ontology/City http://dbpedia.org/ontology/Settlement" in tb
        removed = (d / "tbox_removed.owl").read_text()
        assert "FunctionalProperty" in removed and "allValuesFrom" in removed
    assert (cfg.out / "README.md").read_text().startswith("---\nlicense: cc-by-sa-4.0")
    assert (cfg.out / "LICENSE").exists()


def test_props_are_typed(built):
    cfg = built["cfg"]
    ents = pl.read_parquet(tier_dir(cfg, cfg.top_tier) / "entities.parquet")
    fields = {f.name: f.dtype for f in ents.schema["props"].fields}
    assert fields["populationTotal"] == pl.Int64
    assert fields["areaTotal"] == pl.Float64
    assert fields["birthDate"] == pl.String
    assert fields["birthPlace_label"] == pl.List(pl.String)
    people = (
        ents.filter(pl.col("group") == "People")
        .select(pl.col("props").struct.field("birthDate"))
        .drop_nulls()
    )
    assert people.height > 0


def test_edges_are_inside_tier_and_grow(built):
    cfg = built["cfg"]
    sizes = []
    for tier in cfg.tiers:
        d = tier_dir(cfg, tier)
        ents = set(pl.read_parquet(d / "entities.parquet")["iri"])
        edges = pl.read_parquet(d / "edges.parquet")
        assert set(edges["s"]) <= ents and set(edges["o"]) <= ents
        sizes.append(edges.height)
    assert sizes == sorted(sizes)
