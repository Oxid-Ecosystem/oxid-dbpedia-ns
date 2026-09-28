"""Stage 7: validation. Hard checks fail the build; soft checks are printed for review.

Output: out/validation_report.json
"""

from __future__ import annotations

import itertools
import json
import random
import re

import numpy as np
import polars as pl
from rdflib import Graph
from rdflib.namespace import OWL, RDF

from ..config import Config
from ..iri import DBO
from ..ontology import hierarchy_from_graph
from ..util import log, sha256_file, write_json
from .emit import tier_dir

IRI_PATTERN = re.compile(r"^http://dbpedia\.org/resource/[^\s<>\"{}|\\^`]+$")


class Report:
    def __init__(self) -> None:
        self.hard: list[dict] = []
        self.soft: list[dict] = []

    def check(self, name: str, ok: bool, detail: str = "") -> None:
        self.hard.append({"check": name, "ok": bool(ok), "detail": detail})
        log.log(20 if ok else 40, "[7] %s %s %s", "PASS" if ok else "FAIL", name, detail)

    def note(self, name: str, detail, flag: bool = False) -> None:
        self.soft.append({"check": name, "flag": flag, "detail": detail})
        log.log(
            30 if flag else 20,
            "[7] %s %s: %s",
            "FLAG" if flag else "info",
            name,
            json.dumps(detail, ensure_ascii=False)[:400],
        )

    @property
    def ok(self) -> bool:
        return all(h["ok"] for h in self.hard)


def _vectors(df: pl.DataFrame) -> np.ndarray:
    return df.get_column("vector").to_numpy().astype(np.float32)


def run(cfg: Config, force: bool = False) -> bool:
    rep = Report()
    v = cfg["validation"]
    tiers = cfg.tiers
    dirs = {t: tier_dir(cfg, t) for t in tiers}
    ents = {t: pl.read_parquet(dirs[t] / "entities.parquet") for t in tiers}
    edges = {t: pl.read_parquet(dirs[t] / "edges.parquet") for t in tiers}
    dims = int(cfg["embedding"]["dimensions"])

    # --- hard checks --------------------------------------------------------------
    for t in tiers:
        rep.check(f"{dirs[t].name}: size", ents[t].height == t, f"{ents[t].height} rows, expected {t}")
        rep.check(f"{dirs[t].name}: unique IRIs", ents[t].get_column("iri").n_unique() == ents[t].height)
        bad = [i for i in ents[t].get_column("iri").to_list() if not IRI_PATTERN.match(i)]
        rep.check(f"{dirs[t].name}: canonical IRIs", not bad, f"{len(bad)} non-canonical, e.g. {bad[:3]}")
        rep.check(
            f"{dirs[t].name}: ranks are 1..N", ents[t].get_column("rank").to_list() == list(range(1, t + 1))
        )
        rep.check(
            f"{dirs[t].name}: non-empty abstracts", (ents[t].get_column("abstract").str.len_chars() > 0).all()
        )
        rep.check(f"{dirs[t].name}: at least one type", (ents[t].get_column("types").list.len() > 0).all())
        vec = _vectors(ents[t])
        norms = np.linalg.norm(vec, axis=1)
        rep.check(f"{dirs[t].name}: vector dims", vec.shape == (t, dims), str(vec.shape))
        rep.check(
            f"{dirs[t].name}: unit norm within 1e-3",
            bool(np.all(np.abs(norms - 1.0) < 1e-3)),
            f"max deviation {np.max(np.abs(norms - 1.0)):.2e}",
        )
        members = set(ents[t].get_column("iri").to_list())
        dangling = (
            edges[t].filter(~pl.col("s").is_in(list(members)) | ~pl.col("o").is_in(list(members))).height
        )
        rep.check(f"{dirs[t].name}: edge endpoints in tier", dangling == 0, f"{dangling} dangling")
        # Types in abox exist in tbox.
        tbox = Graph().parse(str(dirs[t] / "tbox.owl"), format="xml")
        classes = {str(s) for s in tbox.subjects(RDF.type, OWL.Class)}
        used = {x for types in ents[t].get_column("types").to_list() for x in types}
        missing = used - classes
        rep.check(
            f"{dirs[t].name}: asserted types exist in tbox.owl", not missing, f"missing {sorted(missing)[:5]}"
        )
        # The TBox counts quoted in the docs are recounted from the shipped file, so a manifest
        # can never drift from the bytes it describes. hierarchy_from_graph also reaches classes
        # that DBO mentions only through an equivalence, so it is >= the declared count.
        manifest = json.loads((dirs[t] / "manifest.json").read_text())
        tb = manifest["tbox"]
        hierarchy = hierarchy_from_graph(tbox)
        recount = {
            "classes_declared": len({c for c in classes if c.startswith(DBO)}),
            "hierarchy_nodes": len(hierarchy.classes),
            "subsumptions": sum(
                1
                for c in hierarchy.classes
                for a in hierarchy.ancestors(c)
                if a not in (str(OWL.Thing), DBO + "Thing")
            ),
        }
        drifted = {k: (tb.get(k), v) for k, v in recount.items() if tb.get(k) != v}
        rep.check(f"{dirs[t].name}: manifest tbox counts match tbox.owl", not drifted, f"{drifted}")
        rep.check(
            f"{dirs[t].name}: hierarchy_nodes >= classes_declared",
            recount["hierarchy_nodes"] >= recount["classes_declared"],
            f"{recount['hierarchy_nodes']} vs {recount['classes_declared']}",
        )
        mismatched = [
            n for n, info in manifest["files"].items() if sha256_file(dirs[t] / n) != info["sha256"]
        ]
        rep.check(f"{dirs[t].name}: manifest checksums", not mismatched, f"mismatch {mismatched}")

    # Nesting and identical vectors.
    for small, big in itertools.pairwise(tiers):
        a, b = ents[small], ents[big]
        prefix = b.head(small)
        same_iris = a.get_column("iri").to_list() == prefix.get_column("iri").to_list()
        rep.check(f"nesting: {dirs[small].name} is the prefix of {dirs[big].name}", same_iris)
        same_vec = np.array_equal(_vectors(a), _vectors(prefix))
        rep.check(f"vectors identical: {dirs[small].name} vs {dirs[big].name}", same_vec)

    # --- soft checks --------------------------------------------------------------
    tol = float(v.get("share_tolerance", 0.01))
    for t in tiers:
        counts = dict(ents[t].group_by("bucket").len().iter_rows())
        off = {
            b.name: round(counts.get(b.name, 0) / t - b.share, 4)
            for b in cfg.buckets
            if abs(counts.get(b.name, 0) / t - b.share) > tol
        }
        rep.note(f"{dirs[t].name}: bucket shares off by more than {tol}", off, flag=bool(off))
        by_country = dict(
            sorted(
                ents[t]
                .explode("countries", empty_as_null=False)
                .drop_nulls("countries")
                .group_by("countries")
                .len()
                .iter_rows(),
                key=lambda r: -r[1],
            )
        )
        top_country = v.get("max_country_share_country", "United_States")
        share = by_country.get(top_country, 0) / t
        rep.note(
            f"{dirs[t].name}: {top_country} share",
            round(share, 3),
            flag=share > float(v.get("max_country_share", 0.5)),
        )
        rep.note(f"{dirs[t].name}: country distribution", dict(list(by_country.items())[:12]))

    top = ents[tiers[-1]]
    rng = random.Random(0)
    heads = {b: g.sort("rank").head(20).get_column("title").to_list() for b, g in top.group_by("bucket")}
    rep.note(
        "top 20 per bucket (household names expected)",
        {k[0] if isinstance(k, tuple) else k: v[:20] for k, v in heads.items()},
    )
    tail = top.filter(pl.col("rank") > max(0, tiers[-1] - 10000))
    rep.note(
        "tail sample (ranks in the last 10K)",
        rng.sample(tail.get_column("title").to_list(), min(50, tail.height)),
    )
    rep.note(
        "leakage sample (check by eye for out-of-region entities)",
        rng.sample(top.select("title", "countries").rows(), min(200, top.height))[:200],
    )

    # Semantic smoke test: nearest neighbours of the probes.
    vec = _vectors(top)
    titles = top.get_column("title").to_list()
    index = {t: i for i, t in enumerate(titles)}
    k = int(v.get("neighbours", 10))
    nn: dict[str, list[str]] = {}
    for probe in v.get("probes", []):
        i = index.get(probe)
        if i is None:
            nn[probe] = ["<probe not in dataset>"]
            continue
        sims = vec @ vec[i]
        order = np.argsort(-sims)[1 : k + 1]
        nn[probe] = [f"{titles[j]} ({sims[j]:.3f})" for j in order]
    rep.note("semantic smoke test: nearest neighbours", nn)

    # Inferred types agree with the closure file.
    inferred = pl.read_parquet(dirs[tiers[-1]] / "types_inferred.parquet")
    rep.note(
        "neurosymbolic smoke test",
        "run loader/oxiddb_load.py --smoke against an OxidDB instance; types_inferred.parquet is the oracle",
        flag=False,
    )
    rep.note("inferred type assertions in top tier", inferred.height)

    write_json({"ok": rep.ok, "hard": rep.hard, "soft": rep.soft}, cfg.out / "validation_report.json")
    log.info("[7] validation %s", "PASSED" if rep.ok else "FAILED")
    return rep.ok
