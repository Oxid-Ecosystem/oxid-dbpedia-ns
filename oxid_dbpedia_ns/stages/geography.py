"""Stage 3: geography filter.

Resolves each candidate to zero or more countries and keeps it when any resolved
country is in the target set. Unresolved entities pass when they have articles in
enough target-language editions.

Outputs: work/geo.parquet           (iri, countries, resolved_via, pass_reason)
         work/geo_rejected.parquet  (iri, bucket, reason, countries)
         work/languages.parquet     (iri, languages, target_editions)  -- reused by Stage 4
         work/geo_report.json
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import polars as pl

from ..config import Config
from ..iri import DBO, DBR, RDF_TYPE
from ..util import log, timed, write_json, write_parquet_atomic


def _load_country_map(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            out[DBR + row["iri"].strip()] = DBR + row["country"].strip()
    return out


def compute_languages(sameas_parquet: Path, target_editions: list[str]) -> pl.DataFrame:
    """Per English resource IRI: number of Wikipedia editions with an article and how many are target editions."""
    lf = pl.scan_parquet(sameas_parquet)
    en = (
        lf.filter(pl.col("lang") == "en")
        .select("q", (pl.lit(DBR) + pl.col("title")).alias("iri"))
        .unique(subset=["q"], keep="first")
    )
    counts = lf.group_by("q").agg(
        pl.col("lang").n_unique().alias("languages"),
        pl.col("lang").filter(pl.col("lang").is_in(target_editions)).n_unique().alias("target_editions"),
    )
    return (
        en.join(counts, on="q", how="inner")
        .select("iri", "languages", "target_editions")
        .collect(engine="streaming")
    )


class Resolver:
    """Walks property values up to a country IRI using in-memory adjacency maps."""

    def __init__(
        self,
        edges: dict[str, dict[str, list[str]]],
        countries_all: set[str],
        country_map: dict[str, str],
        walk_up: list[str],
        max_hops: int,
    ):
        self.edges = edges
        self.countries_all = countries_all
        self.country_map = country_map
        self.walk_up = walk_up
        self.max_hops = max_hops

    def canon_country(self, iri: str) -> str | None:
        iri = self.country_map.get(iri, iri)
        return iri if iri in self.countries_all else None

    def place_to_countries(self, place: str, hops: int = 0) -> set[str]:
        c = self.canon_country(place)
        if c:
            return {c}
        if hops >= self.max_hops:
            return set()
        found: set[str] = set()
        for p in self.walk_up:
            for nxt in self.edges.get(p, {}).get(place, ()):
                if nxt != place:
                    found |= self.place_to_countries(nxt, hops + 1)
            if found:
                break
        return found

    def resolve(
        self,
        iri: str,
        props: list[str],
        inherit_from: set[str],
        person_props: list[str],
        org_props: list[str],
        depth: int = 0,
    ) -> tuple[set[str], str | None]:
        """Returns (countries, property that resolved them)."""
        own = self.canon_country(iri)
        if own:  # the entity is itself a country (or a historic state mapped to one)
            return {own}, "self"
        for p in props:
            values = self.edges.get(p, {}).get(iri)
            if not values:
                continue
            found: set[str] = set()
            for v in values:
                if p in inherit_from:
                    if depth == 0:
                        c, _ = self.resolve(v, person_props, set(), person_props, org_props, depth + 1)
                        if not c:
                            c, _ = self.resolve(v, org_props, set(), person_props, org_props, depth + 1)
                        found |= c
                else:
                    found |= self.place_to_countries(v)
            if found:
                return found, p
        return set(), None


def run(cfg: Config, force: bool = False) -> None:
    pq = cfg.cache / "parquet"
    geo = cfg["geography"]
    resolution: dict[str, list[str]] = {
        k: list(v) for k, v in geo["resolution"].items() if isinstance(v, list)
    }
    inherit_from = set(geo["resolution"].get("inherit_from", []))
    walk_up = list(geo["resolution"].get("walk_up", ["country", "isPartOf"]))
    needed = {DBO + p for props in resolution.values() for p in props} | {DBO + p for p in walk_up}
    target = {DBR + c for c in geo["countries"]}
    country_map = _load_country_map(cfg.root / geo["country_map"])

    with timed("[3] languages per entity"):
        languages = compute_languages(pq / "sameas_all_wikis.parquet", list(geo["editions"]))
        write_parquet_atomic(languages, cfg.work / "languages.parquet")

    candidates = pl.read_parquet(cfg.work / "candidates.parquet", columns=["iri", "bucket", "group"])

    with timed("[3] load edges"):
        redirects = pl.scan_parquet(pq / "redirects.parquet").select(
            pl.col("s").alias("o"), pl.col("o").alias("target")
        )
        edges_df = (
            pl.scan_parquet(pq / "mappingbased_objects.parquet")
            .filter(pl.col("o_is_iri") & pl.col("p").is_in(list(needed)))
            .select("s", "p", "o")
            .join(redirects, on="o", how="left")
            .select("s", "p", pl.coalesce("target", "o").alias("o"))
            .collect(engine="streaming")
        )
        countries_all = (
            set(
                pl.scan_parquet(pq / "instance_types.parquet")
                .filter((pl.col("p") == RDF_TYPE) & (pl.col("o") == DBO + "Country"))
                .select("s")
                .collect()
                .get_column("s")
                .to_list()
            )
            | target
            | set(country_map.values())
        )
        edges: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
        for s, p, o in edges_df.iter_rows():
            edges[p[len(DBO) :]][s].append(o)
        log.info(
            "[3] %d edges over %d predicates; %d country IRIs known",
            edges_df.height,
            len(edges),
            len(countries_all),
        )

    resolver = Resolver(edges, countries_all, country_map, walk_up, int(geo.get("max_hops", 3)))
    person_props = resolution.get("People", [])
    org_props = resolution.get("Organisations", [])
    min_editions = int(geo.get("unresolved_min_editions", 4))
    editions_of = dict(
        zip(languages.get_column("iri").to_list(), languages.get_column("target_editions").to_list())
    )

    with timed("[3] resolve"):
        rows_pass: list[tuple] = []
        rows_reject: list[tuple] = []
        for iri, bucket, group in candidates.iter_rows():
            countries, via = resolver.resolve(
                iri, resolution.get(group, []), inherit_from, person_props, org_props
            )
            hit = sorted(c for c in countries if c in target)
            if hit:
                rows_pass.append((iri, hit, via, "target_country"))
            elif countries:
                rows_reject.append((iri, bucket, "non_target_country", sorted(countries)))
            elif editions_of.get(iri, 0) >= min_editions:
                rows_pass.append((iri, [], None, "unresolved_editions"))
            else:
                rows_reject.append((iri, bucket, "unresolved", []))

    passed = pl.DataFrame(
        rows_pass,
        schema={
            "iri": pl.String,
            "countries": pl.List(pl.String),
            "resolved_via": pl.String,
            "pass_reason": pl.String,
        },
        orient="row",
    ).sort("iri")
    rejected = pl.DataFrame(
        rows_reject,
        schema={"iri": pl.String, "bucket": pl.String, "reason": pl.String, "countries": pl.List(pl.String)},
        orient="row",
    ).sort("iri")
    write_parquet_atomic(passed, cfg.work / "geo.parquet")
    write_parquet_atomic(rejected, cfg.work / "geo_rejected.parquet")

    report = {
        "candidates": candidates.height,
        "passed": passed.height,
        "passed_by_reason": dict(sorted(passed.group_by("pass_reason").len().iter_rows())),
        "rejected": rejected.height,
        "rejected_by_reason": dict(sorted(rejected.group_by("reason").len().iter_rows())),
        "passed_by_country": dict(
            sorted(
                passed.explode("countries", empty_as_null=False)
                .drop_nulls("countries")
                .group_by("countries")
                .len()
                .iter_rows(),
                key=lambda r: -r[1],
            )
        ),
        "top_non_target_countries": dict(
            sorted(
                rejected.filter(pl.col("reason") == "non_target_country")
                .explode("countries", empty_as_null=False)
                .group_by("countries")
                .len()
                .iter_rows(),
                key=lambda r: -r[1],
            )[:40]
        ),
    }
    write_json(report, cfg.work / "geo_report.json")
    log.info("[3] geography: %d passed, %d rejected", passed.height, rejected.height)
