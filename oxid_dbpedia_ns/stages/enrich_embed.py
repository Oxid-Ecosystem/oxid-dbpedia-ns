"""Stage 5: abstracts, properties, edges and embeddings for the top tier.

Runs on the top tier only; smaller tiers are cut from the result in Stage 6.

Outputs: work/enriched.parquet     one row per top-tier entity, vector included
         work/edges_top.parquet    s, p, o edges with both endpoints in the top tier
         work/embedding_manifest.json
"""

from __future__ import annotations

import polars as pl

from ..config import Config
from ..embeddings import Embedder
from ..iri import DBO, GEO_LAT, GEO_LONG, title_from_iri
from ..util import log, timed, write_json, write_parquet_atomic

_SPECIAL_LITERALS = {"geo:lat": GEO_LAT, "geo:long": GEO_LONG}


def literal_iri(name: str) -> str:
    return _SPECIAL_LITERALS.get(name, DBO + name)


def _cast(col: pl.Expr, kind: str) -> pl.Expr:
    if kind == "int":
        return col.str.strip_chars().str.replace_all(r"\.0+$", "").cast(pl.Int64, strict=False)
    if kind == "float":
        return col.cast(pl.Float64, strict=False)
    return col


def run(cfg: Config, force: bool = False) -> None:
    pq = cfg.cache / "parquet"
    props_cfg = cfg["properties"]
    type_of = {k: v for k, v in props_cfg.get("types", {}).items()}
    groups = {g: v for g, v in props_cfg.items() if g != "types"}

    with timed("[5] assemble top tier"):
        ranked = pl.read_parquet(cfg.work / "ranked.parquet")
        cand = pl.read_parquet(cfg.work / "candidates.parquet")
        geo = pl.read_parquet(cfg.work / "geo.parquet", columns=["iri", "countries"])
        base = (
            ranked.join(cand.drop("bucket"), on="iri", how="left")
            .join(geo, on="iri", how="left")
            .with_columns((pl.col("title") + "\n" + pl.col("abstract")).alias("embed_text"))
            .sort("rank")
        )
        top_iris = base.get_column("iri")
        in_top = pl.DataFrame({"iri": top_iris})
        group_of = dict(zip(base.get_column("iri").to_list(), base.get_column("group").to_list()))

    # --- literals ------------------------------------------------------------
    with timed("[5] literals"):
        lit_names = sorted({n for g in groups.values() for n in g.get("literals", [])})
        lit_map = {literal_iri(n): n for n in lit_names}
        lits = (
            pl.scan_parquet(pq / "mappingbased_literals.parquet")
            .filter(~pl.col("o_is_iri") & pl.col("p").is_in(list(lit_map)))
            .join(in_top.lazy(), left_on="s", right_on="iri", how="semi")
            .select(
                pl.col("s").alias("iri"),
                pl.col("p").replace_strict(lit_map, return_dtype=pl.String).alias("name"),
                pl.col("o").alias("value"),
            )
            .unique(subset=["iri", "name"], keep="first")
            .collect(engine="streaming")
        )
        # Keep only the literals whitelisted for the entity's group.
        allowed = pl.DataFrame(
            [(g, n) for g, spec in groups.items() for n in spec.get("literals", [])],
            schema={"group": pl.String, "name": pl.String},
            orient="row",
        )
        lits = lits.with_columns(
            pl.col("iri").replace_strict(group_of, return_dtype=pl.String).alias("group")
        ).join(allowed, on=["group", "name"], how="semi")
        lit_wide = (
            lits.pivot(on="name", index="iri", values="value")
            if lits.height
            else pl.DataFrame({"iri": []}, schema={"iri": pl.String})
        )
        for n in lit_names:
            if n not in lit_wide.columns:
                lit_wide = lit_wide.with_columns(pl.lit(None, dtype=pl.String).alias(n))
        lit_wide = lit_wide.with_columns(
            [_cast(pl.col(n), type_of.get(n, "string")).alias(n) for n in lit_names]
        )

    # --- object properties ---------------------------------------------------
    with timed("[5] object properties and edges"):
        obj_names = sorted({n for g in groups.values() for n in g.get("objects", [])})
        obj_map = {DBO + n: n for n in obj_names}
        redirects = pl.scan_parquet(pq / "redirects.parquet").select(
            pl.col("s").alias("o"), pl.col("o").alias("target")
        )
        objs = (
            pl.scan_parquet(pq / "mappingbased_objects.parquet")
            .filter(pl.col("o_is_iri") & pl.col("p").is_in(list(obj_map)))
            .join(in_top.lazy(), left_on="s", right_on="iri", how="semi")
            .join(redirects, on="o", how="left")
            .select(
                pl.col("s"),
                pl.col("p").replace_strict(obj_map, return_dtype=pl.String).alias("name"),
                pl.coalesce("target", "o").alias("o"),
            )
            .unique()
            .collect(engine="streaming")
        )
        allowed_o = pl.DataFrame(
            [(g, n) for g, spec in groups.items() for n in spec.get("objects", [])],
            schema={"group": pl.String, "name": pl.String},
            orient="row",
        )
        objs = objs.with_columns(
            pl.col("s").replace_strict(group_of, return_dtype=pl.String).alias("group")
        ).join(allowed_o, on=["group", "name"], how="semi")
        objs = objs.filter(pl.col("s") != pl.col("o"))
        edges = (
            objs.join(in_top, left_on="o", right_on="iri", how="semi")
            .select(pl.col("s"), (pl.lit(DBO) + pl.col("name")).alias("p"), pl.col("o"))
            .unique()
            .sort(["s", "p", "o"])
        )
        labels = (
            objs.with_columns(pl.col("o").map_elements(title_from_iri, return_dtype=pl.String).alias("label"))
            .group_by("s", "name")
            .agg(pl.col("label").sort().alias("labels"))
            .with_columns((pl.col("name") + "_label").alias("name"))
        )
        label_wide = (
            labels.pivot(on="name", index="s", values="labels").rename({"s": "iri"})
            if labels.height
            else pl.DataFrame({"iri": []}, schema={"iri": pl.String})
        )
        for n in obj_names:
            col = n + "_label"
            if col not in label_wide.columns:
                label_wide = label_wide.with_columns(pl.lit(None, dtype=pl.List(pl.String)).alias(col))
        write_parquet_atomic(edges, cfg.work / "edges_top.parquet")
        log.info("[5] %d edges inside the top tier, %d object values with labels", edges.height, objs.height)

    props_cols = lit_names + [n + "_label" for n in obj_names]
    enriched = (
        base.join(lit_wide, on="iri", how="left")
        .join(label_wide, on="iri", how="left")
        .with_columns(pl.struct(props_cols).alias("props"))
        .drop(props_cols)
    )

    # --- embeddings --------------------------------------------------------------
    with timed("[5] embeddings"):
        embedder = Embedder(dict(cfg["embedding"]), cfg.work)
        vectors, info = embedder.embed(
            enriched.get_column("iri").to_list(), enriched.get_column("embed_text").to_list()
        )
        write_json(info, cfg.work / "embedding_manifest.json")
        enriched = enriched.join(vectors, on="iri", how="left")

    enriched = enriched.with_columns(
        pl.col("most_specific_type")
        .map_elements(lambda t: [t], return_dtype=pl.List(pl.String))
        .alias("types"),
        pl.col("countries")
        .fill_null([])
        .list.eval(pl.element().str.slice(len("http://dbpedia.org/resource/"))),
    ).select(
        "iri",
        "rank",
        "tier_min",
        "title",
        "abstract",
        "embed_text",
        "bucket",
        "group",
        "types",
        "countries",
        "props",
        "score",
        "views",
        "languages",
        "vector",
    )
    write_parquet_atomic(enriched, cfg.work / "enriched.parquet")
    log.info("[5] enriched %d entities (%s, %d tokens)", enriched.height, info["model"], info["total_tokens"])
