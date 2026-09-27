"""Stage 2: candidate pool and class filter.

Keeps every entity with an English abstract and a most-specific DBO type that maps
to a configured bucket. Drops lists, disambiguation pages and redirects.

Output: work/candidates.parquet  (iri, title, most_specific_type, bucket, group, abstract)
        work/candidates_report.json
"""

from __future__ import annotations

import polars as pl

from ..config import Config
from ..iri import DBO, DBR, RDF_TYPE, RDFS_COMMENT, title_from_iri
from ..ontology import assign_buckets, load_hierarchy
from ..util import log, timed, write_json, write_parquet_atomic


def run(cfg: Config, force: bool = False) -> None:
    pq = cfg.cache / "parquet"
    out = cfg.work / "candidates.parquet"
    cfg.work.mkdir(parents=True, exist_ok=True)
    report: dict = {}

    with timed("[2] ontology closure and bucket map"):
        h = load_hierarchy(pq / "ontology.parquet")
        class_bucket = assign_buckets(h, cfg.buckets)
        group_of = {b.name: b.group for b in cfg.buckets}
        report["classes"] = len(h.classes)
        report["classes_with_bucket"] = len(class_bucket)
        bucket_df = pl.DataFrame(
            {
                "most_specific_type": list(class_bucket.keys()),
                "bucket": list(class_bucket.values()),
            }
        ).with_columns(pl.col("bucket").replace_strict(group_of, return_dtype=pl.String).alias("group"))

    with timed("[2] types + abstracts"):
        types = (
            pl.scan_parquet(pq / "instance_types.parquet")
            .filter(
                (pl.col("p") == RDF_TYPE)
                & pl.col("o").str.starts_with(DBO)
                & pl.col("s").str.starts_with(DBR)
            )
            .select(pl.col("s").alias("iri"), pl.col("o").alias("most_specific_type"))
            .unique(subset=["iri"], keep="first")
        )
        abstracts = (
            pl.scan_parquet(pq / "short_abstracts.parquet")
            .filter(
                (pl.col("p") == RDFS_COMMENT) & (pl.col("lang") == "en") & pl.col("s").str.starts_with(DBR)
            )
            .select(
                pl.col("s").alias("iri"),
                pl.col("o").str.replace_all(r"\s+", " ").str.strip_chars().alias("abstract"),
            )
            .unique(subset=["iri"], keep="first")
        )
        disamb = pl.scan_parquet(pq / "disambiguations.parquet").select(pl.col("s").alias("iri")).unique()
        redirects = pl.scan_parquet(pq / "redirects.parquet").select(pl.col("s").alias("iri")).unique()

        pool = types.join(abstracts, on="iri", how="inner")
        n_typed_with_abstract = pool.select(pl.len()).collect().item()
        pool = pool.join(disamb, on="iri", how="anti").join(redirects, on="iri", how="anti")
        n_after_page_filter = pool.select(pl.len()).collect().item()

        min_chars = int(cfg["candidates"].get("abstract_min_chars", 0))
        prefixes = list(cfg["candidates"].get("drop_title_prefixes", []))
        local = pl.col("iri").str.slice(len(DBR))
        title_ok = pl.lit(True)
        for p in prefixes:
            title_ok = title_ok & ~local.str.starts_with(p)
        pool = pool.filter(title_ok & (pl.col("abstract").str.len_chars() >= min_chars))
        n_after_title_filter = pool.select(pl.len()).collect().item()

        pool = pool.join(bucket_df.lazy(), on="most_specific_type", how="inner")
        df = pool.collect(engine="streaming")

    df = df.with_columns(pl.col("iri").map_elements(title_from_iri, return_dtype=pl.String).alias("title"))
    df = df.select("iri", "title", "most_specific_type", "bucket", "group", "abstract").sort("iri")
    write_parquet_atomic(df, out)

    report.update(
        {
            "typed_with_abstract": n_typed_with_abstract,
            "after_disambiguation_redirect_filter": n_after_page_filter,
            "after_title_and_length_filter": n_after_title_filter,
            "candidates": df.height,
            "per_bucket": dict(sorted(df.group_by("bucket").len().iter_rows())),
        }
    )
    write_json(report, cfg.work / "candidates_report.json")
    log.info("[2] candidates: %d entities in %d buckets", df.height, df.get_column("bucket").n_unique())
