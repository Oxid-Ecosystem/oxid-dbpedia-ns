"""Stage 4: notability ranking and nested tiers.

score = w_v * z(log1p(views)) + w_l * z(log1p(languages)), z within bucket.
The global order is a quota-preserving interleave over the per-bucket orders.
Tiers are prefixes of that order, so nesting holds by construction.

Outputs: work/ranked.parquet   (rank, iri, bucket, score, views, languages, tier_min)
         work/preflight.json   pool sizes above the floor vs demand, per bucket
         work/rank_report.json underflow events and per-tier bucket counts
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ..config import Bucket, Config
from ..iri import DBR
from ..util import log, timed, write_json, write_parquet_atomic


def _en_titles(sameas: pl.LazyFrame) -> pl.LazyFrame:
    """q -> English resource IRI (one per q)."""
    return (
        sameas.filter(pl.col("lang") == "en")
        .select("q", (pl.lit(DBR) + pl.col("title")).alias("iri"))
        .unique(subset=["q"], keep="first")
    )


def needed_titles(entities: pl.LazyFrame, sameas: pl.LazyFrame) -> pl.DataFrame:
    """(lang, title) pairs whose pageviews feed the ranking: the English title of every entity
    plus the titles of its sister articles in the other editions (via the Wikidata Q)."""
    en = entities.select(pl.lit("en").alias("lang"), pl.col("iri").str.slice(len(DBR)).alias("title"))
    q_of = _en_titles(sameas).join(entities.select("iri"), on="iri", how="inner").select("q")
    other = sameas.filter(pl.col("lang") != "en").join(q_of, on="q", how="inner").select("lang", "title")
    return pl.concat([en, other]).unique().collect(engine="streaming")


def sum_month_views(month_files: list[Path], needed: pl.DataFrame) -> pl.DataFrame:
    """Sum (lang, title, views) over the monthly pageview files, one file at a time, keeping only
    the rows in `needed`. Summing all ~1e9 unfiltered rows in one group_by needs more RAM than a
    laptop has (it crashed a 48 GB machine); after the semi-join each month is a few million rows."""
    parts = []
    for f in month_files:
        with timed(f"[4] pageviews {f.stem}"):
            parts.append(
                pl.scan_parquet(f)
                .join(needed.lazy(), on=["lang", "title"], how="semi")
                .group_by("lang", "title")
                .agg(pl.col("views").sum())
                .collect(engine="streaming")
            )
    if not parts:
        return pl.DataFrame(schema={"lang": pl.String, "title": pl.String, "views": pl.Int64})
    return pl.concat(parts).group_by("lang", "title").agg(pl.col("views").sum())


def views_per_entity(pageviews: pl.LazyFrame, sameas: pl.LazyFrame) -> pl.LazyFrame:
    """Sum pageviews over English and the other target editions, keyed on the English resource IRI."""
    en_views = pageviews.filter(pl.col("lang") == "en").select(
        (pl.lit(DBR) + pl.col("title")).alias("iri"), "views"
    )
    other = (
        pageviews.filter(pl.col("lang") != "en")
        .join(sameas.filter(pl.col("lang") != "en"), on=["lang", "title"], how="inner")
        .join(_en_titles(sameas), on="q", how="inner")
        .select("iri", "views")
    )
    return pl.concat([en_views, other]).group_by("iri").agg(pl.col("views").sum())


@dataclass
class Interleaved:
    order: list[tuple[str, str]]  # (iri, bucket)
    underflow: list[dict]


def interleave(pools: dict[str, list[str]], buckets: list[Bucket], n: int) -> Interleaved:
    """Deficit-round-robin: at step k pick the bucket with the largest share*k - taken."""
    order: list[tuple[str, str]] = []
    taken = {b.name: 0 for b in buckets}
    pos = {b.name: 0 for b in buckets}
    active = [b for b in buckets if pools.get(b.name)]
    underflow: list[dict] = []
    for b in buckets:
        if not pools.get(b.name):
            underflow.append({"bucket": b.name, "step": 0, "note": "empty pool"})
    k = 0
    while len(order) < n and active:
        k += 1
        best = max(active, key=lambda b: (b.share * k - taken[b.name], -buckets.index(b)))
        pool = pools[best.name]
        order.append((pool[pos[best.name]], best.name))
        pos[best.name] += 1
        taken[best.name] += 1
        if pos[best.name] >= len(pool):
            active.remove(best)
            underflow.append({"bucket": best.name, "step": k, "taken": taken[best.name]})
    return Interleaved(order=order, underflow=underflow)


def run(cfg: Config, force: bool = False, allow_short: bool = False) -> None:
    r = cfg["ranking"]
    w_v, w_l = float(r["weight_views"]), float(r["weight_languages"])
    floor_v, floor_l = int(r["floor_views"]), int(r["floor_languages"])
    top = cfg.top_tier

    cand = pl.scan_parquet(cfg.work / "candidates.parquet").select("iri", "bucket")
    passed = pl.scan_parquet(cfg.work / "geo.parquet").select("iri")
    langs = pl.scan_parquet(cfg.work / "languages.parquet").select("iri", "languages")
    sameas = pl.scan_parquet(cfg.cache / "parquet" / "sameas_all_wikis.parquet")
    month_files = [cfg.cache / "pageviews" / f"{m}.parquet" for m in cfg["pageviews"]["months"]]
    missing = [f.name for f in month_files if not f.exists()]
    if missing:
        raise RuntimeError(
            f"missing monthly pageview files in {cfg.cache / 'pageviews'}: {missing} (run acquire)"
        )

    with timed("[4] pageview titles needed"):
        needed = needed_titles(cand.join(passed, on="iri", how="inner"), sameas)
        log.info("[4] %d (lang, title) pairs needed from the pageview files", needed.height)

    with timed("[4] sum pageviews over months"):
        views = views_per_entity(sum_month_views(month_files, needed).lazy(), sameas)

    with timed("[4] join signals"):
        df = (
            cand.join(passed, on="iri", how="inner")
            .join(views, on="iri", how="left")
            .join(langs, on="iri", how="left")
            .with_columns(
                pl.col("views").fill_null(0).cast(pl.Int64), pl.col("languages").fill_null(1).cast(pl.Int32)
            )
            .collect(engine="streaming")
        )

    with timed("[4] score"):
        df = df.with_columns(
            (pl.col("views").cast(pl.Float64) + 1).log().alias("lv"),
            (pl.col("languages").cast(pl.Float64) + 1).log().alias("ll"),
        )

        def z(c: str) -> pl.Expr:
            std = pl.when(pl.col(c).std() > 0).then(pl.col(c).std()).otherwise(1.0)
            return ((pl.col(c) - pl.col(c).mean()) / std).over("bucket")

        df = df.with_columns((w_v * z("lv") + w_l * z("ll")).alias("score")).drop("lv", "ll")
        df = df.with_columns(
            ((pl.col("views") >= floor_v) & (pl.col("languages") >= floor_l)).alias("above_floor")
        )

    # Pre-flight: pool above floor vs demand.
    preflight = []
    shortfall = False
    for b in cfg.buckets:
        sub = df.filter(pl.col("bucket") == b.name)
        above = int(sub.get_column("above_floor").sum())
        demand = round(b.share * top)
        preflight.append(
            {
                "bucket": b.name,
                "share": round(b.share, 4),
                "pool": sub.height,
                "above_floor": above,
                "demand_top_tier": demand,
                "shortfall": max(0, demand - above),
            }
        )
        shortfall |= above < demand
    write_json(
        {"floor_views": floor_v, "floor_languages": floor_l, "top_tier": top, "buckets": preflight},
        cfg.work / "preflight.json",
    )
    log.info("[4] pre-flight (bucket: above_floor / demand):")
    for p in preflight:
        flag = "  SHORT" if p["shortfall"] else ""
        log.info("    %-18s %8d / %8d%s", p["bucket"], p["above_floor"], p["demand_top_tier"], flag)
    if shortfall:
        log.warning(
            "[4] some buckets cannot meet their %d-tier demand; their share is redistributed (see rank_report.json)",
            top,
        )

    with timed("[4] interleave"):
        eligible = df.filter(pl.col("above_floor")).sort(
            ["bucket", "score", "iri"], descending=[False, True, False]
        )
        pools = (
            {
                name: g.get_column("iri").to_list()
                for name, g in eligible.group_by("bucket", maintain_order=True).__iter__()
            }
            if eligible.height
            else {}
        )
        pools = {k[0] if isinstance(k, tuple) else k: v for k, v in pools.items()}
        result = interleave(pools, cfg.buckets, top)
        if len(result.order) < top:
            msg = f"only {len(result.order)} entities available above the floor for a top tier of {top}"
            if not allow_short:
                raise RuntimeError(msg + " (lower the floor, widen the geography, or pass --allow-short)")
            log.warning("[4] %s", msg)

    ranked = pl.DataFrame({"iri": [o[0] for o in result.order]}).with_row_index("rank", offset=1)
    ranked = ranked.join(df.select("iri", "bucket", "score", "views", "languages"), on="iri", how="left")
    tier_expr = pl.lit(None, dtype=pl.Int32)
    for t in reversed(cfg.tiers):
        tier_expr = pl.when(pl.col("rank") <= t).then(pl.lit(t, dtype=pl.Int32)).otherwise(tier_expr)
    ranked = ranked.with_columns(pl.col("rank").cast(pl.Int32), tier_expr.alias("tier_min")).select(
        "rank", "iri", "bucket", "score", "views", "languages", "tier_min"
    )
    write_parquet_atomic(ranked, cfg.work / "ranked.parquet")

    per_tier = {
        str(t): dict(sorted(ranked.filter(pl.col("rank") <= t).group_by("bucket").len().iter_rows()))
        for t in cfg.tiers
    }
    write_json(
        {"ranked": ranked.height, "underflow_events": result.underflow, "per_tier_bucket_counts": per_tier},
        cfg.work / "rank_report.json",
    )
    log.info("[4] ranked %d entities; %d underflow events", ranked.height, len(result.underflow))
