"""Stage 6: emit one self-contained directory per tier.

out/<tier>/
  entities.parquet        one row per entity, vector included
  edges.parquet           s, p, o between entities in this tier
  abox.nt                 rdf:type + rdfs:label + edges + literals, N-Triples
  tbox.owl                DBO filtered to OWL 2 EL (RDF/XML); tbox.ttl / tbox.nt are the same graph
  tbox_removed.owl        axioms stripped for EL compliance
  types_inferred.parquet  subclass closure of the asserted types, for validation
  oxid_tbox.txt           OxidDB line import: SUBCLASS lines
  oxid_abox.txt           OxidDB line import: INSTANCE and PROPERTY lines
  manifest.json
  ATTRIBUTION.md
out/README.md             dataset card (Hugging Face front matter)
out/LICENSE               CC BY-SA 4.0 notice
"""

from __future__ import annotations

import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from rdflib import Graph

from .. import __version__
from ..config import Config
from ..iri import DBO, RDF_TYPE, RDFS_LABEL
from ..ntriples import format_iri, format_literal
from ..ontology import clean_tbox, el_filter, hierarchy_from_graph, load_ontology_graph
from ..util import log, read_json, sha256_file, timed, write_json, write_parquet_atomic
from .enrich_embed import literal_iri

XSD = "http://www.w3.org/2001/XMLSchema#"
_DATE_RE = re.compile(r"^-?\d{4}-\d{2}-\d{2}$")
_YEAR_RE = re.compile(r"^-?\d{4}$")


def tier_dir(cfg: Config, tier: int) -> Path:
    return cfg.out / f"t{tier // 1000}" if tier % 1000 == 0 else cfg.out / f"t{tier}"


def _literal_line(s: str, name: str, value, kind: str) -> str:
    p = literal_iri(name)
    if kind == "int":
        lit = format_literal(str(int(value)), datatype=XSD + "integer")
    elif kind == "float":
        lit = format_literal(repr(float(value)), datatype=XSD + "double")
    else:
        v = str(value)
        if _DATE_RE.match(v):
            lit = format_literal(v, datatype=XSD + "date")
        elif _YEAR_RE.match(v):
            lit = format_literal(v, datatype=XSD + "gYear")
        else:
            lit = format_literal(v)
    return f"{format_iri(s)} {format_iri(p)} {lit} .\n"


def write_abox(
    entities: pl.DataFrame, edges: pl.DataFrame, path: Path, literal_names: list[str], type_of: dict[str, str]
) -> int:
    n = 0
    with open(path, "w", encoding="utf-8") as f:
        for row in entities.select("iri", "title", "types", "props").iter_rows(named=True):
            s = format_iri(row["iri"])
            for t in row["types"]:
                f.write(f"{s} {format_iri(RDF_TYPE)} {format_iri(t)} .\n")
                n += 1
            f.write(f"{s} {format_iri(RDFS_LABEL)} {format_literal(row['title'], lang='en')} .\n")
            n += 1
            props = row["props"] or {}
            for name in literal_names:
                v = props.get(name)
                if v is None:
                    continue
                f.write(_literal_line(row["iri"], name, v, type_of.get(name, "string")))
                n += 1
        for s, p, o in edges.iter_rows():
            f.write(f"{format_iri(s)} {format_iri(p)} {format_iri(o)} .\n")
            n += 1
    return n


def write_oxid_files(entities: pl.DataFrame, edges: pl.DataFrame, tbox: Graph, out_dir: Path) -> dict:
    """OxidDB line-oriented import (POST /import/csv): SUBCLASS / INSTANCE / PROPERTY."""
    from rdflib import URIRef
    from rdflib.namespace import RDFS

    n_sub = 0
    with open(out_dir / "oxid_tbox.txt", "w", encoding="utf-8") as f:
        f.write("# OxidDB import: DBO subclass axioms (OWL 2 EL subset)\n")
        for s, o in sorted(tbox.subject_objects(RDFS.subClassOf)):
            if (
                isinstance(s, URIRef)
                and isinstance(o, URIRef)
                and str(s).startswith(DBO)
                and str(o).startswith(DBO)
            ):
                f.write(f"SUBCLASS {s} {o}\n")
                n_sub += 1
    n_inst = n_prop = 0
    with open(out_dir / "oxid_abox.txt", "w", encoding="utf-8") as f:
        f.write("# OxidDB import: asserted types and object edges\n")
        for iri, types in entities.select("iri", "types").iter_rows():
            for t in types:
                f.write(f"INSTANCE {iri} {t}\n")
                n_inst += 1
        for s, p, o in edges.iter_rows():
            f.write(f"PROPERTY {s} {p} {o}\n")
            n_prop += 1
    return {"subclass_lines": n_sub, "instance_lines": n_inst, "property_lines": n_prop}


def attribution_text(cfg: Config, sources: dict) -> str:
    src = cfg["sources"]
    lines = [
        f"# Attribution for {cfg['dataset']['name']} {cfg['dataset']['version']}",
        "",
        "This dataset is a derivative work released under CC BY-SA 4.0 (see LICENSE).",
        "",
        "## Sources",
        "",
        f"- DBpedia snapshot `{src['databus_snapshot']}` from the DBpedia Databus (https://databus.dbpedia.org), CC BY-SA.",
        "  Titles, abstracts, types and properties are extracted by DBpedia from the English Wikipedia,",
        "  written by Wikipedia contributors and released under CC BY-SA 4.0 (https://www.wikipedia.org).",
        f"- DBpedia Ontology release `{src['ontology_release']}` via DBpedia Archivo, CC BY-SA.",
        "- Wikimedia pageview_complete dumps (https://dumps.wikimedia.org/other/pageview_complete/), CC0.",
        f"  Months: {', '.join(cfg['pageviews']['months'])}. Only the derived notability score is distributed.",
        f"- Vectors: OpenAI `{cfg['embedding']['model']}`, {cfg['embedding']['dimensions']} dimensions, generated by the dataset authors.",
        "",
        "## Files used",
        "",
        "| key | url | sha256 |",
        "| --- | --- | --- |",
    ]
    for key, info in sorted(sources.items()):
        if isinstance(info, dict) and "url" in info:
            lines.append(f"| {key} | {info['url']} | `{info.get('sha256', '')}` |")
    lines += ["", "Built with https://github.com/Oxid-Ecosystem/oxid-dbpedia-ns (Apache 2.0)."]
    return "\n".join(lines) + "\n"


def dataset_card(cfg: Config, manifests: list[dict]) -> str:
    d = cfg["dataset"]
    rows = "\n".join(
        f"| {m['tier_name']} | {m['counts']['entities']:,} | {m['counts']['edges']:,} | {m['size_bytes'] / 1e9:.2f} GB |"
        for m in manifests
    )
    emb = manifests[-1]["embedding"] if manifests else {}
    return f"""---
license: cc-by-sa-4.0
language:
- en
pretty_name: OxidDB Neurosymbolic DBpedia Dataset
size_categories:
- 100K<n<1M
task_categories:
- feature-extraction
- text-retrieval
tags:
- dbpedia
- knowledge-graph
- owl
- embeddings
- neurosymbolic
- vector-search
configs:
{chr(10).join(f"- config_name: {m['tier_name']}{chr(10)}  data_files: {m['tier_name']}/entities.parquet" for m in manifests)}
---

# {d["name"]} {d["version"]}

Nested tiers of internationally known Western European and American DBpedia entities. Every
entity carries its DBpedia IRI, title, English short abstract, most-specific DBpedia Ontology
(DBO) type, whitelisted properties, resolved countries, notability signals and a
{cfg["embedding"]["dimensions"]}-dimensional `{emb.get("model", cfg["embedding"]["model"])}` vector, all keyed on the same IRI.
Each tier ships the symbolic side too: an OWL 2 EL TBox and an N-Triples ABox, so the data
loads into a reasoner as well as a vector store.

| Tier | Entities | Edges | Size |
| --- | --- | --- | --- |
{rows}

Tiers are strict prefixes of one notability-ranked list: everything in `t50` is in `t100`, and
everything in `t100` is in `t200`, with identical ranks and byte-identical vectors.

## Files per tier

| File | Contents |
| --- | --- |
| `entities.parquet` | `iri`, `rank`, `tier_min`, `title`, `abstract`, `embed_text`, `bucket`, `group`, `types`, `countries`, `props`, `score`, `views`, `languages`, `vector` |
| `edges.parquet` | `s`, `p`, `o` object-property edges between entities of the tier |
| `abox.nt` | `rdf:type`, `rdfs:label`, literals and edges as N-Triples |
| `tbox.owl`, `tbox.ttl`, `tbox.nt` | DBO restricted to OWL 2 EL (RDF/XML, Turtle, N-Triples) |
| `tbox_removed.owl` | axioms removed for EL compliance |
| `types_inferred.parquet` | subclass closure of the asserted types |
| `oxid_tbox.txt`, `oxid_abox.txt` | OxidDB line import format |
| `manifest.json` | pinned sources, checksums, counts, embedding run |
| `ATTRIBUTION.md` | credits and source files |

`embed_text` is exactly `title + "\\n" + abstract`, the input that produced `vector`, so anyone can
reproduce or extend the vectors with the same model.

## How it was built

Selection: DBpedia snapshot `{cfg["sources"]["databus_snapshot"]}`, English chapter. Entities need an
English abstract and a most-specific type inside one of {len(cfg.buckets)} class buckets (scientists,
artists, athletes, cities, companies, films, ...). A geography filter keeps entities that resolve to
one of {len(cfg["geography"]["countries"])} Western European countries or the United States. Ranking uses
`{cfg["ranking"]["weight_views"]} * z(log1p(pageviews)) + {cfg["ranking"]["weight_languages"]} * z(log1p(languages))`
within each bucket, with a floor of {cfg["ranking"]["floor_views"]:,} yearly pageviews and
{cfg["ranking"]["floor_languages"]} Wikipedia editions, and a quota-preserving interleave across buckets.
Pipeline source: https://github.com/Oxid-Ecosystem/oxid-dbpedia-ns

## License

CC BY-SA 4.0. Derived from DBpedia and Wikipedia (CC BY-SA); see `ATTRIBUTION.md` in every tier.
"""


def run(cfg: Config, force: bool = False) -> None:
    cfg.out.mkdir(parents=True, exist_ok=True)
    props_cfg = cfg["properties"]
    type_of = dict(props_cfg.get("types", {}))
    literal_names = sorted(
        {n for g, spec in props_cfg.items() if g != "types" for n in spec.get("literals", [])}
    )
    sources = read_json(cfg.cache / "sources.lock.json", {}) or {}
    embedding_info = read_json(cfg.work / "embedding_manifest.json", {}) or {}

    with timed("[6] TBox: OWL 2 EL filter"):
        onto_spec = next(f for f in cfg["sources"]["files"] if f["key"] == "ontology")
        onto_path = cfg.cache / "downloads" / Path(onto_spec["url"].split("/")[-1]).name
        g = load_ontology_graph(onto_path)
        tb = cfg.get("tbox", {})
        g, clean_stats = clean_tbox(
            g,
            drop_non_ascii_dbo=bool(tb.get("drop_non_ascii_dbo", True)),
            drop_external_equivalents=bool(tb.get("drop_external_equivalents", True)),
            label_languages=tuple(tb.get("label_languages", ["en"])),
        )
        kept, removed, el_stats = el_filter(g)
        el_stats = {**el_stats, "cleaned": clean_stats}
        hierarchy = hierarchy_from_graph(kept)
        log.info(
            "[6] TBox: %d triples kept, %d removed", el_stats["kept_triples"], el_stats["removed_triples"]
        )

    enriched = pl.read_parquet(cfg.work / "enriched.parquet")
    edges_top = pl.read_parquet(cfg.work / "edges_top.parquet")
    manifests: list[dict] = []
    for tier in cfg.tiers:
        d = tier_dir(cfg, tier)
        d.mkdir(parents=True, exist_ok=True)
        with timed(f"[6] tier {d.name}"):
            ents = enriched.filter(pl.col("rank") <= tier).sort("rank")
            members = ents.select("iri")
            edges = (
                edges_top.join(members, left_on="s", right_on="iri", how="semi")
                .join(members, left_on="o", right_on="iri", how="semi")
                .sort(["s", "p", "o"])
            )
            write_parquet_atomic(ents, d / "entities.parquet")
            write_parquet_atomic(edges, d / "edges.parquet")
            n_triples = write_abox(ents, edges, d / "abox.nt", literal_names, type_of)
            kept.serialize(destination=str(d / "tbox.owl"), format="xml")
            kept.serialize(destination=str(d / "tbox.ttl"), format="turtle")
            kept.serialize(destination=str(d / "tbox.nt"), format="nt", encoding="utf-8")
            removed.serialize(destination=str(d / "tbox_removed.owl"), format="xml")
            inferred = pl.DataFrame(
                [
                    (iri, t)
                    for iri, types in ents.select("iri", "types").iter_rows()
                    for a in types
                    for t in sorted(hierarchy.closure(a))
                ],
                schema={"iri": pl.String, "type": pl.String},
                orient="row",
            )
            write_parquet_atomic(inferred, d / "types_inferred.parquet")
            oxid_stats = write_oxid_files(ents, edges, kept, d)
            (d / "ATTRIBUTION.md").write_text(attribution_text(cfg, sources), encoding="utf-8")

            files = sorted(p.name for p in d.iterdir() if p.is_file() and p.name != "manifest.json")
            manifest = {
                "dataset": dict(cfg["dataset"]),
                "pipeline_version": __version__,
                "tier": tier,
                "tier_name": d.name,
                "tiers": cfg.tiers,
                "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "config_sha256": cfg.config_hash,
                "sources": sources,
                "embedding": embedding_info,
                "tbox": {"ontology_release": cfg["sources"]["ontology_release"], **el_stats},
                "counts": {
                    "entities": ents.height,
                    "edges": edges.height,
                    "abox_triples": n_triples,
                    "inferred_type_assertions": inferred.height,
                    "per_bucket": dict(sorted(ents.group_by("bucket").len().iter_rows())),
                    "per_group": dict(sorted(ents.group_by("group").len().iter_rows())),
                    "per_country": dict(
                        sorted(
                            ents.explode("countries", empty_as_null=False)
                            .drop_nulls("countries")
                            .group_by("countries")
                            .len()
                            .iter_rows(),
                            key=lambda r: -r[1],
                        )
                    ),
                    **oxid_stats,
                },
                "files": {
                    name: {"sha256": sha256_file(d / name), "bytes": (d / name).stat().st_size}
                    for name in files
                },
            }
            manifest["size_bytes"] = sum(v["bytes"] for v in manifest["files"].values())
            write_json(manifest, d / "manifest.json")
            manifests.append(manifest)
            log.info(
                "[6] %s: %d entities, %d edges, %.2f GB",
                d.name,
                ents.height,
                edges.height,
                manifest["size_bytes"] / 1e9,
            )

    (cfg.out / "README.md").write_text(dataset_card(cfg, manifests), encoding="utf-8")
    shutil.copyfile(cfg.root / "LICENSE-DATA", cfg.out / "LICENSE")
