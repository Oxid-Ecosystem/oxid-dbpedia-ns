# oxid-dbpedia-ns

Build pipeline for the **OxidDB Neurosymbolic DBpedia Dataset**: three nested tiers (50K, 100K,
180K) of internationally known Western European and American entities from DBpedia. Every entity
carries its DBpedia IRI, title, English abstract, most-specific DBpedia Ontology (DBO) type,
whitelisted properties, resolved countries, notability signals and a 1536-dimensional
`text-embedding-3-small` vector, all keyed on the same IRI. Each tier also ships the symbolic
side: an OWL 2 EL TBox and an N-Triples ABox, plus OxidDB's line import format.

The dataset is meant for demos and benchmarks of engines that combine vector search with
ontology reasoning. The tiers exist to compare load time, index build time, query latency,
memory and cost across three sizes of the same data.

- Design and every decision: [docs/briefing.md](docs/briefing.md)
- Planned v2 (no exclusions, tiers 1M/2.5M/full, benchmark workload): [docs/briefing-v2.md](docs/briefing-v2.md) — plan only, not executed; v1 `0.1.0` below is what is built and in use.
- Licenses: dataset CC BY-SA 4.0, pipeline code Apache 2.0 — see [Licenses](#licenses).
- Publishing the tiers to Hugging Face and Kaggle: [docs/publishing.md](docs/publishing.md).

## Quick start

```bash
uv sync --extra dev            # Python 3.12 virtualenv with polars, pyarrow, rdflib, openai
make test                      # unit tests + a full build on a synthetic fixture (seconds)
cp .env.example .env           # add OPENAI_API_KEY
make all                       # stages 1-7 on real data (hours; see "Cost and time")
make load                      # load out/t50 into a running OxidDB and run the smoke tests
```

Each stage is a separate command that reads the previous stage's Parquet output, so a config
change reruns only from the stage it touches:

| Stage | Command | Output |
| --- | --- | --- |
| 1 Acquire | `oxid-dbpedia-ns acquire` | `cache/parquet/*.parquet`, `cache/pageviews/<month>.parquet`, `cache/sources.lock.json` |
| 2 Candidates | `oxid-dbpedia-ns candidates` | `work/candidates.parquet` |
| 3 Geography | `oxid-dbpedia-ns geography` | `work/geo.parquet`, `work/geo_rejected.parquet`, `work/languages.parquet` |
| 4 Rank | `oxid-dbpedia-ns rank` | `work/ranked.parquet`, `work/preflight.json` |
| 5 Enrich + embed | `oxid-dbpedia-ns enrich` | `work/enriched.parquet`, `work/vectors/part-*.parquet` |
| 6 Emit | `oxid-dbpedia-ns emit` | `out/t50`, `out/t100`, `out/t180`, `out/README.md` |
| 7 Validate | `oxid-dbpedia-ns validate` | `out/validation_report.json` (non-zero exit on a hard failure) |

The numbered scripts (`01_acquire.py` ... `07_validate.py`) are thin wrappers around the same
commands. `oxid-dbpedia-ns all --from rank` reruns from a given stage.

## Layout

```
config.toml              every knob: sources, buckets and shares, countries, floor, properties, tiers
data/country_map.csv     historic states, demonyms and aliases mapped to modern countries
oxid_dbpedia_ns/         the pipeline package (stages/, ntriples parser, ontology tools, embeddings)
loader/oxiddb_load.py    standalone loader for OxidDB, with the neurosymbolic smoke test
loader/oxd_oracle.py     differential oracle against OxidDB's offline reasoner (oxd hierarchy / extents)
publish/preflight.py     release gate: validation passed, real vectors, bytes match the manifests
publish/to_*.py          upload out/ to Hugging Face Datasets and to Kaggle
tests/                   unit tests and a synthetic end-to-end fixture
docs/briefing.md         the design document and decision log
docs/publishing.md       the release runbook for GitHub, Hugging Face and Kaggle
docs/queries.html        the demo script: eight OxQL queries with the results a live t50 returned
deploy/                  ship a verified backup bundle to a private demo box (see provision.md)
cache/ work/ out/ dist/  downloads, per-stage Parquet, final tiers, bundles (all gitignored)
```

## What each tier contains

```
out/t180/
  entities.parquet        iri, rank, tier_min, title, abstract, embed_text, bucket, group, types,
                          countries, props (struct), score, views, languages, vector (float32[1536])
  edges.parquet           s, p, o object-property edges with both endpoints in the tier
  abox.nt                 rdf:type, rdfs:label, literals and edges as N-Triples
  tbox.owl / .ttl / .nt   DBO restricted to OWL 2 EL, cleaned of DBO artefacts (788 classes)
  tbox_removed.owl        the axioms stripped for EL compliance (30 functional-property axioms in DBO 2024.08)
  types_inferred.parquet  subclass closure of the asserted types: the oracle for a reasoner's classification
  oxid_tbox.txt           OxidDB import lines: SUBCLASS
  oxid_abox.txt           OxidDB import lines: INSTANCE, PROPERTY
  manifest.json           pinned sources with checksums, config hash, embedding run, counts, file hashes
  ATTRIBUTION.md
```

Tiers are strict prefixes of one ranked list, so `t50` is the first 50,000 rows of `t180` with
identical ranks and byte-identical vectors. `embed_text` is exactly `title + "\n" + abstract`,
the input that produced `vector`.

## Sources

All DBpedia files come from one Databus snapshot (`2022.12.01`, the latest release), the ontology
from DBpedia Archivo (`2024.08.01`), notability from twelve months of Wikimedia `pageview_complete`
dumps and the Wikidata sameAs links, and vectors from OpenAI. URLs and SHA-256 hashes are pinned
in `config.toml` and verified on download; observed hashes land in `cache/sources.lock.json` and in
every tier's manifest.

## Cost and time

| Step | Size | Time (M-series laptop) | Money |
| --- | --- | --- | --- |
| DBpedia downloads | 1.9 GB compressed | minutes | free |
| Pageview dumps | 12 x 5.6 GB, deleted after aggregation | hours (bzip2 bound; `brew install lbzip2` helps) | free |
| Parsing to Parquet | ~15 GB in `cache/` | tens of minutes | free |
| Embeddings, 180K entities | 19.1M tokens (measured: 95-105 tokens per entity) | minutes to hours (Batch API) | $0.19 measured via Batch at $0.01 per 1M tokens; $0.38 at the standard rate |

Disk: budget 40 GB for `cache/` if the pageview files are kept, 15 GB otherwise.

`make cost` is the embedding cost counter. Before Stage 5 it estimates the tokens for the top tier from
the ranked abstracts (4.06 characters per token, measured on real DBpedia abstracts); during the run it
sums the tokens OpenAI reported for every completed part (`work/vectors/tokens.json`); afterwards it
shows the final total from `work/embedding_manifest.json`, which also lands in every tier's manifest as
`embedding.cost_usd`. Prices live in `config.toml` under `[embedding]`.

## Loading into OxidDB

Tested against `moonlightarray/oxid-db:0.9.9` (API on 7878, web UI on 7880).

```bash
docker run -d --name oxid-db -p 127.0.0.1:7878:7878 -p 127.0.0.1:7880:7880 -v oxid-data:/app/data moonlightarray/oxid-db:0.9.9
python loader/oxiddb_load.py out/t50 --smoke --report load_t50.json
python loader/oxd_oracle.py out/t50 --docker oxid-db      # offline reasoner check, no database touched
```

The loader sets the embedder to manual, creates a cosine SQ8 collection, imports `tbox.ttl` through
`POST /import/owl?format=ttl` and checks that the reasoner reports the fragment as fully supported,
then upserts every entity with one `POST /entities/batch` call per 400 entities: its DBO classes,
`rdfs:label`, `dbo:abstract`, the typed literal props, the object-property edges from `edges.parquet`
and the vector, all in one transaction. It classifies once at the end and runs the smoke test: nearest
neighbours of a probe, the probe's server-side inferred classes against `types_inferred.parquet`, and a
class-scoped vector query (`FIND ?x WHERE ?x IS-A <http://dbpedia.org/ontology/PopulatedPlace> AND NEAR ?x.dbpedia TO [...]`)
whose results must all lie inside the oracle's extent. It prints timings per phase; the report JSON is
the per-tier measurement the tiers are for.

`oxd_oracle.py` is the differential oracle: OxidDB's own offline `oxd hierarchy` and `oxd extents`
commands run on the tier files and must produce exactly the subsumptions and class memberships the
pipeline wrote. On DBO they do, after `clean_tbox` removes three DBO artefacts (Urdu-localised duplicate
classes, `rdfs:subClassOf` axioms that point at properties, and superclasses in foreign namespaces).

OxidDB 0.9.9 facts the loader relies on:

- No RDF/XML anywhere (deliberately, a quick-xml advisory); Turtle and N-Triples are accepted by
  `/import/owl?format=ttl|nt`. The line format (`oxid_tbox.txt`, `oxid_abox.txt`) still works through
  `/import/csv` and `oxd load`, but it cannot carry typed literals.
- `text` sent with an entity is not stored, so the abstract goes in as the `dbo:abstract` data property.
  Literals have four types (string, integer, float, boolean); dates are ISO strings and compare lexically.
- Request body cap is `[server.limits] max_body_mb` (32 MB); IRIs in URL paths must be percent-encoded;
  the bracketed `<iri>` form is accepted after `IS-A` only, so the smoke test compares by vector, not by
  `LIKE ... TO <iri>`; OxQL number literals allow no exponent.
- Classification is not incremental and the reasoner internalises every entity that has an edge, so
  classify once after the load and expect the 180K tier with its edges to take real time.

For the 180K tier the HTTP path is the slow path (per-commit fsync). Set `[wal] sync_policy =
"group_commit"` and `[server.limits] rate_per_sec = 0` in `oxd.toml` for bulk loads, or use the
in-process Rust ingester in the Oxid-DB repo (`demo/gold/ingest`, about 1,500 vectors/s) and ship the
resulting data directory or an `oxd backup` bundle. `oxd export owl` gives the loaded ontology back.

## Running a demo

`docs/queries.html` is the demo script: five queries where the vector index and the reasoner each
do work the other cannot, three that are pure classification, and the four ways OxQL is easy to
write wrong. Every result on it came back from a live t50 instance. Open it in a browser and copy
the queries straight out of it.

To serve the tier from a private box rather than your laptop, `make bundle` writes a verified
`.oxdb-bundle` and [`deploy/provision.md`](deploy/provision.md) is the runbook: a 2 GB host, an SSH
tunnel, no public endpoint. t50 restores to ~400 MB on disk and ~320-400 MiB resident.

## Publishing

[docs/publishing.md](docs/publishing.md) is the runbook. In short:

```bash
make preflight                            # refuse to ship a build that is not what it claims
make publish-hf ARGS="--yes"              # out/ to Hugging Face Datasets, native Parquet
make publish-kaggle ARGS="--yes --public" # out/ to Kaggle, one zip per tier
```

`make preflight` is the gate: it hard-fails if stage 7 did not pass, if any manifest says
`"provider": "fake"` (the deterministic test embedder), if a file a manifest lists is missing, or if
a byte drifted from its recorded SHA-256. Both publish scripts run it again before uploading, and
neither uploads anything without `--yes`.

| Channel | Form |
| --- | --- |
| [GitHub](https://github.com/Oxid-Ecosystem/oxid-dbpedia-ns) | pipeline code; `cache/ work/ out/ dist/` never enter git |
| [Hugging Face Datasets](https://huggingface.co/datasets/Oxid-Ecosystem/oxid-dbpedia-ns) | `out/` as-is, one loadable config per tier |
| Kaggle Datasets | `t50.zip`, `t100.zip`, `t180.zip` |

## Licenses

Two licenses, because the code and the data have different origins:

- **Pipeline code** — Apache 2.0, [LICENSE](LICENSE). That covers everything in this repository:
  `oxid_dbpedia_ns/`, `loader/`, `publish/`, `deploy/`, `tests/`, the config and the docs.
- **The dataset** — CC BY-SA 4.0, [LICENSE-DATA](LICENSE-DATA). Derived from DBpedia and Wikipedia,
  which are CC BY-SA, so the tiers inherit share-alike. Every tier carries an `ATTRIBUTION.md` with
  the source files and their hashes.

Cite it with [CITATION.cff](CITATION.cff); GitHub renders a "Cite this repository" button from it.

## Tests

`make test` runs the unit tests (IRI normalisation, N-Triples parsing, ontology closure and bucket
assignment, the EL filter, the quota interleave) and then builds tiers of 50/100/200 entities from a
synthetic DBpedia-shaped fixture through all seven stages with the fake embedder, including the
full validation stage.
