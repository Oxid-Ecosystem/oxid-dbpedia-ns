# OxidDB Neurosymbolic DBpedia Dataset v2: Build Briefing

2026-09-19. Plan only — not executed. v1 (`0.1.0`) stays frozen and in use while this is built.

## Status

**v1 `0.1.0` is frozen.** Tiers 50K/100K/180K are built, validated (all hard checks pass), loaded
into OxidDB 0.9.9 and in use for the demo. Nothing in this plan touches `out/t50`, `out/t100`,
`out/t180`, the pinned sources for those tiers, or the code paths that produced them. The 50K tier
remains the working dataset until v2 is built and validated.

**v2 `0.2.0` is additive.** New profile, new tiers, new artifacts, new stages. v1 becomes a named
profile that is regenerable from the same pipeline, not a thing that gets overwritten.

## Why v2 exists

v1 was built as a demo corpus and it is good at that. It is not publishable, for four reasons that
v2 is designed to remove:

1. **The stated scope is false in the shipped data.** "Western European and American entities"
   does not describe a set containing Chongzuo, Seoul Olympic Stadium, Tao Yuanming, N.Flying,
   Luzhniki Stadium, Shah Rukh Khan, the Kaaba and the Ganges. The Stage 3 escape hatch
   (unresolved entities pass on >=4 non-English editions) is a global-fame filter, and it admits
   exactly what the scope claims to exclude. 122,874 of 562,613 survivors came through it.
2. **The curation rationale is disqualifying.** Geographic exclusion and the removal of politicians
   and royalty are justified in v1 as "so demo queries return results the target audience
   recognises". That is a marketing rationale for a benchmark, written down, by the vendor of the
   system under test.
3. **There is no benchmark.** No query workload, no gold answers, no metric, no baselines. v1 is a
   corpus. `types_inferred.parquet` is a reasoner oracle, not retrieval ground truth.
4. **Scale and the embedding model.** 180K is a toy to the ANN community, and vectors from a single
   proprietary, deprecatable model are a reproducibility liability.

## Measured constraints

Everything below was measured on the v1 cache (`cache/parquet/`, snapshot `2022.12.01`) on
2026-09-19. These numbers drive every decision in this plan; do not re-derive them by guess.

### Corpus funnel

| Stage | Entities | Cut by |
| --- | --- | --- |
| English short abstracts | 6,159,994 | — |
| Mapped DBO type (`instance-types` specific) | 7,564,288 | — |
| Both, raw join | 4,421,784 | the join |
| **Both, after IRI normalisation + redirect/disambiguation collapse** | **3,809,177** | canonicalisation |
| After bucket gate | 1,987,778 | only 307 of 1,576 classes map to a bucket |
| After geography gate | 562,613 | — |
| Above notability floor | 184,762 | v1 top tier |

**The ceiling is 3.81M.** 5M is not reachable from this snapshot while requiring an abstract and a
type. Do not put 5M in an abstract.

### Filter lattice

Inferred extent sizes over the 4.4M raw typed+abstract pool, closure computed with
`ontology.load_hierarchy`:

| Inferred extent | DBO classes |
| --- | --- |
| <10 | 15 |
| 10–100 | 47 |
| 100–1K | 144 |
| 1K–10K | 164 |
| 10K–100K | 85 |
| 100K–1M | 19 |
| >1M | 4 |

- **478** DBO classes have a non-empty extent. **311 of the 789 classes in the cleaned TBox have no
  instances at all** — a reportable property of DBpedia in its own right.
- **416** classes have >=100 members, the threshold for a k=100 ground truth.
- Minimum reachable selectivity: **2.33e-05**. Range spans ~5 decades.
- Classes per selectivity decade: 1e-1: 9, 1e-2: 32, 1e-3: 126, 1e-4: 171, 1e-5: 78.

### The decisive consequence

478 is a hard ceiling on distinct class predicates, independent of corpus size. At 3.81M we already
have 416 of them — **87% of the theoretical maximum**.

| | 2.5M | 3.81M | 10M (hypothetical) |
| --- | --- | --- | --- |
| Min selectivity at k=100 | 4.0e-5 | 2.6e-5 | 1.0e-5 |
| Usable filter classes | ~400 | 416 | ~440 |
| Selectivity decades | 4.4 | 4.6 | 5.0 |

Growing the corpus 4x buys 0.6 decades and ~25 classes. **Scale is nearly worthless on the axis
that carries the contribution.** v2 therefore spends its effort widening the filter lattice, not
lengthening the corpus.

## Design decisions

- [x] **Tier chain: 1M / 2.5M / full (~3.81M).** Three scaling points beat two for latency-vs-N
      plots. 2.5M is an intermediate tier, never the ceiling: the full set is defined by a
      construction rule ("every DBpedia entity with an English abstract and a mapped DBO type"),
      and a rule is defensible where a chosen number is not. Marginal embedding cost of full over
      2.5M is ~$1.40.
- [x] **No exclusions.** No geography gate, no class gate, no politician/royalty removal, no
      notability floor. Genuine noise filters stay (`List of`, disambiguation, redirect).
- [x] **Profiles, not a rewrite.** v1 becomes profile `demo-west`; v2 is profile `full-en`.
- [x] **Geography becomes an annotation, not a filter.** `demo-west` is then a filter expression
      over `full-en`, so the two can never drift.
- [x] **Conjunctive constraints are the core contribution**, not a nice-to-have. Class alone bottoms
      out at 1e-5; `class AND country AND date-range` composes to 1e-6 and below at zero corpus
      cost. This is the only lever that meaningfully extends the selectivity axis.
- [x] **Second, open embedding model** ships alongside OpenAI.
- [x] **5M is dropped as a target.** Wikidata `instance-types` is still wanted, but for lattice
      width (it adds classes), not for entity count.

## Architecture: profiles

```toml
[profiles.demo-west]          # frozen; reproduces v1 exactly
  filters = ["geography", "buckets", "floor"]
  rank_mode = "quota"
  tiers = [50000, 100000, 180000]

[profiles.full-en]            # v2
  filters = []
  rank_mode = "flat"
  tiers = [1000000, 2500000, 0]   # 0 = whole pool
```

```
out/
  demo-west/t50 t100 t180     # v1 artifacts, moved once, then never touched
  full-en/t1m t2500k tfull
```

`Config` grows a `profile` field; `cli.py` grows `--profile`. Stage signatures
(`run(cfg, force=...)`) do not change shape — the profile is read off `cfg`.

## Stage-by-stage

### Stage 1 — Acquire (`stages/acquire.py`)

- Add `wikidata_instance_types` source from the same `2022.12.01` snapshot. **Verify the artifact
  exists on the Databus before planning around it.** Its value is added classes, not added
  entities. If it is thin, drop it; the plan does not depend on it.
- Expand `[pageviews].languages` from 11 to ~30 editions by article count (add ru, zh, ja, ar, pl,
  uk, tr, ko, fa, he, cs, hu, ro, el, vi, id, hi, th, ...). **This is not optional.** Removing the
  geography gate while keeping an English-plus-10-Western-European pageview signal just moves the
  bias from the filter into the ranking: the 1M prefix stays Western-heavy and a reviewer finds it
  in one plot. Same I/O (all 12 files are streamed through `bzip2 | grep` either way); the
  aggregate grows ~3x.
- Every v1 pin stays byte-identical so `demo-west` remains reproducible.

### Stage 2 — Candidates (`stages/candidates.py`)

- Bucket assignment becomes non-exclusive **labelling**. No entity is dropped for lacking a bucket;
  unmapped entities get their top-level DBO class as the label.
- Restore Politician and Royalty as real buckets.
- Keep `List of`, disambiguation and redirect filters — noise, not exclusion.
- New column `type_provenance` (mappings / wikidata / none).

### Stage 3 — Geography (`stages/geography.py`)

- `Resolver` is unchanged. `run()` resolves `countries` for all 3.81M and **drops nothing**.
- `geo_rejected.parquet` becomes `geo_unresolved.parquet`, diagnostics only.
- Risk: this is a Python-loop resolution over 3.81M instead of 1.99M. If it exceeds ~1h, rewrite
  `place_to_countries` as an iterative join-based closure in polars.

### Stage 4 — Rank (`stages/rank.py`)

- Add `rank_mode`. `flat` = pure notability descending, no `interleave()`, no floor. `quota` keeps
  the existing path untouched for `demo-west`.
- `floor_views` / `floor_languages` go to 0 for `full-en`.
- **Hold out a 10K query set here**, excluded from every base tier, stratified over buckets and
  extent size. Doing it at rank time makes it deterministic and inherited by all tiers.
- Bucket stays a stratification label in the output, never a gate.

### Stage 5 — Enrich + embed (`stages/enrich_embed.py`, `embeddings.py`)

- OpenAI cost is a non-issue: 3.81M x ~104 tok ~= 400M tokens ~= **$4** via Batch.
- Real constraint is **~76 batch files** at 50K requests each plus org-level enqueued-token limits.
  Part-checkpointing already exists; add a queue manager with retry/backoff. Budget multi-day
  wall-clock.
- **Add a second, open embedding model**: `intfloat/multilingual-e5-large` (1024-d) or
  `BAAI/bge-m3`. Multilingual matters now that the corpus is global. This removes the
  proprietary-model reproducibility objection and adds a second experimental axis: does hybrid
  filter behaviour depend on the embedding model? Rented GPU, a few hours, ~$20.

### Stage 6 — Emit (`stages/emit.py`)

- Split vectors from metadata so the metadata Parquet stays small and browsable on Hugging Face.
- `vectors.hdf5` in ann-benchmarks layout (`train`, `test`, `neighbors`, `distances`) and `.fvecs`
  for classic tooling. Parquet alone will not get adoption from the ANN community.
- Precision variants: float32 canonical, float16, int8 with stored scales.
- TBox variants: EL for all tiers; RL/QL/DL only for the small tiers, where a DL reasoner
  terminates.
- **Existential restrictions** derived from the data (`∃birthPlace.{Germany}`,
  `∃author.Scientist`). These answer the "this is a taxonomy, not an ontology" objection and feed
  the conjunctive workload at the same time.

### Stage 7 — Validate (`stages/validate.py`)

- Profile-aware checks; v1's hard checks carry over unchanged.
- Ground-truth self-consistency (every gold neighbour is inside the stated extent).
- **ELK cross-check of the materialised closure.** Today the only independent check is OxidDB's own
  reasoner, which makes the oracle circular for a paper. An external reasoner fixes that.
- Delete the soft checks that never fire. The v1 leakage and "household names" checks printed lists
  with no criterion and flagged `false` while the data was visibly wrong.

## New stages

### Stage 8 — Workload generation

For each held-out query vector, paired with predicates sampled to stratify selectivity across
decades:

1. **Class-constrained**: inferred extent from the materialised closure, exact top-100 restricted
   to it. Record `(query_id, predicate, selectivity, neighbors, distances)`.
2. **Conjunctive**: `class AND country`, `class AND date-range`, `class AND country AND
   date-range`. This is what carries the selectivity axis below 1e-5 — the core contribution.
3. **Unconstrained** kNN: the standard ANN track.
4. **Reasoning-only**: classification and instance retrieval, ground truth from ELK.

Compute: 1e4 queries x 3.81M x 1536-d ~= 1.2e14 FLOPs — **under an hour on one A100**, a long
weekend on CPU BLAS. Filtered variants are strictly cheaper. Not the bottleneck it looks like.

### Stage 9 — Baseline harness

OxidDB, Qdrant with the closure materialised as payload filters (the honest strong baseline),
Weaviate, Milvus, pgvector + recursive CTE over `rdfs:subClassOf`, and ELK+FAISS as the reference
two-phase implementation.

Report recall@100, p50/p95 latency, QPS, memory and build time **as a function of selectivity**.
If OxidDB wins at every selectivity in our own paper, nobody will believe it. The publishable
result is where the pre-filter/post-filter crossover sits and why.

## Budget

| Item | Cost |
| --- | --- |
| OpenAI embeddings, 3.81M | ~$4 (Batch) |
| Open-model embeddings (rented GPU) | ~$20 |
| Ground-truth kNN (GPU) | ~$10 |
| Working disk | ~150 GB |
| Published artifact | ~60 GB (fp32 23 + fp16 12 + int8 6 + second model 16) |
| **Money total** | **< $100** |

Wall-clock, not money, is the cost. Roughly 3–5 weeks: ~1 week re-acquire and pipeline changes,
~1 week embedding wall-clock, ~1 week workload and ground truth, ~1–2 weeks baselines.

## Publication

Hugging Face is the primary host — 60 GB of vectors is uncomfortable for a single Zenodo record.
Mint the Zenodo DOI on the **metadata + workload + ground truth**, which is the small citable core,
and point it at Hugging Face for the vectors.

| Paper | Venue | Contribution |
| --- | --- | --- |
| Resource / benchmark | ISWC or ESWC Resources track, NeurIPS D&B | Dataset + workload + ground truth + baselines. Sells on real-world data at scale with aligned embeddings, which LUBM and UOBM lack. |
| Empirical systems | VLDB, SIGMOD, EDBT | "Filtering an ANN index by an inferred class is not attribute filtering." The selectivity crossover is the result. |
| Sharding | — | The three existing `Oxid-Ecosystem/*.pdf` drafts. |

**Positioning for the ANN community:** do not compete with BigANN on N; the lattice measurement
says winning would not help. YFCC-10M's filters are flat materialised tags. Ours are inferred
extents over a subsumption lattice, where the predicate is not stored and must be derived. That is
a different problem and it is unbenchmarked. A well-argued 3.81M with a structural property nobody
else has beats a me-too 10M.

## Open decisions

- [ ] Verify `dbpedia/wikidata/instance-types` exists at `2022.12.01`. Wanted for lattice width;
      drop it if thin.
- [ ] **Snapshot age.** The KG is `2022.12.01` but pageviews are 2025-09 to 2026-08 — a 3.5-year
      gap, and it is *worse* for a global dataset because non-Western editions grew most in that
      window. Either check for a newer DBpedia release (one day of work) or document it as a
      threat to validity. Do not leave it unstated.
- [ ] `demo-west` migration: move `out/t50|t100|t180` into `out/demo-west/` and regenerate
      manifests, or leave v1 paths alone and only namespace `full-en`? The loader and any demo
      wiring point at the current paths.
- [ ] Choose the second embedding model (`multilingual-e5-large` vs `bge-m3`).
- [ ] Confirm CC BY-SA 4.0 for the data with someone qualified — carried over from v1, still open.

## Decision log

Resolved 2026-09-19:

- [x] Top tier is the full rule-defined pool (~3.81M), not a chosen number. 2.5M is an intermediate
      tier only.
- [x] 5M dropped as a target; the snapshot cannot reach it with an abstract-and-type requirement.
- [x] Scale is not the lever. 478-class ceiling, 416 already reached; effort goes to conjunctive
      constraints and existential restrictions instead.
- [x] Geography and buckets become annotations; `demo-west` is derived by filtering `full-en`.
- [x] Pageview language list expands with the scope, or the bias just moves into the ranking.
- [x] v1 `0.1.0` stays frozen and in use. The 50K tier is the working dataset until v2 validates.
