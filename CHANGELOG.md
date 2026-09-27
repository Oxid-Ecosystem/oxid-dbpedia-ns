# Changelog

All notable changes to this project are documented here. Versions follow the dataset, not the code:
a version bump means the published tiers changed.

## 0.1.0 — 2026-09-27

First public release. Pipeline and dataset built and verified on 2026-09-19.

### Dataset

- Three nested tiers: `t50` (50,000 entities), `t100` (100,000), `t180` (180,000). Strict prefixes
  of one notability-ranked list, with identical ranks and byte-identical vectors across tiers.
- 168,646 object-property edges in `t180` with both endpoints inside the tier.
- 1536-dimensional `text-embedding-3-small` vectors over `title + "\n" + abstract`, generated
  through the OpenAI Batch API: 19.1M tokens, USD 0.19 measured.
- OWL 2 EL TBox per tier (788 DBO classes) in RDF/XML, Turtle and N-Triples, with the 30 functional
  property axioms removed for EL compliance shipped separately in `tbox_removed.owl`.
- `types_inferred.parquet`: the subclass closure of the asserted types, as a reasoner oracle.
- OxidDB line import format (`oxid_tbox.txt`, `oxid_abox.txt`) alongside the standard serialisations.
- Every tier carries a `manifest.json` pinning source URLs and SHA-256 hashes, the config hash, the
  embedding run and per-file hashes, plus an `ATTRIBUTION.md`.

### Sources

- DBpedia snapshot `2022.12.01`, English chapter.
- DBpedia Ontology via Archivo, `2024.08.01`.
- Twelve months of Wikimedia `pageview_complete` dumps, and Wikidata sameAs links for the language
  count.

### Verified

- Stage 7 validation passes every hard check (`out/validation_report.json`: `ok: true`).
- `t50`, `t100` and `t180` all load into OxidDB 0.9.9 and pass the neurosymbolic smoke test:
  nearest neighbours, server-side inferred classes against `types_inferred.parquet`, and a
  class-scoped vector query whose results all lie inside the offline reasoner's extent.
- `loader/oxd_oracle.py` reproduces the pipeline's subsumptions and class memberships exactly using
  OxidDB's own offline `oxd hierarchy` and `oxd extents`.

### Known limitations

- Coverage is deliberately narrow: 21 class buckets, 20 Western European countries plus the United
  States, a floor of 5,000 yearly pageviews and 5 Wikipedia editions. This is a benchmark and demo
  corpus, not a representative sample of DBpedia. [docs/briefing-v2.md](docs/briefing-v2.md) is the
  plan for a version without those exclusions; it is not built.
- The DBpedia snapshot is `2022.12.01`, the latest Databus release, so facts are as of late 2022.
- Abstracts are English only.
