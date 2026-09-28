# Contributing

This repository is the build pipeline for a published dataset, so the bar for a change is: after it
lands, `make test` still passes and a full rebuild still produces a tier set whose manifest explains
itself. Read [docs/briefing.md](docs/briefing.md) first — it is the decision log, and most "why is it
like this" questions are answered there.

## Setup

```bash
uv sync --extra dev          # Python 3.12 venv with polars, pyarrow, rdflib, openai
make test                    # unit tests + a full synthetic build through all seven stages, seconds
```

No API key and no network access are needed for the tests: the fixture is a synthetic
DBpedia-shaped tree and the embedder is the deterministic fake one.

## Before you open a pull request

```bash
make lint      # ruff check + ruff format --check over every package
make test      # unit tests and the synthetic end-to-end build
```

CI runs exactly those, plus two metadata checks: `CITATION.cff` must validate against CFF 1.2.0,
and the version must be identical in `pyproject.toml`, `CITATION.cff` and `CHANGELOG.md`. `make`
with no target lists every target.

## What a change should come with

- **A stage change** (`oxid_dbpedia_ns/stages/*.py`): a unit test for the new behaviour, and a note
  in `docs/briefing.md` if it changes what ends up in a tier or why.
- **A selection or ranking change** (`config.toml`, buckets, floors, the geography filter): say what
  it does to the tier composition. `work/preflight.json` and `work/rank_report.json` are the
  before/after evidence. Remember that `config.toml` is hashed into every manifest as
  `config_sha256`, so any edit means the published tiers no longer match a rebuild — that is a
  release decision, not a silent one.
- **A change to emitted files** (`stages/emit.py`, the OxidDB line format, the TBox cleaning): the
  validation stage must still pass, and `loader/oxd_oracle.py` must still reproduce the pipeline's
  subsumptions exactly. `make test` covers the first; the second needs a real tier.
- **A new column in `entities.parquet`**: add a line to `COLUMN_DOCS` in `stages/emit.py`. The
  dataset card renders the dictionary against the frame's real schema, so an undocumented column is
  published with the word **undocumented** next to it rather than quietly omitted.
- **A number you want to quote in the docs**: if it describes the artefact, write it into the
  manifest and cite that, rather than typing it into prose where it will rot. The TBox counts
  (`classes_declared`, `hierarchy_nodes`, `subsumptions`) work this way and stage 7 recounts them
  from the shipped `tbox.owl`.
- **A loader change** (`loader/`): the OxidDB version you tested against, and the `--report` JSON.

## Things that are deliberate

- No RDF/XML is sent to OxidDB. Turtle and N-Triples only; see the OxidDB notes in the README.
- The TBox is restricted to OWL 2 EL, and the axioms dropped to get there are shipped in
  `tbox_removed.owl` rather than discarded silently.
- Tiers are strict prefixes of one ranked list. Anything that breaks
  `t50 ⊂ t100 ⊂ t180` with identical ranks and byte-identical vectors is a breaking change.
- `cache/`, `work/`, `out/` and `dist/` never enter git.

## Reporting a problem in the data

Open an issue with the entity IRI, the tier, and what the wrong value is. If it came from DBpedia
itself — a wrong type, a bad abstract — say so; the pipeline's job is to pass DBpedia through
faithfully, not to correct it, and upstream errors get documented rather than patched.
