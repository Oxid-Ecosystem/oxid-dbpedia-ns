## What changes

<!-- One or two sentences. Link the issue if there is one. -->

## Effect on the published tiers

<!-- Tick one. A dataset repo's first question is always whether the artefact moves. -->

- [ ] None — code, docs or tests only; a rebuild produces the same tiers
- [ ] The tiers change. `config.toml` and therefore `config_sha256` changed, or the emitted files did.
      Version bumped in `pyproject.toml`, `CITATION.cff` and `CHANGELOG.md`, and `docs/publishing.md`
      followed for the re-release.

## Checks

- [ ] `make lint` passes
- [ ] `make test` passes
- [ ] If a stage changed: a unit test covers the new behaviour
- [ ] If tier contents changed: `make emit && make validate` run on real data, and `make preflight` is clean
