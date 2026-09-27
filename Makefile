# oxid-dbpedia-ns build targets. Requires uv (https://docs.astral.sh/uv/).
PY := .venv/bin/python
CLI := .venv/bin/oxid-dbpedia-ns

.PHONY: env test lint acquire candidates geography rank enrich emit validate all cost load bundle \
        preflight publish-hf publish-kaggle clean-work clean-out

env:            ## create .venv and install the package with dev extras
	uv sync --extra dev

test: env       ## run the unit tests and the synthetic end-to-end build
	$(PY) -m pytest -q

lint: env
	.venv/bin/ruff check oxid_dbpedia_ns tests loader publish
	.venv/bin/ruff format --check oxid_dbpedia_ns tests loader publish

acquire candidates geography rank enrich emit validate: env
	$(CLI) $@

all: env        ## run every stage in order
	$(CLI) all

cost: env       ## embedding token and USD counter: estimate before, running total during, final after Stage 5
	$(CLI) cost

load: env       ## load the smallest tier into a running OxidDB (OXIDDB_URL, default http://localhost:7878)
	$(PY) loader/oxiddb_load.py out/t50 --smoke

bundle:         ## verified backup bundle of the loaded t50 container, ready to ship (see deploy/provision.md)
	./deploy/make-bundle.sh

preflight:      ## verify out/ is publishable: validation passed, real vectors, bytes match manifests
	$(PY) publish/preflight.py out

publish-hf:     ## upload out/ to Hugging Face Datasets (plan only; add ARGS=--yes to upload)
	uv sync --extra dev --extra publish
	$(PY) publish/to_huggingface.py out $(ARGS)

publish-kaggle: ## upload out/ to Kaggle Datasets (plan only; add ARGS="--yes --public" to upload)
	uv sync --extra dev --extra publish
	$(PY) publish/to_kaggle.py out $(ARGS)

clean-work:     ## drop per-stage outputs (keeps downloads and vectors)
	rm -rf work/candidates* work/geo* work/languages* work/ranked* work/preflight.json work/rank_report.json work/enriched* work/edges_top* work/embedding_manifest.json

clean-out:
	rm -rf out
