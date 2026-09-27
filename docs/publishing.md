# Publishing runbook

Three places, one build. The tier set in `out/` is the artifact; GitHub carries the code that makes
it, Hugging Face carries the dataset in its native form, Kaggle carries a zipped copy for reach.
Nothing here regenerates data — publish the `out/` you already validated.

| Channel | What lands there | Why |
| --- | --- | --- |
| GitHub — `Oxid-Ecosystem/oxid-dbpedia-ns` | pipeline code, config, docs, loader, deploy runbook | the reproducible source; `cache/ work/ out/ dist/` never enter git |
| Hugging Face Datasets | `out/` as-is: Parquet, ABox, TBox, manifests, dataset card | native Parquet with one loadable config per tier; the only copy that works with `load_dataset` |
| Kaggle Datasets | `t50.zip`, `t100.zip`, `t180.zip` plus the card | discovery; Kaggle has no configs and archives subdirectories |

## 0. Gate

```bash
make preflight
```

Hard-fails on anything that makes a release a lie: stage 7 did not pass, a manifest says
`"provider": "fake"`, a file listed in a manifest is missing, or a byte drifted from its recorded
SHA-256. Both publish scripts run this again before they upload, so you cannot skip it by accident.

## 1. GitHub

```bash
git push -u origin main
git tag -a v0.1.0 -m "oxid-dbpedia-ns 0.1.0"
git push origin v0.1.0
```

CI runs `ruff check`, `ruff format --check` and `pytest -q` on every push. The tag is what
`CHANGELOG.md` and `CITATION.cff` name, so bump all three together or none.

## 2. Hugging Face

One-time:

```bash
uv sync --extra dev --extra publish
hf auth login                       # a write token: huggingface.co → Settings → Access Tokens
```

Then:

```bash
make publish-hf                                   # prints the plan, uploads nothing
make publish-hf ARGS="--yes"                      # 1.6 GB, public
make publish-hf ARGS="--yes --private"            # if you want to inspect it before it is visible
```

The script uploads `.gitattributes` first so `*.parquet`, `*.nt`, `*.owl`, `*.ttl` and `*.txt` are
tracked as LFS — the Hub's default patterns do not cover `.nt` or `.txt`, and the ABox files are far
past the 10 MB inline limit. `out/README.md` becomes the dataset card: stage 6 writes its frontmatter
with the license and one config per tier, so it needs no editing.

Re-running after a failure is safe: the Hub deduplicates by content hash, so an interrupted upload
resumes instead of re-sending 1.6 GB.

Check afterwards that the dataset viewer renders all three configs. If it does not, the frontmatter
`configs:` block in `out/README.md` is the thing to look at.

## 3. Kaggle

One-time: Kaggle → Settings → API → Create New Token, save `kaggle.json` to `~/.kaggle/`,
`chmod 600 ~/.kaggle/kaggle.json`.

```bash
make publish-kaggle                                  # prints the plan
make publish-kaggle ARGS="--yes"                     # creates it private
make publish-kaggle ARGS="--yes --public"            # creates it public
```

Kaggle-specific notes:

- The script writes `out/dataset-metadata.json` from `out/README.md` with the YAML frontmatter
  stripped, because Kaggle reads the description from that file.
- Keywords are left empty on purpose: Kaggle rejects tags outside its own vocabulary. Add them in the
  web UI after the first upload.
- A second `--yes` run creates a new *version* of the same dataset rather than a duplicate.
- Title and subtitle are length-checked in the plan output (Kaggle allows 6-50 and 20-80 characters).

## 4. After a release

- Point the Hugging Face card and the Kaggle description at the GitHub tag, not `main`, if you want
  the published data and the published code to stay in step.
- `docs/queries.html` is the demo; it needs a live t50, see `deploy/provision.md`.

## Releasing a rebuild

A rebuild that changes `config.toml` changes `config_sha256` in every manifest, which is the whole
point of that field: the published tiers no longer match a rebuild from the previous config. So a
config change means a version bump in `pyproject.toml`, `CITATION.cff` and `CHANGELOG.md`, a fresh
`make emit && make validate`, and a new upload to both channels. Do not quietly overwrite a
published tier set with different bytes under the same version.

### What a rebuild reproduces byte for byte, and what it does not

Stage 6 is deterministic for the data, not for the ontology serialisations. Re-running
`make emit` on the same `work/` reproduces these exactly:

`entities.parquet`, `edges.parquet`, `abox.nt`, `types_inferred.parquet`, `oxid_abox.txt`,
`oxid_tbox.txt`

These four come out semantically identical but byte-different on every run:

`tbox.nt`, `tbox.owl`, `tbox.ttl`, `tbox_removed.owl`

The cause is rdflib: its N-Triples and RDF/XML writers emit triples in set-iteration order, and its
Turtle writer numbers auto-bound prefixes (`ns1:`, `ns2:`) in the order namespaces are first seen.
The triple set, the class count and the `tbox` block of the manifest are stable — only the byte
layout moves. So someone verifying a published tier by rebuilding it will match six file hashes and
mismatch four, and that is expected rather than a corrupted download. `make preflight` compares the
bytes you are about to ship against the manifest written alongside them, so it is unaffected.

Making those four reproducible means sorting the serialised triples before writing them, which is a
change to `stages/emit.py` and a fresh version of the dataset. It has not been done.
