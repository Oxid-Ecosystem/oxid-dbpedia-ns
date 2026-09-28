# Security

## Scope

This repository builds a public dataset from public sources. It has no server, no user accounts and
no runtime that accepts untrusted input, so the realistic risks are narrow:

- **Supply chain.** Dependencies are pinned in `uv.lock`; the DBpedia and ontology downloads are
  pinned by URL *and* SHA-256 in `config.toml` and verified on download, with the observed hashes
  recorded in `cache/sources.lock.json` and in every tier manifest.
- **Parsing untrusted RDF.** The pipeline parses multi-gigabyte third-party N-Triples and Turtle.
  A malicious or malformed file is a denial-of-service or memory-exhaustion concern rather than a
  code-execution one, but report anything worse.
- **Credentials.** `OPENAI_API_KEY` and the publishing tokens are read from the environment or a
  gitignored `.env`. Nothing writes them to `work/`, `out/` or a manifest. If you find a path that
  leaks one into an artefact, that is a real vulnerability — please report it.

## Reporting

Email **info@moonlightarray.nl** with the details and a way to reproduce. Please do not open a
public issue for a vulnerability.

Expect an acknowledgement within a few working days. This is a small project with no formal SLA and
no bounty programme, but credible reports are taken seriously and credited if you want the credit.

## Data, not code

If your concern is a factual error in the published data, or personal information about a living
person that appears in the tiers, that is not a security issue but it is still important: open a
"Data problem" issue, or email the address above if you would rather not do so publicly. The tiers
derive from DBpedia and Wikipedia and carry the biographical facts those sources publish.
