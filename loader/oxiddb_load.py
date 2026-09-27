#!/usr/bin/env python3
"""Load one tier of oxid-dbpedia-ns into a running OxidDB instance over HTTP (tested on 0.9.9).

Standalone: needs `requests` and `polars` plus a tier directory (pipeline output or the published dataset).

    python loader/oxiddb_load.py out/t50 --url http://localhost:7878 --smoke --report load_t50.json

Phases, each timed and reported (the tiers exist to compare these numbers):
  1. PUT  /config/embedder {"provider":"manual"}   vectors are supplied, never computed by the server
  2. POST /collections                             cosine, 1536 dims, sq8 (+ cold_f32 when the server allows it)
  3. POST /import/owl?format=ttl  tbox.ttl         the OWL 2 EL TBox; GET /reasoner/fragment must say fully_supported
  4. POST /entities/batch         entities.parquet + edges.parquet
       one compound upsert per entity: classes, rdfs:label, dbo:abstract, typed literal props,
       object-property edges and the vector, all in one transaction
  5. POST /classify               once, at the end (classification is not incremental)
  6. --smoke: nearest neighbours of a probe, then a class-scoped vector query whose results are checked
     against types_inferred.parquet, and the probe's inferred_classes against the same file.

Why not /insert/batch + /import/csv: `text` is not stored by the server, the line format cannot carry
typed literals, and /entities/batch writes everything in one call. The line files (oxid_tbox.txt,
oxid_abox.txt) remain for `oxd load` / `oxd extents` and for --tbox-lines.

Throughput: the HTTP path fsyncs per commit. Set `[wal] sync_policy = "group_commit"` and
`[server.limits] rate_per_sec = 0` in oxd.toml for bulk loads. For the 200K tier the in-process
Rust ingester in the Oxid-DB repo (demo/gold/ingest) is the fast path; see the README.

Environment: OXIDDB_URL, OXIDDB_API_KEY (sent as Authorization: Bearer).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

import polars as pl
import requests

DBO = "http://dbpedia.org/ontology/"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
GEO = {
    "geo:lat": "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
    "geo:long": "http://www.w3.org/2003/01/geo/wgs84_pos#long",
}
RETRYABLE = {429, 503, 507}


class Oxid:
    def __init__(self, url: str, token: str | None, timeout: float = 300.0):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        if token:
            self.s.headers["Authorization"] = f"Bearer {token}"
        self.timeout = timeout
        self.requests = 0

    def call(
        self,
        method: str,
        path: str,
        *,
        json_body=None,
        data=None,
        headers=None,
        ok=(200, 201, 204),
        retries=10,
    ):
        for attempt in range(retries):
            self.requests += 1
            r = self.s.request(
                method, self.url + path, json=json_body, data=data, headers=headers, timeout=self.timeout
            )
            if r.status_code in RETRYABLE or (r.status_code >= 500 and attempt < retries - 1):
                wait = float(r.headers.get("Retry-After", min(30, 2**attempt)))
                time.sleep(max(0.2, wait))
                continue
            if r.status_code not in ok:
                raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
            return r
        raise RuntimeError(f"{method} {path}: gave up after {retries} attempts")

    def health(self) -> dict:
        return self.call("GET", "/health").json()


class Timer:
    def __init__(self) -> None:
        self.timings: dict[str, float] = {}

    def __call__(self, name: str):
        timings = self.timings

        class _T:
            def __enter__(self):
                self.t = time.time()
                print(f"[{name}] ...", flush=True)

            def __exit__(self, *a):
                timings[name] = round(time.time() - self.t, 2)
                print(f"[{name}] done in {timings[name]}s", flush=True)

        return _T()


def enc(iri: str) -> str:
    return quote(iri, safe="")


def import_lines(db: Oxid, path: Path, chunk_lines: int) -> int:
    n, buf = 0, []

    def flush():
        nonlocal n
        if buf:
            db.call(
                "POST",
                "/import/csv",
                data="\n".join(buf).encode("utf-8") + b"\n",
                headers={"Content-Type": "text/plain; charset=utf-8"},
            )
            n += len(buf)
            buf.clear()

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if line and not line.startswith("#"):
                buf.append(line)
                if len(buf) >= chunk_lines:
                    flush()
    flush()
    return n


def literal_props(props: dict, type_hint: dict[str, type]) -> list[dict]:
    out = []
    for name, value in props.items():
        if value is None or name.endswith("_label"):
            continue
        if isinstance(value, float) and value != value:  # NaN
            continue
        prop = GEO.get(name, DBO + name)
        out.append({"property": prop, "kind": "data", "value": value})
    return out


def entity_payloads(
    ents: pl.DataFrame, edges_by_s: dict[str, list[tuple[str, str]]], collection: str, with_abstract: bool
):
    vectors = ents.get_column("vector").to_numpy()
    rows = ents.select("iri", "title", "abstract", "types", "props").iter_rows(named=True)
    for i, row in enumerate(rows):
        props = [{"property": RDFS_LABEL, "kind": "data", "value": row["title"]}]
        if with_abstract:
            props.append({"property": DBO + "abstract", "kind": "data", "value": row["abstract"]})
        props.extend(literal_props(row["props"] or {}, {}))
        props.extend({"property": p, "kind": "object", "value": o} for p, o in edges_by_s.get(row["iri"], ()))
        yield {
            "iri": row["iri"],
            "classes": list(row["types"]),
            "properties": props,
            "vector": {"values": [round(float(x), 6) for x in vectors[i]], "collection": collection},
        }


def load_entities(
    db: Oxid, ents: pl.DataFrame, edges: pl.DataFrame, collection: str, batch: int, with_abstract: bool
) -> tuple[int, list[str]]:
    edges_by_s: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for s, p, o in edges.iter_rows():
        edges_by_s[s].append((p, o))
    inserted, errors, buf = 0, [], []
    total = ents.height

    def flush():
        nonlocal inserted
        r = db.call(
            "POST", "/entities/batch", json_body={"atomic": False, "gate": False, "entities": buf}
        ).json()
        inserted += int(r.get("inserted", 0))
        errors.extend(r.get("errors", []))
        buf.clear()
        if inserted % (batch * 10) < batch:
            print(f"  upserted {inserted}/{total}", flush=True)

    for payload in entity_payloads(ents, edges_by_s, collection, with_abstract):
        buf.append(payload)
        if len(buf) >= batch:
            flush()
    if buf:
        flush()
    return inserted, errors


def smoke(db: Oxid, tier: Path, ents: pl.DataFrame, collection: str, k: int) -> dict:
    """Semantic + neurosymbolic smoke test. Returns results plus an `ok` flag."""
    inferred = pl.read_parquet(tier / "types_inferred.parquet")
    closure: dict[str, set[str]] = defaultdict(set)
    for iri, t in inferred.iter_rows():
        closure[iri].add(t)
    titles = dict(zip(ents.get_column("iri").to_list(), ents.get_column("title").to_list()))
    generic = {DBO + c for c in ("Thing", "Agent", "Person", "Place", "Work", "Event", "Organisation")}

    # Probe: the highest ranked entity whose inferred types include a non-generic superclass that is
    # not asserted, so the class-scoped query genuinely needs the reasoner.
    probe = scope = None
    for i, (iri, types) in enumerate(ents.select("iri", "types").iter_rows()):
        supers = closure[iri] - set(types) - generic
        if supers:
            probe, scope, idx = iri, min(supers), i
            break
    if probe is None:
        return {"ok": False, "reason": "no entity with a non-trivial inferred superclass"}
    out: dict = {"probe": probe, "probe_title": titles.get(probe), "scope_class": scope}

    r = db.call("GET", f"/individuals/{enc(probe)}/similar?k={k}&collection={collection}").json()
    out["nearest"] = [titles.get(x["iri"], x["iri"]) for x in r.get("results", [])]

    detail = db.call("GET", f"/individuals/{enc(probe)}").json()
    inferred_srv = set(detail.get("inferred_classes", [])) - {DBO + "Thing"}
    out["probe_inferred_classes_match_oracle"] = inferred_srv == closure[probe]
    if not out["probe_inferred_classes_match_oracle"]:
        out["probe_inferred_diff"] = {
            "server_only": sorted(inferred_srv - closure[probe]),
            "oracle_only": sorted(closure[probe] - inferred_srv),
        }

    # OxQL number literals allow no exponent, so format every component as plain decimal.
    vec = "[" + ",".join(f"{float(x):.6f}" for x in ents.get_column("vector").to_numpy()[idx]) + "]"
    q = f"FIND ?x WHERE ?x IS-A <{scope}> AND NEAR ?x.{collection} TO {vec} LIMIT {k}"
    r = db.call("POST", "/query", json_body={"query": q}).json()
    results = [x["iri"] for x in r.get("results", [])]
    wrong = [i for i in results if scope not in closure[i]]
    out.update(
        {
            "query": q[:120] + " ...]",
            "results": [
                f"{titles.get(i, i)} ({x.get('score')})" for i, x in zip(results, r.get("results", []))
            ],
            "outside_scope": wrong,
            "elapsed_micros": r.get("elapsed_micros"),
        }
    )
    out["ok"] = bool(results) and not wrong and out["probe_inferred_classes_match_oracle"]
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Load an oxid-dbpedia-ns tier into OxidDB (0.9.9+).")
    ap.add_argument("tier", type=Path, help="tier directory, e.g. out/t50")
    ap.add_argument("--url", default=os.environ.get("OXIDDB_URL", "http://localhost:7878"))
    ap.add_argument("--token", default=os.environ.get("OXIDDB_API_KEY") or None)
    ap.add_argument("--collection", default="dbpedia")
    ap.add_argument(
        "--batch",
        type=int,
        default=400,
        help="entities per /entities/batch request (~7 MB at 1536 dims; cap is max_body_mb)",
    )
    ap.add_argument("--no-abstract", action="store_true", help="do not store the abstract as dbo:abstract")
    ap.add_argument("--no-cold-f32", action="store_true", help="do not request cold_f32 on the collection")
    ap.add_argument(
        "--tbox-lines",
        action="store_true",
        help="import oxid_tbox.txt through /import/csv instead of tbox.ttl",
    )
    ap.add_argument("--skip-tbox", action="store_true")
    ap.add_argument("--skip-entities", action="store_true")
    ap.add_argument(
        "--smoke", action="store_true", help="run the semantic and neurosymbolic smoke tests after loading"
    )
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--report", type=Path, help="write timings and smoke results as JSON here")
    args = ap.parse_args(argv)

    tier = args.tier
    manifest = json.loads((tier / "manifest.json").read_text())
    dims = int(manifest["embedding"].get("dimensions", 1536))
    ents = pl.read_parquet(tier / "entities.parquet")
    edges = pl.read_parquet(tier / "edges.parquet")
    db = Oxid(args.url, args.token)
    print(
        f"OxidDB {args.url}: {db.health()}  tier {manifest['tier_name']} ({ents.height} entities, {edges.height} edges)"
    )
    timed = Timer()
    report: dict = {
        "tier": manifest["tier_name"],
        "entities": ents.height,
        "edges": edges.height,
        "url": args.url,
    }

    with timed("embedder=manual"):
        db.call("PUT", "/config/embedder", json_body={"provider": "manual"})
    with timed("collection"):
        existing = {c["name"] for c in db.call("GET", "/collections").json().get("collections", [])}
        if args.collection not in existing:
            body = {
                "name": args.collection,
                "dimensions": dims,
                "metric": "cosine",
                "quantization": "sq8",
                "cold_f32": not args.no_cold_f32,
            }
            r = db.s.post(f"{db.url}/collections", json=body, timeout=60)
            if r.status_code >= 400 and body["cold_f32"]:
                print(f"  cold_f32 refused ({r.text[:120]}); creating without it")
                body["cold_f32"] = False
                r = db.s.post(f"{db.url}/collections", json=body, timeout=60)
            if r.status_code >= 400:
                raise RuntimeError(f"POST /collections -> {r.status_code}: {r.text[:200]}")
            report["collection"] = body
        db.call("POST", f"/collections/{args.collection}/default", ok=(200, 201, 204, 404))
    if not args.skip_tbox:
        with timed("tbox"):
            if args.tbox_lines:
                report["tbox"] = {"lines": import_lines(db, tier / "oxid_tbox.txt", 10000)}
            else:
                r = db.call("POST", "/import/owl?format=ttl", data=(tier / "tbox.ttl").read_bytes()).json()
                report["tbox"] = {k: v for k, v in r.items() if k != "warnings"}
                frag = db.call("GET", "/reasoner/fragment").json()
                report["tbox"]["fully_supported"] = frag.get("fully_supported")
                if not frag.get("fully_supported"):
                    print(f"  WARNING: reasoner fragment not fully supported: {json.dumps(frag)[:300]}")
    if not args.skip_entities:
        with timed("entities"):
            inserted, errors = load_entities(
                db, ents, edges, args.collection, args.batch, not args.no_abstract
            )
            report["inserted"], report["insert_errors"] = inserted, errors[:20]
            if errors:
                print(f"  {len(errors)} upsert errors, first: {errors[:3]}")
    with timed("classify"):
        report["classify"] = db.call("POST", "/classify").json()
    if args.smoke:
        with timed("smoke"):
            report["smoke"] = smoke(db, tier, ents, args.collection, args.k)
            print(json.dumps(report["smoke"], indent=2, ensure_ascii=False))
    report["timings_s"] = timed.timings
    report["http_requests"] = db.requests
    report["entities_per_second"] = (
        round(ents.height / timed.timings["entities"], 1) if timed.timings.get("entities") else None
    )
    print(
        f"total {sum(timed.timings.values()):.1f}s over {db.requests} requests; {report['entities_per_second']} entities/s on upsert"
    )
    if args.report:
        args.report.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    if args.smoke and not report["smoke"].get("ok"):
        print("SMOKE TEST FAILED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
