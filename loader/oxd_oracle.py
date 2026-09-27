#!/usr/bin/env python3
"""Differential oracle: compare a tier's symbolic outputs with OxidDB's offline reasoner.

Uses the `oxd` CLI (0.9.9+) inside a running container or on PATH, without touching any database:

  oxd hierarchy tbox.ttl                  -> inferred class subsumptions  vs  the Python closure of tbox.ttl
  oxd extents oxid_tbox.txt+oxid_abox.txt -> inferred class memberships    vs  types_inferred.parquet

    python loader/oxd_oracle.py out/t50 --docker oxid-db-099
    python loader/oxd_oracle.py out/t50 --oxd /path/to/oxd

Exit code 0 when both sets are identical.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
from pathlib import Path

import polars as pl

DBO = "http://dbpedia.org/ontology/"


def run_oxd(args: argparse.Namespace, sub: str, local_file: Path) -> set[tuple[str, str]]:
    if args.docker:
        remote = f"/tmp/oracle_{local_file.name}"
        subprocess.run(["docker", "cp", str(local_file), f"{args.docker}:{remote}"], check=True)
        cmd = ["docker", "exec", args.docker, "oxd", sub, remote]
    else:
        cmd = [args.oxd, sub, str(local_file)]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        sys.exit(f"{' '.join(cmd)} failed (rc={proc.returncode}):\n{proc.stderr[-2000:]}")
    out = proc.stdout
    return {tuple(line.split("\t")) for line in out.splitlines() if line.strip()}  # type: ignore[misc]


def python_hierarchy(tbox_ttl: Path) -> set[tuple[str, str]]:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from oxid_dbpedia_ns.ontology import hierarchy_from_graph, load_ontology_graph

    h = hierarchy_from_graph(load_ontology_graph(tbox_ttl))
    return {(c, a) for c in h.classes for a in h.ancestors(c) if a != DBO + "Thing"}


def report(name: str, ours: set, theirs: set) -> bool:
    only_theirs, only_ours = theirs - ours, ours - theirs
    ok = not only_theirs and not only_ours
    print(f"{name}: pipeline {len(ours)} rows, oxd {len(theirs)} rows -> {'MATCH' if ok else 'DIFFER'}")
    for label, diff in (("only in oxd", only_theirs), ("only in pipeline", only_ours)):
        if diff:
            print(f"  {label}: {len(diff)}, e.g. {sorted(diff)[:3]}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description="Compare tier outputs with OxidDB's offline reasoner.")
    ap.add_argument("tier", type=Path)
    ap.add_argument("--docker", help="container name running the oxd image (uses docker cp + docker exec)")
    ap.add_argument("--oxd", default="oxd", help="oxd binary when not using --docker")
    args = ap.parse_args()
    tier = args.tier

    ok = report(
        "hierarchy (tbox.ttl)",
        python_hierarchy(tier / "tbox.ttl"),
        run_oxd(args, "hierarchy", tier / "tbox.ttl"),
    )

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as f:
        f.write((tier / "oxid_tbox.txt").read_text(encoding="utf-8"))
        f.write((tier / "oxid_abox.txt").read_text(encoding="utf-8"))
        combined = Path(f.name)
    combined.chmod(0o644)  # docker cp keeps the mode, and oxd runs unprivileged in the container
    inferred = pl.read_parquet(tier / "types_inferred.parquet")
    ours = {(i, t) for i, t in inferred.iter_rows()}
    ok &= report("extents (types_inferred.parquet)", ours, run_oxd(args, "extents", combined))
    combined.unlink(missing_ok=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
