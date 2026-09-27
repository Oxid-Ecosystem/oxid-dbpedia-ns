#!/usr/bin/env python3
"""Stage 2: thin wrapper around `oxid-dbpedia-ns candidates`. See oxid_dbpedia_ns/stages/candidates.py."""
import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["candidates", *sys.argv[1:]]))
