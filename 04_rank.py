#!/usr/bin/env python3
"""Stage 4: thin wrapper around `oxid-dbpedia-ns rank`. See oxid_dbpedia_ns/stages/rank.py."""
import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["rank", *sys.argv[1:]]))
