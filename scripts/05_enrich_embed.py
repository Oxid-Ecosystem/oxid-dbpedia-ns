#!/usr/bin/env python3
"""Stage 5: thin wrapper around `oxid-dbpedia-ns enrich`. See oxid_dbpedia_ns/stages/enrich_embed.py."""

import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["enrich", *sys.argv[1:]]))
