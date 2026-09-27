#!/usr/bin/env python3
"""Stage 1: thin wrapper around `oxid-dbpedia-ns acquire`. See oxid_dbpedia_ns/stages/acquire.py."""
import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["acquire", *sys.argv[1:]]))
