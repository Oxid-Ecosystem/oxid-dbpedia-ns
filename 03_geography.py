#!/usr/bin/env python3
"""Stage 3: thin wrapper around `oxid-dbpedia-ns geography`. See oxid_dbpedia_ns/stages/geography.py."""
import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["geography", *sys.argv[1:]]))
