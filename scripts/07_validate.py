#!/usr/bin/env python3
"""Stage 7: thin wrapper around `oxid-dbpedia-ns validate`. See oxid_dbpedia_ns/stages/validate.py."""

import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["validate", *sys.argv[1:]]))
