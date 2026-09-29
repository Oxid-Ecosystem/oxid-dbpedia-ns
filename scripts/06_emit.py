#!/usr/bin/env python3
"""Stage 6: thin wrapper around `oxid-dbpedia-ns emit`. See oxid_dbpedia_ns/stages/emit.py."""

import sys

from oxid_dbpedia_ns.cli import main

sys.exit(main(["emit", *sys.argv[1:]]))
