"""IRI normalisation. One tested function, reused by every stage.

Canonical form:
- scheme http, never https
- no trailing slash
- percent-encoded octets decoded to characters, except the characters that are
  not allowed raw inside an IRI (space, quotes, angle brackets, #, %, and a few
  more) which are re-encoded with uppercase hex
- host lowercased
"""

from __future__ import annotations

import re
from urllib.parse import unquote

DBR = "http://dbpedia.org/resource/"
DBO = "http://dbpedia.org/ontology/"
DBP = "http://dbpedia.org/property/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_COMMENT = "http://www.w3.org/2000/01/rdf-schema#comment"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
RDFS_SUBCLASSOF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"
OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
GEO_LAT = "http://www.w3.org/2003/01/geo/wgs84_pos#lat"
GEO_LONG = "http://www.w3.org/2003/01/geo/wgs84_pos#long"
FOAF_NAME = "http://xmlns.com/foaf/0.1/name"

# Characters that must stay percent-encoded inside an IRI reference.
_KEEP_ENCODED = ' "<>#%{}|\\^`'


def _reencode(s: str) -> str:
    """Percent-encode only the characters that cannot appear raw in an IRI; keep Unicode as is."""
    return "".join(f"%{ord(c):02X}" if c in _KEEP_ENCODED or ord(c) < 0x20 else c for c in s)


_HOST_RE = re.compile(r"^(https?)://([^/]+)(/.*)?$", re.DOTALL)
_LANG_HOST_RE = re.compile(r"^http://([a-z\-]+)\.dbpedia\.org/resource/(.*)$", re.DOTALL)


def normalize_iri(iri: str) -> str:
    """Return the canonical form of a DBpedia-style IRI."""
    iri = iri.strip()
    m = _HOST_RE.match(iri)
    if not m:
        return iri
    scheme, host, rest = m.group(1), m.group(2).lower(), m.group(3) or ""
    if scheme == "https":
        scheme = "http"
    if "%" in rest:
        # Decode everything, then re-encode only the characters that must not appear raw.
        rest = _reencode(unquote(rest))
    if len(rest) > 1 and rest.endswith("/"):
        rest = rest.rstrip("/")
    return f"{scheme}://{host}{rest}"


def local_name(iri: str) -> str:
    """The part after the last '/' or '#'."""
    for sep in ("#", "/"):
        idx = iri.rfind(sep)
        if idx >= 0:
            return iri[idx + 1 :]
    return iri


def title_from_iri(iri: str) -> str:
    """Wikipedia title for a DBpedia resource IRI: local name, underscores to spaces, percent-decoded."""
    return unquote(local_name(iri)).replace("_", " ")


def dbr(title: str) -> str:
    """DBpedia resource IRI for a Wikipedia title (spaces or underscores)."""
    return normalize_iri(DBR + title.strip().replace(" ", "_"))


def dbr_lang(lang: str, title: str) -> str:
    """Resource IRI in a language chapter, e.g. http://de.dbpedia.org/resource/Berlin."""
    if lang == "en":
        return dbr(title)
    return normalize_iri(f"http://{lang}.dbpedia.org/resource/{title.strip().replace(' ', '_')}")


def split_lang_resource(iri: str) -> tuple[str, str] | None:
    """('de', 'Berlin') for http://de.dbpedia.org/resource/Berlin, ('en', ...) for the main chapter, else None."""
    if iri.startswith(DBR):
        return "en", iri[len(DBR) :]
    m = _LANG_HOST_RE.match(iri)
    if m:
        return m.group(1), m.group(2)
    return None


def is_resource(iri: str) -> bool:
    return iri.startswith(DBR)
