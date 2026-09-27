"""DBpedia ontology (DBO) handling: class hierarchy, subclass closure, bucket
assignment and the OWL 2 EL profile filter used for the published TBox."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import OWL, RDF, RDFS

from .config import Bucket
from .iri import DBO, OWL_CLASS, RDF_TYPE, RDFS_SUBCLASSOF, local_name


@dataclass
class Hierarchy:
    classes: set[str]
    parents: dict[str, set[str]] = field(default_factory=dict)

    def ancestors(self, cls: str) -> set[str]:
        """Transitive superclasses, excluding cls itself."""
        seen: set[str] = set()
        stack = list(self.parents.get(cls, ()))
        while stack:
            c = stack.pop()
            if c not in seen:
                seen.add(c)
                stack.extend(self.parents.get(c, ()))
        seen.discard(cls)  # equivalence cycles would otherwise make a class its own ancestor
        return seen

    def closure(self, cls: str) -> set[str]:
        """cls plus all its transitive superclasses."""
        return {cls} | self.ancestors(cls)

    def levels(self, cls: str) -> list[list[str]]:
        """Breadth-first levels: [[cls], [parents], [grandparents], ...] without repeats."""
        out: list[list[str]] = [[cls]]
        seen = {cls}
        frontier = [cls]
        while frontier:
            nxt: list[str] = []
            for c in frontier:
                for p in sorted(self.parents.get(c, ())):
                    if p not in seen:
                        seen.add(p)
                        nxt.append(p)
            if nxt:
                out.append(nxt)
            frontier = nxt
        return out

    def all_closures(self) -> dict[str, set[str]]:
        return {c: self.closure(c) for c in self.classes}


def load_hierarchy(ontology_parquet: Path) -> Hierarchy:
    """Build the DBO class hierarchy from the ontology's N-Triples Parquet conversion."""
    df = pl.read_parquet(ontology_parquet)
    classes = set(
        df.filter((pl.col("p") == RDF_TYPE) & (pl.col("o") == OWL_CLASS) & pl.col("s").str.starts_with(DBO))
        .get_column("s")
        .to_list()
    )
    sub = df.filter(
        (pl.col("p") == RDFS_SUBCLASSOF)
        & pl.col("s").str.starts_with(DBO)
        & pl.col("o").str.starts_with(DBO)
        & pl.col("o_is_iri")
    )
    parents: dict[str, set[str]] = {}
    for s, o in zip(sub.get_column("s").to_list(), sub.get_column("o").to_list()):
        if s == o:
            continue
        parents.setdefault(s, set()).add(o)
        classes.add(s)
        classes.add(o)
    return Hierarchy(classes=classes, parents=parents)


def assign_buckets(h: Hierarchy, buckets: list[Bucket]) -> dict[str, str]:
    """Map every DBO class to a bucket name: walk the class upward level by level;
    the first level that contains a bucket root wins, bucket order breaks ties."""
    root_to_bucket: dict[str, str] = {}
    for b in buckets:  # earlier bucket wins on duplicate roots
        for r in b.roots:
            root_to_bucket.setdefault(DBO + r, b.name)
    unknown = [r for r in root_to_bucket if r not in h.classes]
    if unknown:
        raise ValueError(f"bucket roots not in ontology: {[local_name(u) for u in unknown]}")
    order = {b.name: i for i, b in enumerate(buckets)}
    mapping: dict[str, str] = {}
    for cls in h.classes:
        for level in h.levels(cls):
            hits = [root_to_bucket[c] for c in level if c in root_to_bucket]
            if hits:
                mapping[cls] = min(hits, key=order.__getitem__)
                break
    return mapping


# --- OWL 2 EL profile filter --------------------------------------------------

EL_ALLOWED_PREDICATES = {
    RDF.type,
    RDFS.subClassOf,
    RDFS.subPropertyOf,
    RDFS.domain,
    RDFS.range,
    RDFS.label,
    RDFS.comment,
    RDFS.isDefinedBy,
    RDFS.seeAlso,
    OWL.equivalentClass,
    OWL.equivalentProperty,
    OWL.disjointWith,
    OWL.propertyChainAxiom,
    OWL.someValuesFrom,
    OWL.intersectionOf,
    OWL.onProperty,
    OWL.hasValue,
    OWL.versionInfo,
    OWL.imports,
    OWL.priorVersion,
    OWL.versionIRI,
    OWL.propertyDisjointWith,
    RDF.first,
    RDF.rest,
}
EL_ALLOWED_TYPES = {
    OWL.Class,
    OWL.ObjectProperty,
    OWL.DatatypeProperty,
    OWL.AnnotationProperty,
    OWL.Ontology,
    OWL.TransitiveProperty,
    OWL.ReflexiveProperty,
    OWL.NamedIndividual,
    OWL.Restriction,
    RDFS.Datatype,
    RDFS.Class,
    RDF.Property,
}
# Type assertions and predicates that are outside OWL 2 EL and get moved to tbox_removed.owl.
EL_FORBIDDEN_TYPES = {
    OWL.FunctionalProperty,
    OWL.InverseFunctionalProperty,
    OWL.SymmetricProperty,
    OWL.AsymmetricProperty,
    OWL.IrreflexiveProperty,
}
EL_FORBIDDEN_PREDICATES = {
    OWL.inverseOf,
    OWL.unionOf,
    OWL.complementOf,
    OWL.allValuesFrom,
    OWL.oneOf,
    OWL.disjointUnionOf,
    OWL.cardinality,
    OWL.minCardinality,
    OWL.maxCardinality,
    OWL.qualifiedCardinality,
    OWL.minQualifiedCardinality,
    OWL.maxQualifiedCardinality,
    OWL.hasSelf,
    OWL.hasKey,
}

# DBO also links to external vocabularies with these annotation-like predicates; they are harmless.
PROV = Namespace("http://www.w3.org/ns/prov#")


def _bnode_closure(g: Graph, node: BNode) -> set[BNode]:
    """All blank nodes reachable from node (a restriction or list)."""
    seen: set[BNode] = set()
    q: deque[BNode] = deque([node])
    while q:
        n = q.popleft()
        if n in seen:
            continue
        seen.add(n)
        for _, _, o in g.triples((n, None, None)):
            if isinstance(o, BNode):
                q.append(o)
    return seen


def el_filter(g: Graph) -> tuple[Graph, Graph, dict]:
    """Split an ontology graph into an OWL 2 EL compliant part and the removed remainder.

    Rules applied triple by triple:
    - rdf:type with a forbidden type (functional, inverse functional, symmetric, ...) is removed;
    - forbidden predicates (inverseOf, unionOf, allValuesFrom, cardinalities, ...) are removed;
    - a blank-node expression (restriction, list) that contains any forbidden triple is removed as a whole,
      together with the axiom that points at it;
    - every other triple, including unknown annotation predicates, is kept.
    Datatype ranges are kept as they are; OWL 2 EL restricts datatypes, and a downstream loader may
    map unsupported XSD types itself. The manifest records the removed count.
    """
    kept, removed = Graph(), Graph()
    for prefix, ns in g.namespaces():
        kept.bind(prefix, ns)
        removed.bind(prefix, ns)

    bad_bnodes: set[BNode] = set()
    for s, p, o in g:
        forbidden = p in EL_FORBIDDEN_PREDICATES or (p == RDF.type and o in EL_FORBIDDEN_TYPES)
        if forbidden and isinstance(s, BNode):
            bad_bnodes.add(s)
    # Propagate badness to every blank node that references a bad one.
    changed = True
    while changed:
        changed = False
        for s, _p, o in g:
            if isinstance(o, BNode) and o in bad_bnodes and isinstance(s, BNode) and s not in bad_bnodes:
                bad_bnodes.add(s)
                changed = True

    reasons: dict[str, int] = {}
    for s, p, o in g:
        why = None
        if p in EL_FORBIDDEN_PREDICATES:
            why = f"predicate {g.namespace_manager.normalizeUri(p)}"
        elif p == RDF.type and o in EL_FORBIDDEN_TYPES:
            why = f"type {g.namespace_manager.normalizeUri(o)}"
        elif isinstance(s, BNode) and s in bad_bnodes:
            why = "member of removed anonymous expression"
        elif isinstance(o, BNode) and o in bad_bnodes:
            why = "axiom pointing at removed anonymous expression"
        if why:
            removed.add((s, p, o))
            reasons[why] = reasons.get(why, 0) + 1
        else:
            kept.add((s, p, o))
    stats = {"kept_triples": len(kept), "removed_triples": len(removed), "removed_by_reason": reasons}
    return kept, removed, stats


def clean_tbox(
    g: Graph,
    drop_non_ascii_dbo: bool = True,
    drop_external_equivalents: bool = True,
    label_languages: tuple[str, ...] = ("en",),
) -> tuple[Graph, dict]:
    """Remove DBO artefacts that are noise for a demo TBox.

    - DBO carries ~1,000 Urdu-localised duplicate classes and properties (non-ASCII local names,
      linked to their English twins); a reasoner treats them as extra superclasses of everything.
    - equivalentClass / subClassOf links to wikidata, schema.org and DUL make a loader create hundreds of foreign classes.
    - Labels and comments in 40 languages quadruple the file size.
    Returns the cleaned graph and counts per rule.
    """
    out = Graph()
    for prefix, ns in g.namespaces():
        out.bind(prefix, ns)
    stats = {
        "non_ascii_dbo": 0,
        "external_equivalents": 0,
        "foreign_language_annotations": 0,
        "subclass_of_non_class": 0,
    }
    label_preds = {RDFS.label, RDFS.comment}
    declared_classes = set(g.subjects(RDF.type, OWL.Class))
    for s_, p_, o_ in g:
        # DBO bug: a few classes are declared subclasses of a *property* (dbo:Hospital ⊑ dbo:building).
        if (
            p_ == RDFS.subClassOf
            and isinstance(o_, URIRef)
            and str(o_).startswith(DBO)
            and o_ not in declared_classes
        ):
            stats["subclass_of_non_class"] += 1
            continue
        if drop_non_ascii_dbo and any(
            isinstance(x, URIRef) and str(x).startswith(DBO) and not str(x).isascii() for x in (s_, o_)
        ):
            stats["non_ascii_dbo"] += 1
            continue
        if (
            drop_external_equivalents
            and p_
            in (OWL.equivalentClass, OWL.equivalentProperty, OWL.sameAs, RDFS.subClassOf, RDFS.subPropertyOf)
            and isinstance(o_, URIRef)
            and not str(o_).startswith(DBO)
            and o_ not in (OWL.Thing,)
        ):
            stats["external_equivalents"] += 1
            continue
        if (
            p_ in label_preds
            and isinstance(o_, Literal)
            and o_.language
            and o_.language not in label_languages
        ):
            stats["foreign_language_annotations"] += 1
            continue
        out.add((s_, p_, o_))
    return out, stats


def load_ontology_graph(path: Path) -> Graph:
    """Parse an ontology file. DBpedia names its N-Triples dumps `.ttl`, so sniff the content."""
    g = Graph()
    fmt = None
    if path.suffix in (".nt", ".ttl"):
        with open(path, encoding="utf-8", errors="replace") as f:
            head = "".join(f.readline() for _ in range(20))
        fmt = "turtle" if "@prefix" in head or "@base" in head or "PREFIX " in head else "nt"
    g.parse(str(path), format=fmt)
    return g


def hierarchy_from_graph(g: Graph) -> Hierarchy:
    classes: set[str] = set()
    parents: dict[str, set[str]] = {}
    for s in g.subjects(RDF.type, OWL.Class):
        if isinstance(s, URIRef) and str(s).startswith(DBO):
            classes.add(str(s))
    for s, o in g.subject_objects(RDFS.subClassOf):
        if (
            isinstance(s, URIRef)
            and isinstance(o, URIRef)
            and str(s).startswith(DBO)
            and str(o).startswith(DBO)
        ):
            parents.setdefault(str(s), set()).add(str(o))
            classes.add(str(s))
            classes.add(str(o))
    # Equivalent named classes subsume each other: model the link as a subclass edge both ways.
    for s, o in g.subject_objects(OWL.equivalentClass):
        if (
            isinstance(s, URIRef)
            and isinstance(o, URIRef)
            and str(s).startswith(DBO)
            and str(o).startswith(DBO)
            and s != o
        ):
            parents.setdefault(str(s), set()).add(str(o))
            parents.setdefault(str(o), set()).add(str(s))
            classes.add(str(s))
            classes.add(str(o))
    return Hierarchy(classes=classes, parents=parents)


def class_labels(g: Graph) -> dict[str, str]:
    out: dict[str, str] = {}
    for s, o in g.subject_objects(RDFS.label):
        if isinstance(s, URIRef) and isinstance(o, Literal) and (o.language in (None, "en")):
            out.setdefault(str(s), str(o))
    return out
