from __future__ import annotations

from rdflib import Graph

from oxid_dbpedia_ns.config import Bucket
from oxid_dbpedia_ns.iri import dbr, normalize_iri, title_from_iri
from oxid_dbpedia_ns.ntriples import parse_lines, unescape_literal
from oxid_dbpedia_ns.ontology import Hierarchy, assign_buckets, el_filter, hierarchy_from_graph
from oxid_dbpedia_ns.stages.rank import interleave


def test_normalize_iri_rules():
    assert normalize_iri("https://dbpedia.org/resource/Berlin/") == "http://dbpedia.org/resource/Berlin"
    assert normalize_iri("http://dbpedia.org/resource/Caf%C3%A9") == "http://dbpedia.org/resource/Café"
    assert normalize_iri("http://dbpedia.org/resource/A%20B") == "http://dbpedia.org/resource/A%20B"
    assert normalize_iri("http://dbpedia.org/resource/A%22B") == "http://dbpedia.org/resource/A%22B"
    assert normalize_iri("http://DBpedia.org/resource/X") == "http://dbpedia.org/resource/X"
    assert normalize_iri("http://dbpedia.org/resource/Café") == "http://dbpedia.org/resource/Café"
    assert dbr("Café de Flore") == "http://dbpedia.org/resource/Café_de_Flore"
    assert title_from_iri("http://dbpedia.org/resource/Caf%C3%A9_de_Flore") == "Café de Flore"


def test_unescape():
    assert unescape_literal(r"a\"b\\c\ndé\U0001F600") == 'a"b\\c\ndé😀'


def test_parse_lines_handles_comments_literals_and_blank_nodes():
    lines = [
        "# header\n",
        '<http://dbpedia.org/resource/X> <http://p> "v\\"q"@en .\n',
        '<http://dbpedia.org/resource/X> <http://p> "3"^^<http://www.w3.org/2001/XMLSchema#int> .\n',
        "_:b <http://p> <https://dbpedia.org/resource/Y/> .\n",
    ]
    df = parse_lines(lines)
    assert df.height == 3
    assert df["o"][0] == 'v"q' and df["lang"][0] == "en"
    assert df["datatype"][1].endswith("#int")
    assert df["s"][2] == "_:b" and df["o"][2] == "http://dbpedia.org/resource/Y"


def _hier() -> Hierarchy:
    dbo = "http://dbpedia.org/ontology/"
    parents = {
        dbo + "Painter": {dbo + "Artist"},
        dbo + "Artist": {dbo + "Person"},
        dbo + "MusicalArtist": {dbo + "Artist"},
        dbo + "Person": {dbo + "Agent"},
        dbo + "Company": {dbo + "Organisation"},
    }
    classes = set(parents) | {c for v in parents.values() for c in v}
    return Hierarchy(classes=classes, parents=parents)


def test_assign_buckets_nearest_root_then_order():
    dbo = "http://dbpedia.org/ontology/"
    buckets = [
        Bucket("Artist", "People", ["Painter", "Artist"], 0.5),
        Bucket("MusicalArtist", "People", ["MusicalArtist"], 0.3),
        Bucket("Company", "Organisations", ["Company"], 0.2),
    ]
    m = assign_buckets(_hier(), buckets)
    assert m[dbo + "Painter"] == "Artist"
    assert (
        m[dbo + "MusicalArtist"] == "MusicalArtist"
    )  # its own root at distance 0 beats Artist at distance 1
    assert m[dbo + "Artist"] == "Artist"
    assert dbo + "Person" not in m and dbo + "Organisation" not in m
    assert m[dbo + "Company"] == "Company"


def test_el_filter_removes_non_el_axioms():
    ttl = """
    @prefix owl: <http://www.w3.org/2002/07/owl#> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix dbo: <http://dbpedia.org/ontology/> .
    dbo:Person a owl:Class . dbo:Place a owl:Class . dbo:City a owl:Class ; rdfs:subClassOf dbo:Place .
    dbo:birthPlace a owl:ObjectProperty , owl:FunctionalProperty .
    dbo:isPartOf a owl:ObjectProperty ; owl:inverseOf dbo:hasPart .
    dbo:Person rdfs:subClassOf [ a owl:Restriction ; owl:onProperty dbo:birthPlace ; owl:allValuesFrom dbo:Place ] .
    dbo:City rdfs:subClassOf [ a owl:Restriction ; owl:onProperty dbo:country ; owl:someValuesFrom dbo:Place ] .
    """
    g = Graph().parse(data=ttl, format="turtle")
    kept, removed, stats = el_filter(g)
    kept_s = kept.serialize(format="nt")
    removed_s = removed.serialize(format="nt")
    assert "FunctionalProperty" in removed_s and "inverseOf" in removed_s and "allValuesFrom" in removed_s
    assert "FunctionalProperty" not in kept_s and "allValuesFrom" not in kept_s
    assert "someValuesFrom" in kept_s
    assert (
        stats["removed_triples"] == 6
    )  # functional, inverseOf, 3 restriction triples + the subClassOf pointing at it
    h = hierarchy_from_graph(kept)
    assert "http://dbpedia.org/ontology/Place" in h.closure("http://dbpedia.org/ontology/City")


def test_interleave_keeps_shares_and_reports_underflow():
    buckets = [Bucket("A", "g", [], 0.5), Bucket("B", "g", [], 0.3), Bucket("C", "g", [], 0.2)]
    pools = {
        "A": [f"a{i}" for i in range(100)],
        "B": [f"b{i}" for i in range(100)],
        "C": [f"c{i}" for i in range(5)],
    }
    res = interleave(pools, buckets, 60)
    assert len(res.order) == 60
    first20 = [b for _, b in res.order[:20]]
    assert first20.count("A") == 10 and first20.count("B") == 6 and first20.count("C") == 4
    assert [u["bucket"] for u in res.underflow] == ["C"]
    # After C runs dry, A and B keep their 5:3 proportion.
    rest = [b for _, b in res.order[30:60]]
    assert abs(rest.count("A") / 30 - 0.625) < 0.05
    # Prefix property: the first 20 of a 60 run equals a 20 run.
    assert res.order[:20] == interleave(pools, buckets, 20).order
