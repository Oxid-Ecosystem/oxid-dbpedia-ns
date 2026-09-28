"""Synthetic DBpedia-shaped fixture: a mini DBO plus a few hundred entities.

Everything the real pipeline consumes is generated here in the same file formats
(line-based N-Triples, bz2-compressed pageview_complete rows), so the whole build can
run in a temporary directory in seconds. Titles are nonsense; the structure is what matters.
"""

from __future__ import annotations

import bz2
import random
import tomllib
from pathlib import Path

DBO = "http://dbpedia.org/ontology/"
DBR = "http://dbpedia.org/resource/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
RDFS_COMMENT = "http://www.w3.org/2000/01/rdf-schema#comment"
RDFS_SUBCLASSOF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
OWL = "http://www.w3.org/2002/07/owl#"
XSD = "http://www.w3.org/2001/XMLSchema#"

# child -> parent
HIERARCHY = {
    "Agent": "Thing",
    "Person": "Agent",
    "Organisation": "Agent",
    "Scientist": "Person",
    "Artist": "Person",
    "Painter": "Artist",
    "Sculptor": "Artist",
    "Photographer": "Artist",
    "MusicalArtist": "Artist",
    "Writer": "Artist",
    "Actor": "Artist",
    "Athlete": "Person",
    "SoccerPlayer": "Athlete",
    "Politician": "Person",
    "Royalty": "Person",
    "Band": "Organisation",
    "Company": "Organisation",
    "EducationalInstitution": "Organisation",
    "University": "EducationalInstitution",
    "SportsTeam": "Organisation",
    "SoccerClub": "SportsTeam",
    "NonProfitOrganisation": "Organisation",
    "Place": "Thing",
    "PopulatedPlace": "Place",
    "Settlement": "PopulatedPlace",
    "City": "Settlement",
    "Town": "Settlement",
    "Country": "PopulatedPlace",
    "Region": "Place",
    "AdministrativeRegion": "Region",
    "ArchitecturalStructure": "Place",
    "Building": "ArchitecturalStructure",
    "Museum": "Building",
    "HistoricPlace": "Place",
    "NaturalPlace": "Place",
    "Mountain": "NaturalPlace",
    "BodyOfWater": "NaturalPlace",
    "Stream": "BodyOfWater",
    "River": "Stream",
    "Lake": "BodyOfWater",
    "Work": "Thing",
    "Film": "Work",
    "MusicalWork": "Work",
    "Album": "MusicalWork",
    "Single": "MusicalWork",
    "WrittenWork": "Work",
    "Book": "WrittenWork",
    "TelevisionShow": "Work",
    "Artwork": "Work",
    "Software": "Work",
    "VideoGame": "Software",
    "Event": "Thing",
    "SocietalEvent": "Event",
    "MilitaryConflict": "SocietalEvent",
    "SportsEvent": "Event",
    "MeanOfTransportation": "Thing",
    "Automobile": "MeanOfTransportation",
    "Aircraft": "MeanOfTransportation",
    "Food": "Thing",
}

BUCKET_TYPES = {
    "Scientist": ["Scientist"],
    "Artist": ["Painter", "Sculptor", "Photographer"],
    "MusicalArtist": ["MusicalArtist", "Band"],
    "Writer": ["Writer"],
    "Athlete": ["SoccerPlayer", "Athlete"],
    "Actor": ["Actor"],
    "City": ["City", "Town"],
    "Country": ["AdministrativeRegion"],
    "Building": ["Building", "Museum", "HistoricPlace"],
    "NaturalPlace": ["Mountain", "River", "Lake"],
    "Company": ["Company"],
    "University": ["University"],
    "SportsTeam": ["SoccerClub"],
    "OtherOrganisation": ["NonProfitOrganisation", "Organisation"],
    "Film": ["Film"],
    "Music": ["Album", "Single"],
    "Book": ["Book"],
    "TelevisionShow": ["TelevisionShow"],
    "ArtworkGame": ["Artwork", "VideoGame"],
    "Event": ["MilitaryConflict", "SportsEvent"],
    "Other": ["Automobile", "Aircraft", "Food"],
}
GROUP_OF = {
    "Scientist": "People",
    "Artist": "People",
    "MusicalArtist": "People",
    "Writer": "People",
    "Athlete": "People",
    "Actor": "People",
    "City": "Places",
    "Country": "Places",
    "Building": "Places",
    "NaturalPlace": "Places",
    "Company": "Organisations",
    "University": "Organisations",
    "SportsTeam": "Organisations",
    "OtherOrganisation": "Organisations",
    "Film": "Works",
    "Music": "Works",
    "Book": "Works",
    "TelevisionShow": "Works",
    "ArtworkGame": "Works",
    "Event": "Events",
    "Other": "Other",
}

TARGET = ["Netherlands", "Germany", "France", "United_States", "United_Kingdom"]
NON_TARGET = ["India", "Japan", "Canada"]
CITIES = {
    "Amsterdam": "Netherlands",
    "Berlin": "Germany",
    "Paris": "France",
    "London": "United_Kingdom",
    "Mumbai": "India",
    "Tokyo": "Japan",
}
LANGS = ["de", "fr", "nl", "es", "it", "pt", "sv", "da", "no", "fi", "pl", "ja", "ru", "zh", "ar"]


def iri(s: str) -> str:
    return f"<{s}>"


def lit(s: str, lang: str | None = None, dt: str | None = None) -> str:
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    if lang:
        return f'"{s}"@{lang}'
    if dt:
        return f'"{s}"^^<{dt}>'
    return f'"{s}"'


class Fixture:
    def __init__(self, root: Path, per_bucket: int = 30, seed: int = 7):
        self.root = root
        self.rng = random.Random(seed)
        self.per_bucket = per_bucket
        self.ontology: list[str] = []
        self.types: list[str] = []
        self.objects: list[str] = []
        self.literals: list[str] = []
        self.abstracts: list[str] = []
        self.sameas: list[str] = []
        self.disamb: list[str] = []
        self.redirects: list[str] = []
        self.pageviews: dict[str, list[str]] = {}
        self.entities: list[dict] = []
        self.q = 0

    # -- ontology -----------------------------------------------------------------
    def build_ontology(self) -> None:
        o = self.ontology
        for c in set(HIERARCHY) | set(HIERARCHY.values()):
            if c == "Thing":
                continue
            o.append(f"{iri(DBO + c)} {iri(RDF_TYPE)} {iri(OWL + 'Class')} .")
            o.append(f"{iri(DBO + c)} {iri(RDFS_LABEL)} {lit(c, 'en')} .")
        for c, p in HIERARCHY.items():
            if p != "Thing":
                o.append(f"{iri(DBO + c)} {iri(RDFS_SUBCLASSOF)} {iri(DBO + p)} .")
        for prop in [
            "birthPlace",
            "deathPlace",
            "nationality",
            "country",
            "isPartOf",
            "author",
            "director",
            "starring",
            "award",
            "locationCountry",
            "headquarter",
            "place",
            "manufacturer",
            "team",
            "almaMater",
            "founder",
            "industry",
            "genre",
            "artist",
            "commander",
            "location",
            "citizenship",
            "origin",
            "writer",
            "musicalArtist",
        ]:
            o.append(f"{iri(DBO + prop)} {iri(RDF_TYPE)} {iri(OWL + 'ObjectProperty')} .")
        for prop in [
            "birthDate",
            "deathDate",
            "populationTotal",
            "areaTotal",
            "foundingYear",
            "numberOfEmployees",
            "releaseDate",
            "runtime",
            "date",
        ]:
            o.append(f"{iri(DBO + prop)} {iri(RDF_TYPE)} {iri(OWL + 'DatatypeProperty')} .")
        # Non-EL axioms that the filter must remove.
        o.append(f"{iri(DBO + 'birthDate')} {iri(RDF_TYPE)} {iri(OWL + 'FunctionalProperty')} .")
        o.append(f"{iri(DBO + 'isPartOf')} {iri(OWL + 'inverseOf')} {iri(DBO + 'hasPart')} .")
        o.append(f"{iri(DBO + 'Person')} {iri(RDFS_SUBCLASSOF)} _:r1 .")
        o.append(f"_:r1 {iri(RDF_TYPE)} {iri(OWL + 'Restriction')} .")
        o.append(f"_:r1 {iri(OWL + 'onProperty')} {iri(DBO + 'birthPlace')} .")
        o.append(f"_:r1 {iri(OWL + 'allValuesFrom')} {iri(DBO + 'Place')} .")
        # An EL-compliant restriction that must survive.
        o.append(f"{iri(DBO + 'City')} {iri(RDFS_SUBCLASSOF)} _:r2 .")
        o.append(f"_:r2 {iri(RDF_TYPE)} {iri(OWL + 'Restriction')} .")
        o.append(f"_:r2 {iri(OWL + 'onProperty')} {iri(DBO + 'country')} .")
        o.append(f"_:r2 {iri(OWL + 'someValuesFrom')} {iri(DBO + 'Country')} .")
        o.append(f"{iri(DBO + 'Person')} {iri(OWL + 'disjointWith')} {iri(DBO + 'Place')} .")

    # -- helpers ---------------------------------------------------------------------
    def add_type(self, name: str, cls: str) -> None:
        self.types.append(f"{iri(DBR + name)} {iri(RDF_TYPE)} {iri(DBO + cls)} .")

    def add_abstract(self, name: str, text: str, encoded_name: str | None = None) -> None:
        self.abstracts.append(f"{iri(DBR + (encoded_name or name))} {iri(RDFS_COMMENT)} {lit(text, 'en')} .")

    def add_obj(self, s: str, p: str, o: str) -> None:
        self.objects.append(f"{iri(DBR + s)} {iri(DBO + p)} {iri(DBR + o)} .")

    def add_lit(self, s: str, p: str, v: str, dt: str) -> None:
        self.literals.append(f"{iri(DBR + s)} {iri(DBO + p)} {lit(v, dt=dt)} .")

    def add_languages(self, name: str, n_langs: int, views: int) -> None:
        self.q += 1
        q = f"<http://wikidata.dbpedia.org/resource/Q{self.q}>"
        same = f"<{OWL}sameAs>"
        self.sameas.append(f"{q} {same} {iri(DBR + name)} .")
        langs = LANGS[: max(0, n_langs - 1)]
        for lg in langs:
            self.sameas.append(f"{q} {same} <http://{lg}.dbpedia.org/resource/{name}_{lg}> .")
        # Pageviews: English gets most of the views, the target editions share the rest, over two months.
        months = list(self.pageviews)
        for m in months:
            en = views // 2
            self.pageviews[m].append(f"en.wikipedia {name} 1 desktop {en // 2} A1")
            self.pageviews[m].append(f"en.wikipedia {name} 1 mobile-web {en - en // 2} A1")
            for lg in langs[:3]:
                self.pageviews[m].append(f"{lg}.wikipedia {name}_{lg} 2 desktop {max(1, views // 20)} A1")

    # -- entities ---------------------------------------------------------------------
    def build_places(self) -> None:
        for c in TARGET + NON_TARGET + ["Kingdom_of_Prussia"]:
            self.add_type(c, "Country")
            self.add_abstract(
                c, f"{c.replace('_', ' ')} is a country with a long history and many notable citizens."
            )
            self.add_languages(c, 40, 200000)
            self.entities.append(
                {
                    "name": c,
                    "bucket": "Country",
                    "group": "Places",
                    "expect_pass": c in TARGET or c == "Kingdom_of_Prussia",
                }
            )
        for city, country in CITIES.items():
            self.add_type(city, "City")
            self.add_abstract(
                city,
                f"{city} is a large city in {country.replace('_', ' ')} known for its museums and canals.",
            )
            self.add_obj(city, "country", country)
            self.add_lit(
                city, "populationTotal", str(self.rng.randint(100000, 5000000)), XSD + "nonNegativeInteger"
            )
            self.add_languages(city, 35, 150000)
            self.entities.append(
                {"name": city, "bucket": "City", "group": "Places", "expect_pass": country in TARGET}
            )
        # Two-hop chain: New_York_City isPartOf New_York (state) which has country United_States.
        self.add_type("New_York", "AdministrativeRegion")
        self.add_abstract("New_York", "New York is a state in the northeastern United States of America.")
        self.add_obj("New_York", "country", "United_States")
        self.add_languages("New_York", 30, 90000)
        self.entities.append(
            {"name": "New_York", "bucket": "Country", "group": "Places", "expect_pass": True}
        )
        self.add_type("New_York_City", "City")
        self.add_abstract(
            "New_York_City", "New York City is the most populous city in the United States of America."
        )
        self.add_obj("New_York_City", "isPartOf", "New_York")
        self.add_languages("New_York_City", 45, 400000)
        self.entities.append(
            {
                "name": "New_York_City",
                "bucket": "City",
                "group": "Places",
                "expect_pass": True,
                "note": "two-hop",
            }
        )
        # A redirect alias used as an object value.
        self.redirects.append(
            f"{iri(DBR + 'USA')} {iri(DBO + 'wikiPageRedirects')} {iri(DBR + 'United_States')} ."
        )
        self.redirects.append(
            f"{iri(DBR + 'Redirect_page')} {iri(DBO + 'wikiPageRedirects')} {iri(DBR + 'Amsterdam')} ."
        )

    def geo_for(self, group: str, name: str, i: int) -> bool:
        """Attach geography properties. Returns the expected pass/fail for the geography filter."""
        r = i % 10
        if r == 9:
            return False  # nothing attached; decided by editions later
        if r == 8:
            country = self.rng.choice(NON_TARGET)
            expect = False
        elif r == 7:
            country = self.rng.choice(["Kingdom_of_Prussia", "Dutch_people", "USA"])
            expect = True
        else:
            country = self.rng.choice(TARGET)
            expect = True
        if group == "People":
            if r % 2 == 0 and country in TARGET + NON_TARGET:
                city = self.rng.choice([c for c, cc in CITIES.items() if cc == country] or [country])
                self.add_obj(name, "birthPlace", city)
            else:
                self.add_obj(name, "nationality", country)
        elif group == "Places":
            self.add_obj(name, "country", country)
        elif group == "Organisations":
            self.add_obj(
                name,
                "locationCountry" if r % 2 else "headquarter",
                country if r % 2 else self.rng.choice(list(CITIES)),
            )
            if r % 2 == 0:
                expect = CITIES[self.objects[-1].split("/resource/")[-1].rstrip("> .")] in TARGET
        elif group == "Works":
            if r % 2:
                self.add_obj(name, "country", country)
            else:
                author = f"Author_{name}"
                self.add_type(
                    author, "Writer"
                )  # no abstract: not a candidate, but resolvable through inheritance
                self.add_obj(author, "nationality", country)
                self.add_obj(name, "author", author)
        elif group == "Events":
            self.add_obj(name, "place", self.rng.choice(list(CITIES)))
            expect = CITIES[self.objects[-1].split("/resource/")[-1].rstrip("> .")] in TARGET
        else:
            maker = f"Maker_{name}"
            self.add_type(maker, "Company")
            self.add_obj(maker, "locationCountry", country)
            self.add_obj(name, "manufacturer", maker)
        return expect

    def build_entities(self) -> None:
        for bucket, types in BUCKET_TYPES.items():
            if bucket in ("Country", "City"):
                continue  # handled by build_places, plus a few extra below
            group = GROUP_OF[bucket]
            for i in range(self.per_bucket):
                cls = types[i % len(types)]
                name = f"{bucket}_{cls}_{i}"
                if i == 3:
                    name = f"Café_{bucket}_{i}"  # unicode title; abstract file uses percent-encoding
                self.add_type(name, cls)
                text = f"{name.replace('_', ' ')} is a well known {cls.lower()} associated with {group.lower()} in the fixture world."
                self.add_abstract(
                    name, text, encoded_name=name.replace("é", "%C3%A9") if "é" in name else None
                )
                expect = self.geo_for(group, name, i)
                n_langs = self.rng.choice([1, 2, 3, 5, 8, 12, 20, 30])
                views = self.rng.choice([10, 50, 200, 1000, 5000, 20000, 80000])
                if i % 10 == 9:
                    n_langs = 12 if i % 20 == 9 else 2  # unresolved: passes on editions or not
                    expect = i % 20 == 9
                self.add_languages(name, n_langs, views)
                if group == "People":
                    self.add_lit(
                        name,
                        "birthDate",
                        f"19{self.rng.randint(10, 99)}-0{self.rng.randint(1, 9)}-1{self.rng.randint(0, 9)}",
                        XSD + "date",
                    )
                    if i % 4 == 0:
                        self.add_obj(name, "award", "Nobel_Prize")
                elif group == "Places":
                    self.add_lit(
                        name,
                        "populationTotal",
                        str(self.rng.randint(1000, 90000)),
                        XSD + "nonNegativeInteger",
                    )
                    self.add_lit(name, "areaTotal", f"{self.rng.random() * 1e6:.1f}", XSD + "double")
                elif group == "Organisations":
                    self.add_lit(name, "foundingYear", str(self.rng.randint(1600, 2000)), XSD + "gYear")
                elif group == "Works":
                    self.add_lit(name, "runtime", f"{self.rng.randint(80, 180) * 60}.0", XSD + "double")
                    if i % 3 == 0:
                        self.add_obj(name, "starring", f"Actor_Actor_{i % self.per_bucket}")
                    if i % 5 == 0:
                        self.add_obj(
                            name, "starring", f"Athlete_SoccerPlayer_{i % self.per_bucket}"
                        )  # not whitelisted target? still an edge if in tier
                self.entities.append({"name": name, "bucket": bucket, "group": group, "expect_pass": expect})
        # Noise that must be dropped in Stage 2.
        self.add_type("List_of_scientists", "Scientist")
        self.add_abstract(
            "List_of_scientists",
            "This is a list of scientists, which should never become a candidate entity.",
        )
        self.add_type("Foo_(disambiguation)", "Scientist")
        self.add_abstract(
            "Foo_(disambiguation)",
            "Foo may refer to several unrelated things; this is a disambiguation page.",
        )
        self.disamb.append(
            f"{iri(DBR + 'Foo_(disambiguation)')} {iri(DBO + 'wikiPageDisambiguates')} {iri(DBR + 'Amsterdam')} ."
        )
        self.add_type("Redirect_page", "Scientist")
        self.add_abstract(
            "Redirect_page", "A redirect page that happens to carry a type and an abstract in the dumps."
        )
        self.add_type("Politician_1", "Politician")
        self.add_abstract(
            "Politician_1", "Politician 1 is a politician, and politicians are not a bucket in this dataset."
        )
        self.add_obj("Politician_1", "nationality", "Netherlands")
        self.add_type("NoAbstract_1", "Scientist")
        self.add_type("Short_1", "Scientist")
        self.add_abstract("Short_1", "Too short.")

    # -- write ------------------------------------------------------------------------
    def write(self) -> dict[str, Path]:
        self.root.mkdir(parents=True, exist_ok=True)
        months = ["2025-01", "2025-02"]
        self.pageviews = {m: [] for m in months}
        self.build_ontology()
        self.build_places()
        self.build_entities()
        files: dict[str, Path] = {}

        def dump(key: str, name: str, lines: list[str], compress: bool) -> None:
            p = self.root / name
            data = ("# fixture\n" + "\n".join(lines) + "\n").encode("utf-8")
            if compress:
                p.write_bytes(bz2.compress(data))
            else:
                p.write_bytes(data)
            files[key] = p

        dump("ontology", "ontology_type=parsed.nt", self.ontology, False)
        dump("instance_types", "instance-types_lang=en_specific.ttl.bz2", self.types, True)
        dump("mappingbased_objects", "mappingbased-objects_lang=en.ttl.bz2", self.objects, True)
        dump("mappingbased_literals", "mappingbased-literals_lang=en.ttl.bz2", self.literals, True)
        dump("short_abstracts", "short-abstracts_lang=en.ttl.bz2", self.abstracts, True)
        dump("sameas_all_wikis", "sameas-all-wikis.ttl.bz2", self.sameas, True)
        dump("disambiguations", "disambiguations_lang=en.ttl.bz2", self.disamb, True)
        dump("redirects", "redirects_lang=en_transitive.ttl.bz2", self.redirects, True)
        for m in months:
            y, mm = m.split("-")
            p = self.root / f"pageviews-{y}{mm}-user.bz2"
            p.write_bytes(bz2.compress(("\n".join(self.pageviews[m]) + "\n").encode("utf-8")))
        files["pageview_template"] = self.root / "pageviews-{year}{month}-user.bz2"
        files["months"] = months  # type: ignore[assignment]
        return files


def write_test_config(
    repo_root: Path, fixture_files: dict, tmp: Path, tiers: list[int] | None = None
) -> Path:
    """Copy the repo config.toml and point it at the fixture, a temp cache/work/out and the fake embedder."""
    import re

    text = (repo_root / "config.toml").read_text(encoding="utf-8")
    cfg = tomllib.loads(text)
    # Deliberately not 50/100/200: those were the historical tier sizes, so a value hardcoded in
    # the pipeline would match the fixture by accident and the test would pass on a real bug. It
    # did exactly that once, and the dataset card shipped naming a `t200` tier that never existed.
    tiers = tiers or [40, 90, 150]
    text = text.replace(f"tiers = {cfg['dataset']['tiers']}", f"tiers = {tiers}")
    for key, path in fixture_files.items():
        if key in ("pageview_template", "months"):
            continue
        text = re.sub(rf'(key = "{key}"\nurl = ")[^"]+(")', rf"\g<1>file://{path}\g<2>", text)
        text = re.sub(rf'(key = "{key}"\nurl = "[^"]+"\nsha256 = ")[^"]*(")', r"\g<1>\g<2>", text)
    text = re.sub(
        r'url_template = "[^"]+"', f'url_template = "file://{fixture_files["pageview_template"]}"', text
    )
    text = re.sub(r"months = \[[^\]]+\]", f"months = {fixture_files['months']}", text)
    text = text.replace('provider = "openai_batch"', 'provider = "fake"')
    text = text.replace("floor_views = 5000", "floor_views = 100").replace(
        "floor_languages = 5", "floor_languages = 2"
    )
    text = re.sub(
        r"probes = \[[^\]]+\]", 'probes = ["Amsterdam", "Scientist Scientist 0", "Nowhere Man"]', text
    )
    text = (
        text.replace('cache = "cache"', f'cache = "{tmp / "cache"}"')
        .replace('work = "work"', f'work = "{tmp / "work"}"')
        .replace('out = "out"', f'out = "{tmp / "out"}"')
    )
    text = text.replace(
        'country_map = "data/country_map.csv"', f'country_map = "{repo_root / "data" / "country_map.csv"}"'
    )
    text = text.replace("LICENSE-DATA", "LICENSE-DATA")
    out = tmp / "config.toml"
    out.write_text(text, encoding="utf-8")
    return out
