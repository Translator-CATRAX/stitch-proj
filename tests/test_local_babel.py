import multiprocessing
import pprint
import sqlite3

import pytest
from stitch.local_babel import (
    _batch_tuple,
    _map_with_batching,
    connect_to_db_read_only,
    get_all_names_for_curie,
    get_categories_for_curie,
    get_label_for_curie,
    get_n_random_curies,
    get_table_row_counts,
    get_taxon_for_gene_or_protein,
    map_any_curie_to_cliques,
    map_curie_to_conflation_curies,
    map_curie_to_preferred_curies,
    map_curies_to_conflation_curies,
    map_curies_to_preferred_curies,
    map_name_to_curie,
    map_pref_curie_to_synonyms,
    map_preferred_curie_to_cliques,
)


@pytest.fixture(scope="session")
def db_filename() -> str:
    return "db/babel-20250901-p3.sqlite"

@pytest.fixture(scope="function")
def pool():
    with multiprocessing.Pool(processes=10) as p:
        yield p

@pytest.fixture(scope="function")
def readonly_conn(db_filename):
    with connect_to_db_read_only(db_filename) as conn:
        yield conn

def test_get_n_random_curies(db_filename: str):
    with multiprocessing.Pool(processes=4) as pool:
        curies = get_n_random_curies(db_filename, 1000, pool)
    assert isinstance(curies, tuple)
    assert len(curies) == 1000
    assert all(isinstance(c, str) for c in curies)


def test_map_any_curie_to_cliques(readonly_conn: sqlite3.Connection):
    curie = "MESH:D014867"
    results = map_any_curie_to_cliques(readonly_conn, curie)
    assert isinstance(results, tuple)
    for row in results:
        assert "id" in row
        assert "type" in row
        assert "ic" in row
        assert isinstance(row['ic'], float)
        assert isinstance(row['type'], list)


def test_map_pref_curie_to_synonyms(readonly_conn: sqlite3.Connection):
    curie = "CHEBI:15377"
    results = map_pref_curie_to_synonyms(readonly_conn.cursor(), curie)
    assert isinstance(results, set)
    assert all(isinstance(x, str) for x in results)


def test_map_preferred_curie_to_cliques(readonly_conn: sqlite3.Connection):
    curie = "CHEBI:15377"
    results = map_preferred_curie_to_cliques(readonly_conn, curie)
    assert isinstance(results, tuple)
    pprint.pprint(results)
    for item in results:
        assert "id" in item
        assert "ic" in item


def test_map_curies_to_preferred_curies(db_filename: str):
    results = map_curies_to_preferred_curies(db_filename,
                                             ("MESH:D014867",
                                              "CHEBI:15377",
                                              "HP:0001300",
                                              "XYZZY:3432432"),
                                             None)
    assert results == (('CHEBI:15377', 'biolink:SmallMolecule', 'MESH:D014867'),
                       ('CHEBI:15377', 'biolink:SmallMolecule', 'CHEBI:15377'),
                       ('MONDO:0021095', 'biolink:Disease', 'HP:0001300'))

def test_map_curies_to_preferred_curies_big(db_filename: str):
    with multiprocessing.Pool(processes=4) as pool:
        curies = get_n_random_curies(db_filename, 1000, pool)
        assert len(curies)==1000
        mapped = map_curies_to_preferred_curies(db_filename, curies, pool)
    assert isinstance(mapped, tuple)
    unique_ids_mapped = tuple(set(row[2] for row in mapped))
    assert len(unique_ids_mapped) <= len(curies)

def test_map_curie_to_conflation_curies(readonly_conn: sqlite3.Connection):
    curies = map_curie_to_conflation_curies(readonly_conn,
                                            "RXCUI:1014098", 'DrugChemical')
    assert len(curies) >= 14
    curies = map_curie_to_conflation_curies(readonly_conn,
                                            "XYZZY:533234", 'DrugChemical')
    assert not curies

def test_map_curies_to_conflation_curies(db_filename: str):
    with multiprocessing.Pool(processes=4) as pool:
        curies = map_curies_to_conflation_curies(db_filename,
                                                 ("RXCUI:1014098",
                                                  "XYZZY:533234",
                                                  "RXCUI:1014098"),
                                                 pool)
    assert len(curies) >= 28


def test_get_taxon_for_gene_or_protein(readonly_conn):
    curie = get_taxon_for_gene_or_protein(readonly_conn,
                                          'NCBIGene:3569')
    assert curie == 'NCBITaxon:9606'
    curie = get_taxon_for_gene_or_protein(readonly_conn,
                                          'NCBIGene:41')
    assert curie == 'NCBITaxon:9606'
    curie = get_taxon_for_gene_or_protein(readonly_conn,
                                          'XYZZY:234334')
    assert curie is None
    curie = get_taxon_for_gene_or_protein(readonly_conn,
                                          'NCBIGene:16193')
    assert curie == 'NCBITaxon:10090'


def test_map_curie_to_preferred_curies(readonly_conn: sqlite3.Connection):
    res = map_curie_to_preferred_curies(readonly_conn, 'RXCUI:1014098')
    assert res == (('RXCUI:1014098', 'biolink:Drug', 'RXCUI:1014098'),)
    res = map_curie_to_preferred_curies(readonly_conn, 'XYZZY:23434334')
    assert len(res)==0
    res = map_curie_to_preferred_curies(readonly_conn, 'MESH:D014867')
    assert res == (('CHEBI:15377', 'biolink:SmallMolecule', 'MESH:D014867'),)
    res = map_curie_to_preferred_curies(readonly_conn, 'MESH:C115990')
    assert set(res) == {('MESH:C115990', 'biolink:ChemicalEntity', 'MESH:C115990'),
                        ('UMLS:C0000657', 'biolink:Protein', 'MESH:C115990')}


def test_map_chembl(readonly_conn: sqlite3.Connection):
    res = map_curie_to_preferred_curies(readonly_conn, 'CHEMBL.COMPOUND:CHEMBL339829')
    assert res == (('CHEMBL.COMPOUND:CHEMBL339829',
                    'biolink:ChemicalEntity',
                    'CHEMBL.COMPOUND:CHEMBL339829'),)


def _is_case_insensitive_sorted(t: tuple[str, ...]) -> bool:
    return list(t) == sorted(t, key=str.casefold)


def test_get_all_names_for_curie(readonly_conn: sqlite3.Connection):
    # Known-good CURIE from your other tests
    curie = "CHEBI:15377"

    names = get_all_names_for_curie(readonly_conn, curie)

    assert isinstance(names, tuple)
    assert len(names) > 0
    assert all(isinstance(x, str) for x in names)
    assert all(x.strip() != "" for x in names)  # non-empty, non-whitespace
    assert len(names) == len(set(names))  # DISTINCT in SQL

    # ORDER BY name COLLATE NOCASE
    assert _is_case_insensitive_sorted(names)

    # The identifier table label is included (the query always unions ti.label)
    # We don't hardcode the actual label string (db can change), just assert it’s
    # present.
    label = get_label_for_curie(readonly_conn, curie)
    assert label is not None
    assert isinstance(label, str)
    assert label.strip() != ""
    assert label in names

    # Unknown CURIE => empty tuple
    missing = get_all_names_for_curie(readonly_conn, "XYZZY:does_not_exist")
    assert missing == ()


def test_get_categories_for_curie(readonly_conn: sqlite3.Connection):
    # Unknown CURIE => empty
    missing = get_categories_for_curie(readonly_conn, "XYZZY:does_not_exist")
    assert missing == ()

    # A known CURIE from your existing tests
    curie = "MESH:D014867"
    cats = get_categories_for_curie(readonly_conn, curie)

    assert isinstance(cats, tuple)
    assert len(cats) > 0
    assert all(isinstance(x, str) for x in cats)
    assert all(x.strip() != "" for x in cats)
    assert len(cats) == len(set(cats))  # DISTINCT in SQL
    assert _is_case_insensitive_sorted(cats)

    # Sanity: these are type CURIEs and should look like CURIEs (contain ":")
    assert all(":" in x for x in cats)

    # Optional but useful: conflation can expand categories.
    # In your tests, RXCUI:1014098 participates in DrugChemical con

def test_map_any_curie_to_cliques_ordering(readonly_conn: sqlite3.Connection):
    curie = "MESH:D014867"
    results = map_any_curie_to_cliques(readonly_conn, curie)
    assert isinstance(results, tuple)
    assert len(results) > 0
    identifiers = [r['id']['identifier'] for r in results]
    assert identifiers == sorted(identifiers), (
        f"Results should be sorted by identifier, got: {identifiers}"
    )
    results_again = map_any_curie_to_cliques(readonly_conn, curie)
    assert results == results_again, "Results should be identical across repeated calls"

def _make_ordering_test_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE types (id INTEGER PRIMARY KEY, curie TEXT);
        CREATE TABLE identifiers (id INTEGER PRIMARY KEY, curie TEXT, label TEXT);
        CREATE TABLE descriptions (id INTEGER PRIMARY KEY, desc TEXT);
        CREATE TABLE cliques (
            id INTEGER PRIMARY KEY,
            primary_identifier_id INTEGER,
            type_id INTEGER,
            ic REAL,
            preferred_name TEXT
        );
        CREATE TABLE identifiers_cliques (identifier_id INTEGER, clique_id INTEGER);
        CREATE TABLE identifiers_descriptions
                                    (identifier_id INTEGER, description_id INTEGER);

        INSERT INTO types VALUES (1, 'biolink:SmallMolecule');
        INSERT INTO identifiers VALUES (1, 'CHEBI:1', 'Compound A'),
                                       (2, 'CHEBI:2', 'Compound B'),
                                       (3, 'CHEBI:3', 'Compound C'),
                                       (4, 'MESH:D014867', 'Water');
        INSERT INTO descriptions VALUES (1, 'Desc A'), (2, 'Desc B'), (3, 'Desc C');
        INSERT INTO cliques VALUES (3, 3, 1, 0.1, 'Compound C'),
                                   (1, 1, 1, 0.9, 'Compound A'),
                                   (2, 2, 1, 0.5, 'Compound B');
        INSERT INTO identifiers_cliques VALUES (4, 3), (4, 1), (4, 2);
        INSERT INTO identifiers_descriptions VALUES (1, 1), (2, 2), (3, 3);
    """)
    return conn

def test_map_any_curie_to_cliques_ordering_based_on_test_db():
    conn = _make_ordering_test_db()
    results = map_any_curie_to_cliques(conn, "MESH:D014867")
    assert isinstance(results, tuple)
    assert len(results) == 3
    identifiers = [r['id']['identifier'] for r in results]
    assert identifiers == sorted(identifiers), (
        f"Results should be sorted by identifier, got: {identifiers}"
    )
    results_again = map_any_curie_to_cliques(conn, "MESH:D014867")
    assert results == results_again, "Results should be identical across repeated calls"


def _make_name_lookup_test_db() -> sqlite3.Connection:
    """In-memory `identifiers` table for exercising `map_name_to_curie`."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE identifiers (id INTEGER PRIMARY KEY, curie TEXT, label TEXT);
        INSERT INTO identifiers (curie, label) VALUES
            ('CHEBI:15365',  'Aspirin'),
            ('MESH:D014409', 'Interleukin 6 receptor'),
            ('CHEBI:9754',   'Therapy'),
            ('X:1',          'abcdef'),
            ('PMID:12345',   'Pmidexclusive label');
    """)
    return conn


def test_map_name_to_curie_basic():
    conn = _make_name_lookup_test_db()

    # Prefix match: the query is a prefix of a stored label, and the full
    # stored label is returned as the second element of the tuple.
    assert map_name_to_curie(conn, "Aspi") == ("CHEBI:15365", "Aspirin")
    assert map_name_to_curie(conn, "Aspirin") == ("CHEBI:15365", "Aspirin")

    # LIKE is case-insensitive for ASCII.
    assert map_name_to_curie(conn, "aspirin") == ("CHEBI:15365", "Aspirin")

    # Surrounding and repeated whitespace is normalized away.
    assert map_name_to_curie(conn, "   Aspirin   ") == ("CHEBI:15365", "Aspirin")

    # No label has this as a prefix -> None.
    assert map_name_to_curie(conn, "Zzz nonexistent substance") is None


def test_map_name_to_curie_excludes_pmid():
    conn = _make_name_lookup_test_db()
    # 'Pmidexclusive label' exists, but only on a PMID: curie, which the
    # query explicitly filters out -> no usable match.
    assert map_name_to_curie(conn, "Pmidexclusive") is None


def test_map_name_to_curie_variant_generation():
    conn = _make_name_lookup_test_db()

    # Trailing parenthetical is stripped: "Aspirin (...)" -> "Aspirin".
    assert map_name_to_curie(conn, "Aspirin (pain reliever)") == \
        ("CHEBI:15365", "Aspirin")

    # Inline "(ABBR)" is removed: "... (IL6) ..." -> "Interleukin 6 receptor".
    assert map_name_to_curie(conn, "Interleukin 6 (IL6) receptor") == \
        ("MESH:D014409", "Interleukin 6 receptor")

    # Naive singularization: trailing "s" -> "" and "ies" -> "y".
    assert map_name_to_curie(conn, "Aspirins") == ("CHEBI:15365", "Aspirin")
    assert map_name_to_curie(conn, "Therapies") == ("CHEBI:9754", "Therapy")


def test_map_name_to_curie_escapes_like_wildcards():
    conn = _make_name_lookup_test_db()
    # 'abcdef' is in the DB. If '%' / '_' were not escaped they would act
    # as LIKE wildcards and match 'abcdef'; escaped, they are literal, so
    # these queries must NOT match.
    assert map_name_to_curie(conn, "ab%f") is None
    assert map_name_to_curie(conn, "a_c") is None
    # Sanity check: a plain literal prefix of 'abcdef' does match.
    assert map_name_to_curie(conn, "abc") == ("X:1", "abcdef")


def test_get_label_for_curie_in_memory():
    conn = _make_name_lookup_test_db()

    # Known CURIE -> its exact identifiers.label.
    assert get_label_for_curie(conn, "CHEBI:15365") == "Aspirin"
    assert get_label_for_curie(conn, "MESH:D014409") == "Interleukin 6 receptor"

    # Unknown CURIE -> None.
    assert get_label_for_curie(conn, "XYZZY:does_not_exist") is None


def test_get_label_for_curie_real_db(readonly_conn: sqlite3.Connection):
    curie = "CHEBI:15377"

    # The helper should return exactly what the raw identifiers query returns.
    label_row = readonly_conn.cursor().execute(
        "SELECT label FROM identifiers WHERE curie = ?",
        (curie,),
    ).fetchone()
    assert label_row is not None
    expected_label = label_row[0]

    label = get_label_for_curie(readonly_conn, curie)
    assert label == expected_label
    assert isinstance(label, str)
    assert label.strip() != ""

    # Unknown CURIE -> None.
    assert get_label_for_curie(readonly_conn, "XYZZY:does_not_exist") is None


def test_map_name_to_curie_real_db(readonly_conn: sqlite3.Connection):
    # Obviously-absent name -> None.
    assert map_name_to_curie(readonly_conn,
                             "Zzz nonexistent substance 9999") is None

    # A real label should resolve to some (curie, label) pair. We do not
    # assert the exact curie, since labels/contents can change between
    # Babel releases and a prefix match may land on a sibling label.
    label = get_label_for_curie(readonly_conn, "CHEBI:15377")
    assert label

    result = map_name_to_curie(readonly_conn, label)
    assert result is not None
    curie, matched_label = result
    assert isinstance(curie, str) and curie != ""
    assert isinstance(matched_label, str) and matched_label != ""
    assert not curie.startswith("PMID:")

def test_batch_tuple():
    result = _batch_tuple((1, 2, 3, 4, 5), 2)
    assert result == ((1, 2), (3, 4), (5,))
    result = _batch_tuple((), 5)
    assert result == ()
    result = _batch_tuple((1, 2, 3), 10)
    assert result == ((1, 2, 3),)


def test_batch_tuple_size_one():
    assert _batch_tuple((1, 2, 3), 1) == ((1,), (2,), (3,))


def test_batch_tuple_invalid_batch_size():
    with pytest.raises(ValueError):
        _batch_tuple((1, 2, 3), 0)
    with pytest.raises(ValueError):
        _batch_tuple((1, 2, 3), -1)


def test_map_with_batching():
    result = _map_with_batching((1, 2, 3, 4, 5),
                                lambda batch: [x * 2 for x in batch],
                                None,
                                max_batch_size=2)
    assert set(result) == {2, 4, 6, 8, 10}
    result = _map_with_batching((), lambda batch: [], None)
    assert result == ()


def test_get_table_row_counts():
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO t1 VALUES (1, 'a'), (2, 'b');
        CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO t2 VALUES (1, 'c');
    """)
    counts = get_table_row_counts(conn)
    assert counts['t1'] == 2
    assert counts['t2'] == 1


def test_get_table_row_counts_zero_rows():
    conn = sqlite3.connect(":memory:")
    conn.executescript("CREATE TABLE empty_table (id INTEGER PRIMARY KEY);")
    counts = get_table_row_counts(conn)
    assert counts == {"empty_table": 0}


def test_get_table_row_counts_empty_database():
    conn = sqlite3.connect(":memory:")
    counts = get_table_row_counts(conn)
    assert counts == {}


def test_map_curie_to_conflation_curies_gene_protein(readonly_conn):
    curies = map_curie_to_conflation_curies(readonly_conn,
                                            "NCBIGene:3569", "GeneProtein")
    assert len(curies) >= 1
    curies = map_curie_to_conflation_curies(readonly_conn,
                                            "XYZZY:999999", "GeneProtein")
    assert not curies


def test_map_pref_curie_to_synonyms_unknown_curie(readonly_conn: sqlite3.Connection):
    result = map_pref_curie_to_synonyms(readonly_conn.cursor(), "XYZZY:does_not_exist")
    assert result == set()


def test_map_preferred_curie_to_cliques_unknown_curie(
        readonly_conn: sqlite3.Connection
):
    result = map_preferred_curie_to_cliques(readonly_conn, "XYZZY:does_not_exist")
    assert result == ()
