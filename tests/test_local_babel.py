import multiprocessing
import pprint
import sqlite3

import pytest
from stitch.local_babel import (
    connect_to_db_read_only,
    get_n_random_curies,
    get_taxon_for_gene_or_protein,
    map_any_curie_to_cliques,
    map_curie_to_conflation_curies,
    map_curie_to_preferred_curies,
    map_curies_to_conflation_curies,
    map_curies_to_preferred_curies,
    map_pref_curie_to_synonyms,
    map_preferred_curie_to_cliques,
)


@pytest.fixture(scope="session")
def db_filename() -> str:
    return "db/babel-20250901.sqlite"

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
    curies = map_curie_to_conflation_curies(readonly_conn, "RXCUI:1014098", 1)
    assert len(curies) >= 14
    curies = map_curie_to_conflation_curies(readonly_conn, "XYZZY:533234", 1)
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
