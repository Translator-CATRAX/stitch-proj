
import multiprocessing
import pprint
import sqlite3
import time

import pytest
from stitch.local_babel import (
    connect_to_db_read_only,
    get_n_random_curies,
    map_any_curie_to_cliques,
    map_curies_to_preferred_curies,
    map_pref_curie_to_synonyms,
    map_preferred_curie_to_cliques,
)


@pytest.fixture(scope="session")
def db_filename() -> str:
    return "db/babel-20250123.sqlite"

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
    for item in results:
        assert "id" in item
        assert "type" in item


def test_map_pref_curie_to_synonyms(readonly_conn: sqlite3.Connection):
    curie = "CHEBI:15377"
    results = map_pref_curie_to_synonyms(readonly_conn.cursor(), curie)
    assert isinstance(results, set)
    assert all(isinstance(x, str) for x in results)


def test_map_preferred_curie_to_cliques(readonly_conn: sqlite3.Connection):
    curie = "CHEBI:15377"
    results = map_preferred_curie_to_cliques(readonly_conn, curie)
    assert isinstance(results, tuple)
    for item in results:
        assert "id" in item
        assert "ic" in item


def test_map_curies_to_preferred_curies(db_filename: str):
    results = map_curies_to_preferred_curies(db_filename,
                                             ("MESH:D014867",
                                              "CHEBI:15377",
                                              "HP:0001300"),
                                             None)
    pprint.pprint(results)


def test_map_curies_to_preferred_curies_big(db_filename: str):
    with multiprocessing.Pool(processes=4) as pool:
        curies = get_n_random_curies(db_filename, 1000, pool)
        start = time.time()
        mapped = map_curies_to_preferred_curies(db_filename, curies, pool)
        end = time.time()
    assert isinstance(mapped, tuple)
    assert len(mapped) <= len(curies)  # some mappings may fail
    print(f"Mapped {len(mapped)} of {len(curies)} curies in {end - start:.2f}s")
