#!/usr/bin/env python3

# this requires python3.12

import functools
import itertools
import math
import multiprocessing
import multiprocessing.pool
import operator
import random
import sqlite3
from typing import Optional, TypeAlias, TypedDict

# define the type aliases that we need for brevity
MultProcPool: TypeAlias = multiprocessing.pool.Pool
CurieAndType: TypeAlias = tuple[str, str]


class IdentifierInfo(TypedDict):
    description: str
    identifier: str
    label: str


class CliqueInfo(TypedDict):
    ic: float
    id: IdentifierInfo
    type: list[str]



MAX_IDENTIFIERS_PER_STRING_SQLITE = 1000

# below I define the function's type hint using parametric
# polymorphism; T will have be whatever type the user passes
# to the function

def _chunk_tuple[T](x: tuple[T, ...], chunk_size: int) -> tuple[tuple[T, ...], ...]:
    it = iter(x)
    return tuple(
        tuple(itertools.islice(it, chunk_size))
        for _ in range((len(x) + chunk_size - 1) // chunk_size)
    )


def connect_to_db_read_only(db_filename: str) -> sqlite3.Connection:
    return sqlite3.connect("file:" + db_filename + "?mode=ro",
                           uri=True)


def _map_curies_to_preferred_curies(db_filename: str,
                                    curie_chunk: tuple[str, ...]) -> \
                                    tuple[CurieAndType, ...]:
    s = """SELECT prim_identif.curie, types.curie
FROM (((identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id) 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id) 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id)
INNER JOIN types on types.id = cliques.type_id
WHERE identifiers.curie = ?;"""  # noqa W291   
    with connect_to_db_read_only(db_filename) as db_conn:
        cursor = db_conn.cursor()
        res = tuple((row[0], row[1])
                    for curie in curie_chunk
                    for row in cursor.execute(s, (curie,)).fetchall())
    return res


def map_curies_to_preferred_curies(db_filename: str,
                                   curies: tuple[str, ...],
                                   pool: Optional[MultProcPool] = None) -> \
                                   tuple[CurieAndType, ...]:
    processor = functools.partial(_map_curies_to_preferred_curies,
                                  db_filename)
    # the comment "type: ignore[attr-defined]" below is needed in order to
    # quiet an otherwise unavoidable mypy error:
    num_workers = pool._processes if pool else 1   # type: ignore[attr-defined]
    print(f"num workers: {num_workers}")
    chunk_size = min(max(1, math.floor(len(curies) / num_workers)),
                     MAX_IDENTIFIERS_PER_STRING_SQLITE)
    print(f"chunk size: {chunk_size}")
    mapper = pool.imap if pool else map
    chunks = _chunk_tuple(curies, chunk_size)
    return tuple(itertools.chain.from_iterable(mapper(processor, chunks)))


def map_preferred_curie_to_cliques(conn: sqlite3.Connection,
                                   curie: str) -> tuple[CliqueInfo, ...]:
    s = """SELECT prim_identif.curie, types.curie, cliques.ic, desc.desc,
cliques.preferred_name
FROM (((identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id) 
INNER JOIN types on cliques.type_id = types.id)
LEFT JOIN identifiers_descriptions as idd ON idd.identifier_id = prim_identif.id)
INNER JOIN descriptions AS desc ON desc.id = idd.description_id
WHERE prim_identif.curie = ?;"""  # noqa W291
    rows = conn.execute(s, (curie,)).fetchall()
    return tuple({'id': {'identifier': row[0],
                         'description': row[3],
                         'label': row[4]},
                  'ic': row[2],
                  'type': [row[1]]} for row in rows)


def map_any_curie_to_cliques(conn: sqlite3.Connection,
                             curie: str) -> tuple[CliqueInfo, ...]:
    s = """SELECT prim_identif.curie, types.curie, cliques.ic, desc.desc,
cliques.preferred_name
FROM (((((identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id) 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id) 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id)
INNER JOIN types on cliques.type_id = types.id)
LEFT JOIN identifiers_descriptions as idd ON idd.identifier_id = prim_identif.id)
INNER JOIN descriptions AS desc ON desc.id = idd.description_id
WHERE identifiers.curie = ?;"""  # noqa W291
    rows = conn.execute(s, (curie,)).fetchall()
    return tuple({'id': {'identifier': row[0],
                         'description': row[3],
                         'label': row[4]},
                  'ic': row[2],
                  'type': [row[1]]} for row in rows)


def map_pref_curie_to_synonyms(cursor: sqlite3.Cursor,
                               pref_curie: str) -> set[str]:
    s = """SELECT identifiers.curie
FROM ((identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id) 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id) 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id
WHERE prim_identif.curie = ?;"""  # noqa W291
    rows = cursor.execute(s, (pref_curie,)).fetchall()
    return set(c[0] for c in rows)


def _get_identifier_curies_by_int_id(db_filename: str,
                                     id_batch: list[int]) -> list[str]:
    conn = connect_to_db_read_only(db_filename)
    cursor = conn.cursor()
    placeholders = ",".join("?" for _ in id_batch)
    query = f"SELECT curie FROM identifiers WHERE id IN ({placeholders})"
    cursor.execute(query, id_batch)
    result: list[str] = list(map(operator.itemgetter(0), cursor.fetchall()))
    conn.close()
    return result


# This function is intended to be used for testing; it will grab CURIEs from `n`
# rows of the `identifiers` table selected at random.
def get_n_random_curies(db_filename: str,
                        n: int,
                        pool: Optional[MultProcPool]) -> tuple[str, ...]:
    with connect_to_db_read_only(db_filename) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id) FROM identifiers")
        row = cursor.fetchone()
        max_id = row[0] if row else None

    if max_id is None:
        raise ValueError("no rows returned")

    random_ids = random.sample(range(1, max_id + 1), n)
    batch_size = MAX_IDENTIFIERS_PER_STRING_SQLITE
    batches = tuple(
        random_ids[i:i+batch_size]
        for i in range(0, len(random_ids), batch_size)
    )

    mapper = pool.imap if pool else map
    chainer = itertools.chain.from_iterable
    processor = functools.partial(_get_identifier_curies_by_int_id, db_filename)
    return tuple(chainer(mapper(processor, batches)))


