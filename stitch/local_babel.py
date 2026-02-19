"""
Functions for querying the "stitch" Sqlite3 database of
[Babel](https://github.com/TranslatorSRI/Babel)
concept normalizations and conflations

Features of this module:
- Splitting tuples into fixed-size batches and mapping processors over them,
  optionally via a `multiprocessing.Pool` for parallel execution.
- Opening the SQLite database safely in **read-only** mode so multiple Python
  processes can read concurrently.
- Mapping input CURIEs to related CURIEs via conflation clusters and cliques,
  fetching preferred identifiers, synonyms, and per-table row counts.
- Small typed containers (TypedDicts) that describe identifier and clique
  metadata for structured return values.

Typical uses:
    * Use `map_curies_to_preferred_curies` or
      `map_curies_to_conflation_curies` to resolve many CURIEs efficiently,
      optionally in parallel via a provided Pool.
    * Use `map_any_curie_to_cliques` or `map_preferred_curie_to_cliques`
      to retrieve clique metadata for a single CURIE.
    * Use `get_n_random_curies` for test data generation and
      `get_table_row_counts` for quick sanity checks.

Notes
-----
- All read operations are designed to be friendly to multiprocessing:
  `connect_to_db_read_only` opens the DB as `"file:<path>?mode=ro"` with
  `uri=True`, enabling multiple concurrent readers.
- Batch sizing in `_map_with_batching` is capped by
  `MAX_IDENTIFIERS_PER_STRING_SQLITE` to respect SQLite's practical limits
  for `IN` clause sizes and parameter binding.

"""
import functools
import itertools
import multiprocessing
import multiprocessing.pool
import random
import sqlite3
from typing import Callable, Iterable, Optional, TypeAlias, TypedDict, TypeVar

import stitch.stitchutils as su

# define the type aliases that we need for brevity
MultProcPool: TypeAlias = multiprocessing.pool.Pool
CurieCurieAndType: TypeAlias = tuple[str, str, str]
CurieCurieAndIntInt: TypeAlias = tuple[str, str, int, int]
CurieInt: TypeAlias = tuple[str, int]

T = TypeVar("T")
R = TypeVar("R")

class IdentifierInfo(TypedDict):
    """Structured information about a primary identifier.

    Keys
    ----
    description : str
        A human-readable description (may be None/empty in upstream data).
    identifier : str
        The CURIE or canonical identifier string.
    label : str
        A preferred name/label for display.
    """
    description: str  # noqa
    identifier: str  # noqa
    label: str  # noqa


class CliqueInfo(TypedDict):
    """Metadata describing a clique and its associated identifier.

    Keys
    ----
    ic : float
        Information content (IC) value associated with the clique.
    id : IdentifierInfo
        The primary identifier payload (identifier, description, label).
    type : list[str]
        One or more type CURIEs (e.g., Biolink types) for the clique.
    """
    ic: float  # noqa
    id: IdentifierInfo  # noqa
    type: list[str]  # noqa



MAX_IDENTIFIERS_PER_STRING_SQLITE = 1000

# below I define the function's type hint using parametric
# polymorphism; T will have be whatever type the user passes
# to the function



def _batch_tuple(x: tuple[T, ...], batch_size: int) -> tuple[tuple[T, ...], ...]:
    """Split a tuple into fixed-size batches (last batch may be smaller).

    Parameters
    ----------
    x : tuple[T, ...]
        The input tuple to be batched.
    batch_size : int
        Maximum number of elements per batch (must be >= 1).

    Returns
    -------
    tuple[tuple[T, ...], ...]
        A tuple of batches, each batch being a tuple of elements from `x`.

    Raises
    ------
    ValueError
        If `batch_size` is less than 1.
    """
    it = iter(x)
    return tuple(
        tuple(itertools.islice(it, batch_size))
        for _ in range((len(x) + batch_size - 1) // batch_size)
    )

def _map_with_batching(items: tuple[T, ...],
                       processor: Callable[[tuple[T, ...]], Iterable[R]],
                       pool: Optional[MultProcPool],
                       max_batch_size: int = MAX_IDENTIFIERS_PER_STRING_SQLITE) \
                       -> tuple[R, ...]:
    """Map a batch-processor over items, optionally in parallel, then flatten.

    Chooses a batch size based on the number of workers (if a pool is provided)
    and caps it by `max_batch_size`. Each batch is passed as a tuple to
    `processor`, and the iterables returned by `processor` are flattened into
    a single tuple of results.

    Parameters
    ----------
    items : tuple[T, ...]
        The full input to be split into batches.
    processor : Callable[[tuple[T, ...]], Iterable[R]]
        A function that consumes a batch and yields/returns results.
    pool : Optional[multiprocessing.pool.Pool]
        If provided, `pool.imap(processor, batches)` is used; otherwise `map`.
    max_batch_size : int, default MAX_IDENTIFIERS_PER_STRING_SQLITE
        Upper bound on batch size to keep queries within SQLite limits.

    Returns
    -------
    tuple[R, ...]
        All results from all batches, in order.

    Notes
    -----
    Access to `pool._processes` is intentionally used to estimate worker count
    and is silenced for pylint/mypy on that line.
    """
    num_workers = pool._processes if pool else 1  # type: ignore[attr-defined] # pylint: disable=protected-access
    batch_size = min(max(1, len(items) // num_workers), max_batch_size)
    batches = _batch_tuple(items, batch_size)
    mapper = pool.imap if pool else map
    return tuple(itertools.chain.from_iterable(mapper(processor, batches)))

def connect_to_db_read_only(db_filename: str) -> sqlite3.Connection:
    """Open a SQLite database **read-only** with URI syntax and enable FKs.

    Parameters
    ----------
    db_filename : str
        Path to the SQLite database file.

    Returns
    -------
    sqlite3.Connection
        A connection opened as `file:<path>?mode=ro` (URI mode), with foreign
        keys enabled.

    Notes
    -----
    Opening read-only is compatible with multiple processes reading
    concurrently, which is useful for multiprocessing workloads.
    """
    # opening with "?mode=ro" is compatible with multiple python processes
    # reading the database at the same time, which we need for multiprocessing;
    # this is why we are not opening the database file with "?immutable=1"
    conn = sqlite3.connect("file:" + db_filename + "?mode=ro",
                           uri=True)
    conn.execute('PRAGMA foreign_keys = ON;')
    return conn

def _map_curies_to_conflation_curies(db_filename: str,
                                    curie_batch: Iterable[str]) -> \
                                    tuple[CurieCurieAndIntInt, ...]:
    """Resolve a batch of CURIEs to their conflation-cluster neighbors.

    For each input CURIE, returns CURIEs in the same conflation cluster(s),
    along with each member's is_canonical and the cluster's type id.

    Returns tuples: (query_curie, member_curie, member_is_canonical, conflation_type_id)
    """
    s = """
SELECT ? AS query_curie,
       id1.curie AS member_curie,
       cm1.is_canonical AS member_is_canonical,
       cc.type AS conflation_type_id
FROM conflation_clusters AS cc
JOIN conflation_members AS cm1
  ON cm1.cluster_id = cc.id
JOIN identifiers AS id1
  ON id1.id = cm1.identifier_id
WHERE cm1.cluster_id IN (
    SELECT cm2.cluster_id
    FROM conflation_members AS cm2
    JOIN identifiers AS id2
      ON id2.id = cm2.identifier_id
    WHERE id2.curie = ?
);
    """  # noqa W291

    with connect_to_db_read_only(db_filename) as db_conn:
        cursor = db_conn.cursor()
        res = tuple(
            (row[0], row[1], row[2], row[3])
            for curie in curie_batch
            for row in cursor.execute(s, (curie, curie)).fetchall()
        )
    return res

def map_curies_to_conflation_curies(db_filename: str,
                                    curies: tuple[str, ...],
                                    pool: Optional[MultProcPool] = None) -> \
                                    tuple[CurieCurieAndIntInt, ...]:
    """Resolve many CURIEs to conflation neighbors, optionally in parallel.

    Parameters
    ----------
    db_filename : str
        Path to the SQLite database.
    curies : tuple[str, ...]
        CURIEs to resolve.
    pool : Optional[multiprocessing.pool.Pool], default None
        If provided, batches are processed with `pool.imap`.

    Returns
    -------
    tuple[CurieCurieAndIntInt, ...]
        Triples of `(query_curie, neighbor_curie, conflation_type_id)`.
    """
    processor = functools.partial(_map_curies_to_conflation_curies,
                                  db_filename)
    return _map_with_batching(curies, processor, pool)


def map_curie_to_conflation_curies(
        conn: sqlite3.Connection,
        curie: str,
        conflation_type: Optional[str] = None
) -> tuple[CurieInt, ...]:
    """Return CURIEs conflated with `curie` filtered by `conflation_type`.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open database connection.
    curie : str
        The reference CURIE.
    conflation_type : Optional[str]
        Conflation type to select for (`DrugChemical` or `GeneProtein`)

    Returns
    -------
    tuple[CurieInt, ...]
        Neighbor CURIEs within conflation clusters of the given type.
    """
    query_data: tuple[str]|tuple[int, str]
    if conflation_type is not None:
        if conflation_type not in su.CONFLATION_TYPE_NAMES_IDS:
            raise ValueError(f"invalid conflation type name: {conflation_type}")
        conflation_type_int = su.CONFLATION_TYPE_NAMES_IDS[conflation_type]
        conflation_type_query_str = "cc.type = ? AND "
        query_data = (conflation_type_int, curie)
    else:
        conflation_type_query_str = ""
        query_data = (curie, )
    s = f"""
    SELECT id1.curie, cm1.is_canonical
FROM conflation_clusters AS cc
JOIN conflation_members AS cm1
  ON cm1.cluster_id = cc.id
JOIN identifiers AS id1
  ON id1.id = cm1.identifier_id
WHERE {conflation_type_query_str}
    cm1.cluster_id IN (
      SELECT cm2.cluster_id
      FROM conflation_members AS cm2
      JOIN identifiers AS id2
        ON id2.id = cm2.identifier_id
      WHERE id2.curie = ?
  );
    """ # noqa W291
    res = conn.cursor().execute(s, query_data).fetchall()
    return tuple((row[0], row[1]) for row in res)

def _map_curies_to_preferred_curies(db_filename: str,
                                    curie_batch: Iterable[str]) -> \
                                    tuple[CurieCurieAndType, ...]:
    """Resolve batch CURIEs to their preferred CURIEs and types.

    Joins through `cliques` to return the primary (preferred) identifier,
    the type CURIE, and the specific input identifier.

    Parameters
    ----------
    db_filename : str
        Path to the SQLite database.
    curie_batch : Iterable[str]
        Batch of CURIEs to resolve.

    Returns
    -------
    tuple[CurieCurieAndType, ...]
        Triples `(preferred_curie, type_curie, input_curie)`.
    """
    s = """
SELECT prim_identif.curie, types.curie, identifiers.curie
FROM identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id
INNER JOIN types on types.id = cliques.type_id
WHERE identifiers.curie = ?;"""  # noqa W291
    with connect_to_db_read_only(db_filename) as db_conn:
        cursor = db_conn.cursor()
        res = tuple((row[0], row[1], row[2])
                    for curie in curie_batch
                    for row in cursor.execute(s, (curie,)).fetchall())
    return res

def map_curie_to_preferred_curies(db_conn: sqlite3.Connection,
                                  curie: str) -> \
                                  tuple[CurieCurieAndType, ...]:
    """Resolve a single CURIE to its preferred CURIE(s) and type(s).

    Parameters
    ----------
    db_conn : sqlite3.Connection
        Open database connection.
    curie : str
        The identifier to resolve.

    Returns
    -------
    tuple[CurieCurieAndType, ...]
        Triples `(preferred_curie, type_curie, input_curie)`.
    """
    s = """
SELECT prim_identif.curie, types.curie, identifiers.curie
FROM identifiers as prim_identif
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id
INNER JOIN identifiers on idcl.identifier_id = identifiers.id
INNER JOIN types on types.id = cliques.type_id
WHERE identifiers.curie = ?;"""  # noqa W291
    cursor = db_conn.cursor()
    res = tuple((row[0], row[1], row[2])
                for row in cursor.execute(s, (curie,)).fetchall())
    return res

def map_curies_to_preferred_curies(db_filename: str,
                                   curies: tuple[str, ...],
                                   pool: Optional[MultProcPool] = None) -> \
                                   tuple[CurieCurieAndType, ...]:
    """Resolve many CURIEs to preferred CURIEs/types, optionally in parallel.

    Parameters
    ----------
    db_filename : str
        Path to the SQLite database.
    curies : tuple[str, ...]
        CURIEs to resolve.
    pool : Optional[multiprocessing.pool.Pool], default None
        If provided, batches are processed with `pool.imap`.

    Returns
    -------
    tuple[CurieCurieAndType, ...]
        Triples `(preferred_curie, type_curie, input_curie)`.
    """
    processor = functools.partial(_map_curies_to_preferred_curies,
                                  db_filename)
    return _map_with_batching(curies, processor, pool)


def map_preferred_curie_to_cliques(conn: sqlite3.Connection,
                                   curie: str) -> tuple[CliqueInfo, ...]:
    """Return clique metadata for a preferred CURIE.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open database connection.
    curie : str
        Preferred identifier CURIE.

    Returns
    -------
    tuple[CliqueInfo, ...]
        One `CliqueInfo` record per clique row for the preferred CURIE.
    """
    s = """
SELECT prim_identif.curie, types.curie, cliques.ic, descrip.desc,
cliques.preferred_name
FROM identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id 
INNER JOIN types on cliques.type_id = types.id
LEFT JOIN identifiers_descriptions as idd ON idd.identifier_id = prim_identif.id
LEFT JOIN descriptions AS descrip ON descrip.id = idd.description_id
WHERE prim_identif.curie = ?;"""  # noqa W291
    rows = conn.execute(s, (curie,)).fetchall()
    return tuple({'id': {'identifier': row[0],
                         'description': row[3],
                         'label': row[4]},
                  'ic': row[2],
                  'type': [row[1]]} for row in rows)


def map_any_curie_to_cliques(conn: sqlite3.Connection,
                             curie: str) -> tuple[CliqueInfo, ...]:
    """Return clique metadata for any CURIE (preferred or secondary).

    Parameters
    ----------
    conn : sqlite3.Connection
        Open database connection.
    curie : str
        Any identifier CURIE found in a clique.

    Returns
    -------
    tuple[CliqueInfo, ...]
        One `CliqueInfo` record per clique row associated with `curie`.
    """
    s = """
SELECT prim_identif.curie, types.curie, cliques.ic, descrip.desc, cliques.preferred_name
FROM identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id 
INNER JOIN identifiers as sec_identif on idcl.identifier_id = sec_identif.id
INNER JOIN types on cliques.type_id = types.id
LEFT JOIN identifiers_descriptions as idd ON idd.identifier_id = prim_identif.id
LEFT JOIN descriptions AS descrip ON descrip.id = idd.description_id
WHERE sec_identif.curie = ?;"""  # noqa W291
    rows = conn.execute(s, (curie,)).fetchall()
    return tuple({'id': {'identifier': row[0],
                         'description': row[3],
                         'label': row[4]},
                  'ic': row[2],
                  'type': [row[1]]} for row in rows)


def map_pref_curie_to_synonyms(cursor: sqlite3.Cursor,
                               pref_curie: str) -> set[str]:
    """Return all synonym CURIEs within the clique of a preferred CURIE.

    Parameters
    ----------
    cursor : sqlite3.Cursor
        Database cursor bound to the target connection.
    pref_curie : str
        Preferred identifier CURIE.

    Returns
    -------
    set[str]
        All CURIEs belonging to the same clique as `pref_curie`.
    """
    s = """SELECT identifiers.curie
FROM identifiers as prim_identif 
INNER JOIN cliques ON prim_identif.id = cliques.primary_identifier_id 
INNER JOIN identifiers_cliques AS idcl ON cliques.id = idcl.clique_id 
INNER JOIN identifiers on idcl.identifier_id = identifiers.id
WHERE prim_identif.curie = ?;"""  # noqa W291
    rows = cursor.execute(s, (pref_curie,)).fetchall()
    return set(c[0] for c in rows)

def _get_identifier_curies_by_int_id(db_filename: str,
                                     id_batch: Iterable[int]) -> tuple[str, ...]:
    """Fetch identifier CURIEs by internal integer `identifiers.id` values.

    Parameters
    ----------
    db_filename : str
        Path to the SQLite database.
    id_batch : Iterable[int]
        Internal integer IDs to resolve.

    Returns
    -------
    tuple[str, ...]
        Matching identifier CURIEs in the order returned by the query.
    """
    with connect_to_db_read_only(db_filename) as conn:
        id_batch_t = tuple(id_batch)
        cursor = conn.cursor()
        placeholders = ",".join("?" for _ in id_batch_t)
        query = f"SELECT curie FROM identifiers WHERE id IN ({placeholders})"
        cursor.execute(query, id_batch_t)
        result: tuple[str] = tuple(row[0] for row in cursor.fetchall())
        return result

# This function is intended to be used for testing; it will grab CURIEs from `n`
# rows of the `identifiers` table selected at random.
def get_n_random_curies(db_filename: str,
                        n: int,
                        pool: Optional[MultProcPool]) -> tuple[str, ...]:
    """Return `n` random CURIEs from the `identifiers` table.

    Intended primarily for testing and sampling workloads. Random rows are
    chosen by sampling integer primary keys uniformly from `[1, MAX(id)]`,
    then looking up their CURIEs in batched queries.

    Parameters
    ----------
    db_filename : str
        Path to the SQLite database.
    n : int
        Number of random CURIEs to return.
    pool : Optional[multiprocessing.pool.Pool]
        If provided, batch lookups are parallelized with `pool.imap`.

    Returns
    -------
    tuple[str, ...]
        A tuple of `n` randomly selected CURIE strings.

    Raises
    ------
    ValueError
        If the database appears empty (no rows found in `identifiers`).
    """
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


def get_table_row_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """Return a mapping from table name to row count for all user tables.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open database connection.

    Returns
    -------
    dict[str, int]
        Keys are table names (excluding SQLite internal tables), values are
        their `COUNT(*)` results.
    """
    cursor = conn.cursor()

    # Get the list of user-defined tables (excluding internal SQLite tables)
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%';
    """)
    tables = [row[0] for row in cursor.fetchall()]

    row_counts: dict[str, int] = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        count = cursor.fetchone()[0]
        row_counts[table] = count

    return row_counts

def get_taxon_for_gene_or_protein(conn: sqlite3.Connection,
                                  curie: str) -> Optional[str]:
    """Return the taxon CURIE associated with a gene/protein identifier.

    Parameters
    ----------
    conn : sqlite3.Connection
        Open database connection.
    curie : str
        CURIE for a gene or protein.

    Returns
    -------
    Optional[str]
        The associated taxon CURIE if found; otherwise `None`.
    """
    row = conn.cursor().execute("""
    SELECT id2.curie FROM identifiers AS id1
    INNER JOIN identifiers_taxa ON identifiers_taxa.identifier_id = id1.id
    INNER JOIN identifiers AS id2 ON identifiers_taxa.taxa_identifier_id = id2.id
    WHERE id1.curie = ?;
    """, (curie, )).fetchone()
    return row[0] if row else None
