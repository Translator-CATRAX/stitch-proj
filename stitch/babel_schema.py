"""
babel_schema.py — DDL constants for the Babel SQLite database.

Centralizes the `CREATE TABLE` statements and index work-plan used by
`ingest_babel.py` so that other modules (e.g. `local_babel.py`) can inspect
the same schema definitions without depending on the ingestion script.
"""
from stitch import stitchutils as su

ALLOWED_CONFLATION_TYPES = set(su.CONFLATION_TYPE_NAMES_IDS.values())

SQL_CREATE_TABLE_TYPES = \
    '''
        CREATE TABLE types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        curie TEXT NOT NULL UNIQUE);
    '''

SQL_CREATE_TABLE_IDENTIFIERS = \
    '''
        CREATE TABLE identifiers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        curie TEXT NOT NULL UNIQUE,
        label TEXT);
    '''

SQL_CREATE_TABLE_CLIQUES = \
    '''
        CREATE TABLE cliques (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        primary_identifier_id INTEGER NOT NULL,
        ic REAL,
        type_id INTEGER NOT NULL,
        preferred_name TEXT NOT NULL,
        FOREIGN KEY(primary_identifier_id) REFERENCES identifiers(id),
        FOREIGN KEY(type_id) REFERENCES types(id),
        UNIQUE(primary_identifier_id, type_id));
    '''

SQL_CREATE_TABLE_DESCRIPTIONS = \
    '''
        CREATE TABLE descriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        desc TEXT NOT NULL);
    '''

SQL_CREATE_TABLE_IDENTIF_DESCRIP = \
    '''
        CREATE TABLE identifiers_descriptions (
        description_id INTEGER NOT NULL,
        identifier_id INTEGER NOT NULL,
        FOREIGN KEY(description_id) REFERENCES descriptions(id),
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id));
    '''

SQL_CREATE_TABLE_IDENTIFIERS_CLIQUES = \
    '''
        CREATE TABLE identifiers_cliques (
        identifier_id INTEGER NOT NULL,
        clique_id INTEGER NOT NULL,
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        FOREIGN KEY(clique_id) REFERENCES cliques(id));
    '''

SQL_CREATE_TABLE_IDENTIFIERS_TAXA = \
    '''
        CREATE TABLE identifiers_taxa (
        identifier_id INTEGER NOT NULL,
        taxa_identifier_id INTEGER NOT NULL,
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        FOREIGN KEY(taxa_identifier_id) REFERENCES identifiers(id),
        UNIQUE(identifier_id, taxa_identifier_id));
    '''

SQL_CREATE_TABLE_CONFLATION_CLUSTERS = \
    f'''
        CREATE TABLE conflation_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type INTEGER NOT NULL CHECK (type in
        ({su.merge_ints_to_str(ALLOWED_CONFLATION_TYPES, ', ')})));
    '''

SQL_CREATE_TABLE_CONFLATION_MEMBERS = \
    '''
        CREATE TABLE conflation_members (
        cluster_id INTEGER NOT NULL,
        identifier_id INTEGER NOT NULL,
        is_canonical INTEGER NOT NULL CHECK (is_canonical in (0, 1)),
        FOREIGN KEY(cluster_id) REFERENCES conflation_clusters(id),
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        UNIQUE(cluster_id, identifier_id))
    '''

# Index creation work plan. Each entry is (table, column, phase):
#   phase 1: create the index just after CREATE TABLE, before any bulk
#            inserts. Use this for indexes that are needed during the
#            ingest itself -- either to support a SELECT/JOIN lookup
#            that runs during ingest, or to enforce a uniqueness
#            constraint as INSERTs happen.
#   phase 2: defer creation until after all bulk inserts are done.
#            This avoids the per-row index-maintenance cost during
#            ingest, at the cost of one bulk index build at the end.
#            Use this whenever an index is not needed by any
#            ingest-time query plan or constraint check, but is needed
#            after the ingest (e.g., for `local_babel.py` queries).
# As of this writing every entry below is phase 2, because no SELECT
# in `ingest_babel.py` uses any of these indexes (those queries lookup
# `identifiers.curie` and `types.curie`, both of which are covered by
# the implicit UNIQUE indexes from CREATE TABLE). The special partial
# UNIQUE index `one_canonical_per_cluster` on `conflation_members` is
# created in phase 1 from within `_create_indices` in `ingest_babel.py`
# because it enforces an INSERT-time constraint and must exist during
# the conflation ingest.
SQL__CREATE_INDEX_WORK_PLAN = \
    (('cliques',                  'type_id',               2),
     ('cliques',                  'primary_identifier_id', 2),
     ('descriptions',             'desc',                  2),
     ('identifiers_descriptions', 'description_id',        2),
     ('identifiers_descriptions', 'identifier_id',         2),
     ('identifiers_cliques',      'identifier_id',         2),
     ('identifiers_cliques',      'clique_id',             2),
     ('identifiers_taxa',         'taxa_identifier_id',    2),
     ('conflation_members',       'identifier_id',         2),
     ('conflation_clusters',      'type',                  2))
