#!/usr/bin/env python3

# Stephen A. Ramsey
# Oregon State University
# February 2025

# Empirical testing indicates that this script should be able to complete
# an ingest of Babel about 50 hours.

# # How to run the Babel ingest
# - `ssh ubuntu@stitch.rtx.ai`
# - `cd stitch`
# - `screen`
# - `source venv/bin/activate`
# - `python3.12 -u ingest_babel.py > ingest_babel.log 2>&1`
# - `ctrl-X D` (to exit the screen session)
# - `tail -f ingest_babel.log` (so you can watch the progress)
# - In another terminal session, watch memory usage using `top`

# Thank you to Gaurav Vaidya for helpful information about Babel.

import argparse
import ast
import json
import logging
import math
import os
import sqlite3
import sys
import tempfile
import time
import urllib.parse
from datetime import datetime
from typing import IO, Callable, Generator, Iterable, Optional, cast

import pandas as pd
import ray
import swifter  # noqa: F401
from htmllistparse import htmllistparse

# The "noqa: F401" for "import swifter" is needed because swifter is somehow
# automagically used once you import it, but the "ruff" lint checker software
# doesn't detect that use of the "swifter" module, so it flags an F401 error.

# make it convenient to run ingest_babel.py using `python3 stitch/ingest_babel.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from stitch import stitchutils as su

DEFAULT_BABEL_RELEASE_URL =  'https://stars.renci.org/var/babel_outputs/2025mar31'
DEFAULT_BABEL_COMPENDIA_URL = urllib.parse.urljoin(DEFAULT_BABEL_RELEASE_URL,
                                                   'compendia/')
DEFAULT_BABEL_CONFLATION_URL = urllib.parse.urljoin(DEFAULT_BABEL_RELEASE_URL,
                                                    'conflation/')
DEFAULT_DATABASE_FILE_NAME = 'babel.sqlite'

DEFAULT_TEST_TYPE = None
DEFAULT_TEST_FILE = "test-tiny.jsonl"
DEFAULT_CHUNK_SIZE = 100_000
WAL_SIZE = 1000


def _get_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser(description='ingest_babel.py: '
                                         'ingest the Babel compendia '
                                         ' files into a sqlite3 database')
    arg_parser.add_argument('--babel-compendia-url',
                            type=str,
                            dest='babel_compendia_url',
                            default=DEFAULT_BABEL_COMPENDIA_URL,
                            help='the URL of the web page containing an HTML '
                            'index listing of Babel compendia files')
    arg_parser.add_argument('--babel-conflation-url',
                            type=str,
                            dest='babel_conflation_url',
                            default=DEFAULT_BABEL_CONFLATION_URL,
                            help='the URL of the web page containing an HTML '
                            'index listing of the Babel conflation files')
    arg_parser.add_argument('--database-file-name',
                            type=str,
                            dest='database_file_name',
                            default=DEFAULT_DATABASE_FILE_NAME,
                            help='the name of the output sqlite3 database '
                            'file')
    arg_parser.add_argument('--chunk-size',
                            type=int,
                            dest='chunk_size',
                            default=DEFAULT_CHUNK_SIZE,
                            help='the size of a chunk, in rows of JSON-lines')
    arg_parser.add_argument('--use-existing-db',
                            dest='use_existing_db',
                            default=False,
                            action='store_true',
                            help='do not ingest any compendia files; '
                            'just show the work plan (like \"make -n\")')
    arg_parser.add_argument('--test-type',
                            type=int,
                            dest='test_type',
                            default=DEFAULT_TEST_TYPE,
                            help='if running a test, specify the test type '
                            '(1 or 2)')
    arg_parser.add_argument('--test-file',
                            type=str,
                            dest='test_file',
                            default=DEFAULT_TEST_FILE,
                            help='the JSON-lines file to be used for testing '
                            '(test type 1 only)')
    arg_parser.add_argument('--quiet',
                            dest='quiet',
                            default=False,
                            action='store_true')
    arg_parser.add_argument('--dry-run',
                            dest='dry_run',
                            default=False,
                            action='store_true',
                            help='do not ingest any compendia files; '
                            'just show the work plan (like \"make -n\")')
    arg_parser.add_argument('--print-ddl',
                            dest='print_ddl',
                            default=False,
                            action='store_true',
                            help='print out the DDL SQL commands for '
                            'creating the database to stderr, and then exit')
    arg_parser.add_argument('--temp-dir',
                            dest='temp_dir',
                            default=None,
                            help='specify an alternate temp directory instead '
                            'of /tmp')
    arg_parser.add_argument('--no-exec',
                            dest='no_exec',
                            default=False,
                            action='store_true',
                            help='this option is not to be directly set by a '
                            'user; only script sets it internally')
    return arg_parser.parse_args()


# this function does not return microseconds
def _cur_datetime_local_no_ms() -> datetime:
    return datetime.now().astimezone().replace(microsecond=0)


def _create_index(table: str,
                  col: str,
                  conn: sqlite3.Connection,
                  log_work: bool = False,
                  print_ddl_file_obj: IO[str] | None = None):
    statement = 'CREATE INDEX ' +\
        f'idx_{table}_{col} ' +\
        f'ON {table} ({col});'
    conn.execute(statement)
    if log_work:
        print(f"creating index on column \"{col}\" in table \"{table}\"")
    if print_ddl_file_obj is not None:
        print(statement, file=print_ddl_file_obj)

def _do_index_analyze(conn: sqlite3.Connection,
                      log_work: bool):
    if log_work:
        analyze_start_time = time.time()
    conn.execute("ANALYZE;")
    if log_work:
        analyze_end_time = time.time()
        analyze_elapsed_time = \
            su.format_time_seconds_to_str(analyze_end_time -
                                          analyze_start_time)
        print(f"running ANALYZE took: {analyze_elapsed_time} "
              "(HHH:MM::SS)")

def _do_index_vacuum(conn: sqlite3.Connection,
                     log_work: bool):
    if log_work:
        vacuum_start_time = time.time()
    conn.execute("PRAGMA wal_checkpoint(FULL);")
    conn.execute("PRAGMA journal_mode = DELETE;")
    conn.execute("VACUUM;")
    conn.execute("PRAGMA journal_mode = WAL;")
    if log_work:
        vacuum_end_time = time.time()
        vacuum_elapsed_time = \
            su.format_time_seconds_to_str(vacuum_end_time -
                                          vacuum_start_time)
        print(f"running VACUUM took: {vacuum_elapsed_time} "
              "(HHH:MM::SS)")

def _set_auto_vacuum(conn: sqlite3.Connection,
                     auto_vacuum_on: bool,
                     quiet: bool = False):
    switch_str = 'FULL' if auto_vacuum_on else 'NONE'
    conn.execute(f"PRAGMA auto_vacuum={switch_str};")
    if not quiet:
        print(f"setting auto_vacuum to: {switch_str}")

def _merge_ints_to_str(t: Iterable[int], delim: str) -> str:
    return delim.join(map(str, t))

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

SQL_CREATE_TABLE_IDENTIFIERS_DESCRIPTIONS = \
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
        FOREIGN KEY(taxa_identifier_id) REFERENCES identifiers(id));
    '''


COMPENDIA_FILE_SUFFIX = '.txt'
CONFLATION_FILE_SUFFIX = '.txt'

ALLOWED_CONFLATION_TYPES = set(su.CONFLATION_TYPE_NAMES_IDS.values())

SQL_CREATE_TABLE_CONFLATION_CLUSTERS = \
    f'''
        CREATE TABLE conflation_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type INTEGER NOT NULL CHECK (type in
        ({_merge_ints_to_str(ALLOWED_CONFLATION_TYPES, ', ')})));
    '''

SQL_CREATE_TABLE_CONFLATION_MEMBERS = \
    '''
        CREATE TABLE conflation_members (
        cluster_id INTEGER NOT NULL,
        identifier_id INTEGER NOT NULL,
        FOREIGN KEY(cluster_id) REFERENCES conflation_clusters(id),
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        UNIQUE(cluster_id, identifier_id))
    '''

SQL__CREATE_INDEX_WORK_PLAN = \
    (('cliques',                  'type_id'),
     ('cliques',                  'primary_identifier_id'),
     ('identifiers_descriptions', 'description_id'),
     ('identifiers_descriptions', 'identifier_id'),
     ('identifiers_cliques',      'identifier_id'),
     ('identifiers_cliques',      'clique_id'),
     ('identifiers_taxa',         'identifier_id'),
     ('identifiers_taxa',         'taxa_identifier_id'),
     ('conflation_members',       'identifier_id'),
     ('conflation_clusters',      'type'))

def _create_empty_database(database_file_name: str,
                           log_work: bool = False,
                           print_ddl_file_obj: IO[str] | None = None) -> \
                           sqlite3.Connection:
    if os.path.exists(database_file_name):
        os.remove(database_file_name)
    conn = sqlite3.connect(database_file_name)
    _set_auto_vacuum(conn, False)
    cur = conn.cursor()
    table_creation_statements = (
        ('types',
         SQL_CREATE_TABLE_TYPES),
        ('identifiers',
         SQL_CREATE_TABLE_IDENTIFIERS),
        ('cliques',
         SQL_CREATE_TABLE_CLIQUES),
        ('descriptions',
         SQL_CREATE_TABLE_DESCRIPTIONS),
        ('identifiers_descriptions',
         SQL_CREATE_TABLE_IDENTIFIERS_DESCRIPTIONS),
        ('identifiers_cliques',
         SQL_CREATE_TABLE_IDENTIFIERS_CLIQUES),
        ('identifiers_taxa',
         SQL_CREATE_TABLE_IDENTIFIERS_TAXA),
        ('conflation_clusters',
         SQL_CREATE_TABLE_CONFLATION_CLUSTERS),
        ('conflation_members',
         SQL_CREATE_TABLE_CONFLATION_MEMBERS)
    )

    # The `ic` field is the node's "information content", which seems to be
    # assigned by a software program called "UberGraph", and which is a real
    # number between 0 and 100; 100 means that the concept is as specific as it
    # can possibly be, in the relevant ontology (so I suppose it is a leaf node
    # with no subclasses).  A lower `ic` score presumably means it has
    # subclasses; the lower the `ic` score, the more "general" the concept
    # is. So, `ic` seems to me to really be a measure of "semantic specificity"
    for table_name, statement in table_creation_statements:
        cur.execute(statement)
        if log_work:
            print(f"creating table: \"{table_name}\"")
        if print_ddl_file_obj is not None:
            print(statement, file=print_ddl_file_obj)

    return conn


def _get_database(database_file_name: str,
                  log_work: bool = False,
                  from_scratch: bool = True,
                  print_ddl_file_obj: IO[str] | None = None) -> \
                  sqlite3.Connection:
    if from_scratch:
        return _create_empty_database(database_file_name,
                                      log_work,
                                      print_ddl_file_obj)
    conn = sqlite3.connect(database_file_name)
    _set_auto_vacuum(conn, False)
    conn.execute("VACUUM;")
    return conn


def _first_label(group_df: pd.DataFrame) -> pd.Series:
    return group_df.iloc[0]


def _ingest_compendia_jsonl_chunk(chunk: pd.core.frame.DataFrame,
                                  conn: sqlite3.Connection,
                                  insrt_missing_taxa: bool = False):

    unique_biolink_categories = chunk['type'].drop_duplicates().tolist()
    if unique_biolink_categories:
        # Create a string of placeholders like "?, ?, ?..."
        placeholders = ', '.join('?' for _ in unique_biolink_categories)
        query = "SELECT curie, id FROM types " + \
            f"WHERE curie IN ({placeholders})"
        rows = conn.execute(query, unique_biolink_categories).fetchall()

        # Build dictionary from query results
        biolink_curie_to_pkid = {curie: pkid for curie, pkid in rows}

        # Check for any missing curies
        missing = set(unique_biolink_categories) - biolink_curie_to_pkid.keys()
        if missing:
            raise ValueError(f"Missing CURIEs in types table: {missing}")
    else:
        biolink_curie_to_pkid = {}

    cliques_identifiers = chunk.identifiers.tolist()

    curies_and_info = tuple((identif_struct['i'],
                             identif_struct.get('l', None),
                             ci,
                             identif_struct.get('t', None),
                             id)
                            for id, (_, row) in enumerate(chunk.iterrows())
                            for ci, identif_struct
                            in enumerate(row['identifiers']))

    # curies_df has four columns: curie, label, cis, and taxa;
    # each row corresponds to a different identifier in the chunk
    curies_df = pd.DataFrame.from_records(curies_and_info,
                                          columns=('curie',
                                                   'label',
                                                   'cis',
                                                   'taxa',
                                                   'chunk_row'))

    curies = curies_df.curie.tolist()

    # Create a temporary table
    conn.execute("CREATE TEMP TABLE temp_curies (curie TEXT PRIMARY KEY)")
    # Insert curies into the temporary table
    conn.executemany("INSERT INTO temp_curies (curie) VALUES (?)",
                     [(c,) for c in set(curies)])
    # Use a join query to fetch the results
    query = """
    SELECT identifiers.curie, identifiers.id
    FROM identifiers
    JOIN temp_curies ON identifiers.curie = temp_curies.curie
    """
    rows = conn.execute(query).fetchall()
    conn.execute("DROP TABLE IF EXISTS temp_curies;")

    curie_pkids = dict.fromkeys(curies, None)
    curie_pkids.update({curie: id_ for curie, id_ in rows})
    curies_df['pkid'] = tuple(curie_pkids[curie] for curie in curies)
    missing_series = curies_df.pkid.isna()
    missing_df = curies_df.loc[missing_series][['curie', 'label']]
    missing_df_gb = missing_df.swifter.progress_bar(False).groupby(by='curie')
    missing_df_dedup = missing_df_gb.apply(_first_label,
                                           include_groups=False)
    missing_df_t = tuple(missing_df_dedup.itertuples(index=True,
                                                     name=None))
    cursor = conn.cursor()

    ids_inserted = {curie_label[0]:
                    cursor.execute("INSERT INTO identifiers "
                                   "(curie, label) "
                                   "VALUES (?, ?) RETURNING id;",
                                   curie_label).fetchone()[0]
                    for curie_label in missing_df_t}
    mask = curies_df.pkid.isna()
    curies_df.loc[mask, 'pkid'] = curies_df.loc[mask,
                                                'curie'].map(ids_inserted)

    curies_to_pkids = dict(curies_df[['curie',
                                      'pkid']].itertuples(index=False,
                                                          name=None))

    taxa = tuple({taxon for taxon_list in curies_df.taxa.tolist()
                  for taxon in (taxon_list if taxon_list is not None else [])})

    if taxa:

        # Create a temporary table for taxa
        conn.execute("CREATE TEMP TABLE temp_taxa (curie TEXT PRIMARY KEY)")
        # Insert the taxa values into the temporary table
        conn.executemany("INSERT INTO temp_taxa (curie) VALUES (?)", ((taxon,)
                                                                      for taxon
                                                                      in taxa))

        # Use an INNER JOIN with the temporary table instead of an IN clause
        query = (
            "SELECT identifiers.curie, identifiers.id "
            "FROM identifiers "
            "INNER JOIN temp_taxa ON identifiers.curie = temp_taxa.curie"
        )

        rows = conn.execute(query).fetchall()

        # Optionally, drop the temporary table if you want to free up resources
        # immediately
        conn.execute("DROP TABLE IF EXISTS temp_taxa")

        taxa_to_pkids = dict.fromkeys(set(taxa), None)
        taxa_to_pkids.update({curie: pkid for curie, pkid in rows})

        # Build a mapping from taxon CURIE to its id

        for taxon_curie, taxon_id in taxa_to_pkids.items():
            if taxon_id is None:
                if insrt_missing_taxa:
                    id = cursor.execute('INSERT INTO identifiers '
                                        '(curie, label) '
                                        'VALUES (?, \'some taxon\')'
                                        'RETURNING id;',
                                        (taxon_curie,)).fetchone()[0]
                    taxa_to_pkids[taxon_curie] = id
                else:
                    raise ValueError("taxon missing from database: "
                                     f"{taxon_curie}")

        insert_data = tuple((curies_to_pkids[row.iloc[0]],
                             taxa_to_pkids[t])
                            for idx, row
                            in curies_df[['curie', 'taxa']].iterrows()
                            for t in row.iloc[1])

        cursor.executemany('INSERT INTO identifiers_taxa '
                           '(identifier_id, taxa_identifier_id) '
                           'VALUES (?, ?);',
                           insert_data)

    chunk['primary_curie'] = [ident_struct['i']
                              for idx, row
                              in chunk.iterrows()
                              for subidx, ident_struct
                              in enumerate(row.loc['identifiers'])
                              if subidx == 0]

    data_to_insert_cliques = tuple(
        (curies_to_pkids[clique['primary_curie']],
         su.nan_to_none(clique['ic']),
         biolink_curie_to_pkid[clique['type']],
         clique['preferred_name'])
        for _, clique in chunk.iterrows())

    clique_pkids = tuple(
        cursor.execute('INSERT INTO cliques '
                       '(primary_identifier_id, ic, type_id, preferred_name) '
                       'VALUES (?, ?, ?, ?) '
                       'RETURNING id;',
                       clique_data).fetchone()[0]
        for clique_data in data_to_insert_cliques)

    chunk['clique_pkid'] = clique_pkids

    identifiers_cliques_data = tuple(
        (row['pkid'], int(chunk.iloc[row['chunk_row']]['clique_pkid']))
        for id, row in curies_df.iterrows())

    cursor.executemany('INSERT INTO identifiers_cliques '
                       '(identifier_id, clique_id) '
                       'VALUES (?, ?);',
                       identifiers_cliques_data)

    description_ids = tuple(
        (cursor.execute('INSERT INTO descriptions '
                        '(desc) '
                        'VALUES (?) '
                        'RETURNING id;',
                        (description_str,)).fetchone()[0],
         curies_to_pkids[clique_identifier_info['i']])
        for clique_info in cliques_identifiers
        for clique_identifier_info in clique_info
        for description_str in clique_identifier_info.get('d', []))

    cursor.executemany('INSERT INTO identifiers_descriptions '
                       '(description_id, identifier_id) '
                       'VALUES (?, ?);',
                       description_ids)


def _ingest_biolink_categories(biolink_categories: set[str],
                               conn: sqlite3.Connection,
                               log_work: bool = False):
    try:
        # Faster writes, but less safe
        conn.execute("BEGIN TRANSACTION;")
        cursor = conn.cursor()
        if log_work:
            print(f"ingesting {len(biolink_categories)} Biolink categories")
            cursor.executemany("INSERT INTO types (curie) VALUES (?);",
                               tuple((i,) for i in biolink_categories))
            conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def _byte_count_chunk(chunk: pd.core.frame.DataFrame|list[str]) -> int:
    dumpable = chunk.to_dict(orient='records') \
        if type(chunk) is pd.core.frame.DataFrame \
           else chunk
    return len(json.dumps(dumpable))

ROWS_PER_ANALYZE = 50_000_000
ROWS_PER_VACUUM = 100 * ROWS_PER_ANALYZE  # left operand must be integer > 0


def _read_conflation_file_in_chunks(file_path: str, chunk_size: int) -> \
        Generator[list[list[str]], None, None]:
    chunk = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                parsed_list = ast.literal_eval(line)
                chunk.append(parsed_list)
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
        if chunk:
            yield chunk



def _make_conflation_chunk_processor(conn: sqlite3.Connection,
                                     conflation_type: int) -> Callable:
    if conflation_type not in ALLOWED_CONFLATION_TYPES:
        raise ValueError(f"invalid conflation_type value: {conflation_type};"
                         "it must be in the set: {ALLOWED_CONFLATION_TYPES}")
    def _process_conflation_chunk(chunk: Iterable[str]):
        cursor = conn.cursor()
        for line in chunk:
            curie_list = ast.literal_eval(line)
            if not curie_list:
                raise ValueError("empty curie_list")
            # make new id
            cluster_id = cursor.execute("INSERT INTO conflation_clusters (type) "
                                        "VALUES (?) RETURNING id;",
                                        (conflation_type,)).fetchone()[0]
            placeholders = ','.join(['?'] * len(curie_list))
            query = f"SELECT id from identifiers WHERE curie IN ({placeholders});"
            assert len(curie_list)==query.count('?'), "placeholder count mismatch"
            ids = conn.execute(query, curie_list).fetchall()
            insert_data = tuple((cluster_id, curie_id_tuple[0]) for \
                                curie_id_tuple in ids)
            cursor.executemany("INSERT INTO conflation_members "
                               "(cluster_id, identifier_id) "
                               "VALUES (?, ?);",
                               insert_data)
    return _process_conflation_chunk



def _ingest_conflation_file_url(url: str,
                                conn: sqlite3.Connection,
                                chunk_size: int,
                                conflation_type_id: int,
                                log_work: bool = False,
                                total_size: Optional[int] = None,
                                glbl_chnk_cnt_start: int = 0) -> int:
    process_chunk = _make_conflation_chunk_processor(conn,
                                                     conflation_type_id)
    chunk_ctr = 0
    chunks_per_analyze = math.ceil(ROWS_PER_ANALYZE / chunk_size)
    chunks_per_vacuum = math.ceil(ROWS_PER_VACUUM / chunk_size)
    for chunk in su.get_line_chunks_from_url(url, chunk_size):
        chunk_ctr += 1
        try:
            end_str = "" if log_work else "\n"
            print(f"  Loading conflation chunk {chunk_ctr}", end=end_str)
            conn.execute("BEGIN TRANSACTION;")
            if log_work and chunk_ctr == 1:
                start_time = time.time()
                chunk_start_time = start_time
            else:
                chunk_start_time = time.time()
            process_chunk(chunk)
            conn.commit()
            if log_work:
                sub_end_str = "" if total_size is not None else "\n"
                elapsed_time = (time.time() - start_time)
                elapsed_time_str = su.format_time_seconds_to_str(elapsed_time)
                chunk_elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                       chunk_start_time)
                print(f"; time spent on URL: {elapsed_time_str}; "
                      f"spent on chunk: {chunk_elapsed_time_str}",
                      end=sub_end_str)
                if total_size is not None:
                    if chunk_ctr == 1:
                        chunk_size = _byte_count_chunk(chunk)
                        num_chunks = math.ceil(total_size / chunk_size)
                    pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
                    time_to_complete = elapsed_time * \
                        (100.0 - pct_complete)/pct_complete
                    time_to_complete_str = \
                        su.format_time_seconds_to_str(time_to_complete)
                    print(f"; {pct_complete:0.2f}% complete"
                          f"; time to complete file: {time_to_complete_str}")
                else:
                    print("\n")
            if (chunk_ctr + glbl_chnk_cnt_start) % \
               chunks_per_analyze == 0:
                _do_index_analyze(conn, log_work)
                if (chunk_ctr + glbl_chnk_cnt_start) % \
                   chunks_per_vacuum == 0:
                    _do_index_vacuum(conn, log_work)
        except Exception as e:
            conn.rollback()
            raise e
    return chunk_ctr + glbl_chnk_cnt_start

def _ingest_compendia_jsonl_url(url: str,
                                conn: sqlite3.Connection,
                                chunk_size: int,
                                log_work: bool = False,
                                total_size: Optional[int] = None,
                                insrt_missing_taxa: bool = False,
                                glbl_chnk_cnt_start: int = 0) -> int:
    chunk_ctr = 0
    chunks_per_analyze = math.ceil(ROWS_PER_ANALYZE / chunk_size)
    chunks_per_vacuum = math.ceil(ROWS_PER_VACUUM / chunk_size)
    if chunks_per_vacuum % chunks_per_analyze != 0:
        raise ValueError("ROWS_PER_VACUUM / ROWS_PER_ANALYZE must be integer")
    for chunk in pd.read_json(url,
                              lines=True,
                              chunksize=chunk_size):
        chunk_ctr += 1
        try:
            end_str = "" if log_work else "\n"
            print(f"  Loading compendia chunk {chunk_ctr}", end=end_str)
            conn.execute("BEGIN TRANSACTION;")
            if log_work and chunk_ctr == 1:
                start_time = time.time()
                chunk_start_time = start_time
            else:
                chunk_start_time = time.time()
            _ingest_compendia_jsonl_chunk(chunk,
                                          conn,
                                          insrt_missing_taxa=insrt_missing_taxa)
            conn.commit()
            if log_work:
                sub_end_str = "" if total_size is not None else "\n"
                elapsed_time = (time.time() - start_time)
                elapsed_time_str = su.format_time_seconds_to_str(elapsed_time)
                chunk_elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                       chunk_start_time)
                print(f"; time spent on URL: {elapsed_time_str}; "
                      f"spent on chunk: {chunk_elapsed_time_str}",
                      end=sub_end_str)
                if total_size is not None:
                    if chunk_ctr == 1:
                        chunk_size = _byte_count_chunk(chunk)
                        num_chunks = math.ceil(total_size / chunk_size)
                    pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
                    time_to_complete = elapsed_time * \
                        (100.0 - pct_complete)/pct_complete
                    time_to_complete_str = \
                        su.format_time_seconds_to_str(time_to_complete)
                    print(f"; {pct_complete:0.2f}% complete"
                          f"; time to complete file: {time_to_complete_str}")
                else:
                    print("\n")
            if (chunk_ctr + glbl_chnk_cnt_start) % \
               chunks_per_analyze == 0:
                _do_index_analyze(conn, log_work)
                if (chunk_ctr + glbl_chnk_cnt_start) % \
                   chunks_per_vacuum == 0:
                    _do_index_vacuum(conn, log_work)
        except Exception as e:
            conn.rollback()
            raise e
    return chunk_ctr + glbl_chnk_cnt_start


def _create_indices(conn: sqlite3.Connection,
                    log_work: bool = False,
                    print_ddl_file_obj: IO[str] | None = None):
    for table, col in SQL__CREATE_INDEX_WORK_PLAN:
        _create_index(table, col, conn, log_work, print_ddl_file_obj)


TEST_2_COMPENDIA = ('OrganismTaxon.txt',
                    'ComplexMolecularMixture.txt',
                    'Polypeptide.txt',
                    'PhenotypicFeature.txt')
TEST_3_COMPENDIA = ('Drug.txt',
                    'ChemicalEntity.txt',
                    'SmallMolecule.txt.01')
TEST_3_CONFLATION = ('DrugChemical.txt',)
TAXON_FILE = 'OrganismTaxon.txt'

FILE_NAME_SUFFIX_START_NUMBERED = COMPENDIA_FILE_SUFFIX + '.00'


def _prune_compendia_files(file_list: list[htmllistparse.FileEntry]) ->\
    tuple[tuple[str, ...],
          dict[str, htmllistparse.FileEntry]]:
    use_names: list[str] = []
    map_names = {fe.name: fe for fe in file_list}
    for file_entry in file_list:
        file_name = file_entry.name
        if COMPENDIA_FILE_SUFFIX in file_name:
            if file_name.endswith(FILE_NAME_SUFFIX_START_NUMBERED):
                file_name_start_numbered_ind = \
                    file_name.find(FILE_NAME_SUFFIX_START_NUMBERED)
                file_name_prefix = \
                    file_name[0:file_name_start_numbered_ind]
                use_names.remove(file_name_prefix + COMPENDIA_FILE_SUFFIX)
            use_names.append(file_name)
        else:
            print(f"Warning: unrecognized file name {file_name}",
                  file=sys.stderr)
    return tuple(use_names), \
        {file_name: map_names[file_name] for file_name in use_names}

def _prune_conflation_files(file_list: list[htmllistparse.FileEntry]) ->\
        tuple[tuple[str, ...],
              dict[str, htmllistparse.FileEntry]]:
    pairs: tuple[tuple[str, htmllistparse.FileEntry], ...] = tuple(
        (fe.name, fe) for fe in file_list if fe.name.endswith(CONFLATION_FILE_SUFFIX)
    )
    name_tuple, entry_tuple = \
        cast(tuple[tuple[str, ...], tuple[htmllistparse.FileEntry, ...]],
             tuple(zip(*pairs)) if pairs else ((), ())
             )
    return name_tuple, dict(zip(name_tuple, entry_tuple))

def _get_compendia_files(compendia_files_index_url: str) ->\
        tuple[tuple[str, ...], dict[str, htmllistparse.FileEntry]]:
    compendia_listing: list[htmllistparse.FileEntry]
    _, compendia_listing = htmllistparse.fetch_listing(compendia_files_index_url)
    compendia_pruned_files, compendia_map_names = \
        _prune_compendia_files(compendia_listing)
    compendia_sorted_files = sorted(compendia_pruned_files,
                                    key=lambda file_name: 0 \
                                    if file_name == TAXON_FILE else 1)
    return tuple(compendia_sorted_files), compendia_map_names

def _get_conflation_files(conflation_files_index_url: str) ->\
        tuple[tuple[str, ...], dict[str, htmllistparse.FileEntry]]:
    _, conflation_listing = htmllistparse.fetch_listing(conflation_files_index_url)
    conflation_pruned_files, conflation_map_names = \
        _prune_conflation_files(conflation_listing)
    conflation_sorted_files = sorted(conflation_pruned_files)
    return tuple(conflation_sorted_files), conflation_map_names

def _set_pragmas_for_ingestion(conn: sqlite3.Connection,
                               wal_size: int):
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute(f"PRAGMA wal_autocheckpoint = {wal_size};")

def _set_pragmas_for_querying(conn: sqlite3.Connection):
    conn.execute("PRAGMA wal_checkpoint(FULL);")
    conn.execute("PRAGMA journal_mode = DELETE;")
    # This last one is unnecessary if DB is read-only:
    conn.execute("PRAGMA synchronous = FULL;")

def _cleanup_indices(conn: sqlite3.Connection):
    conn.execute("ANALYZE")
    conn.execute("VACUUM")

def _do_final_cleanup(conn: sqlite3.Connection,
                      log_work: bool,
                      glbl_chnk_cnt: int,
                      start_time_sec: float):
        if log_work:
            final_cleanup_start_time = time.time()
        _cleanup_indices(conn)
        if log_work:
            final_cleanup_elapsed_time = \
                su.format_time_seconds_to_str(time.time() -
                                              final_cleanup_start_time)
            print("running ANALYZE and VACUUM (final cleanup) took: "
                  f"{final_cleanup_elapsed_time} (HHH:MM::SS)")
            date_time_local = _cur_datetime_local_no_ms().isoformat()
            print(f"Finished database ingest at: {date_time_local}")
            print(f"Total number of chunks inserted: {glbl_chnk_cnt}")
            elapsed_time_str = \
                su.format_time_seconds_to_str(time.time() - start_time_sec)
            print(f"Elapsed time for Babel ingest: {elapsed_time_str} (HHH:MM::SS)")

def _initialize_ray():
    # initialize Ray after any changes to tmp dir location
    logging.getLogger("ray").setLevel(logging.ERROR)
    ray.init(logging_level=logging.ERROR)

def _customize_temp_dir(temp_dir: str,
                        no_exec: bool,
                        quiet: bool):
    os.environ["SQLITE_TMPDIR"] = temp_dir
    if not no_exec:
        python_exe = sys.executable
        new_args = [python_exe, "-u", sys.argv[0], *sys.argv[1:], "--no-exec"]
        os.execve(python_exe, new_args, os.environ.copy())
    os.environ["RAY_TMPDIR"] = temp_dir
    tempfile.tempdir = temp_dir
    if not quiet:
        print(f"Setting temp dir to: {temp_dir}")

def main(babel_compendia_url: str,
         babel_conflation_url: str,
         database_file_name: str,
         chunk_size: int,
         use_existing_db: bool,
         test_type: Optional[int],
         test_file: Optional[str],
         quiet: bool,
         dry_run: bool,
         print_ddl: bool,
         temp_dir: str,
         no_exec: bool):

    if temp_dir is not None:
        _customize_temp_dir(temp_dir, no_exec, quiet)

    _initialize_ray()

    print_ddl_file_obj = sys.stderr if print_ddl else None
    log_work = not quiet
    from_scratch = not use_existing_db

    compendia_sorted_files, compendia_map_names = \
        _get_compendia_files(babel_compendia_url)
    conflation_sorted_files, conflation_map_names = \
        _get_conflation_files(babel_conflation_url)
    start_time_sec = time.time()
    date_time_local = _cur_datetime_local_no_ms().isoformat()
    print(f"Starting database ingest at: {date_time_local}")
    if test_type:
        print(f"Running in test mode; test type: {test_type}")

    with _get_database(database_file_name,
                       log_work=log_work,
                       from_scratch=from_scratch,
                       print_ddl_file_obj=print_ddl_file_obj) as conn:

        _set_pragmas_for_ingestion(conn, WAL_SIZE)

        if from_scratch:
            _ingest_biolink_categories(su.get_biolink_categories(log_work),
                                       conn,
                                       log_work)
            _create_indices(conn,
                            log_work,
                            print_ddl_file_obj=print_ddl_file_obj)

        glbl_chnk_cnt = 0

        if test_type == 1:
            if test_file is None:
                raise ValueError("test_file cannot be None")
            print(f"ingesting file: {test_file}")
            glbl_chnk_cnt = \
                _ingest_compendia_jsonl_url(test_file,
                                            conn=conn,
                                            chunk_size=chunk_size,
                                            log_work=log_work,
                                            insrt_missing_taxa=True,
                                            glbl_chnk_cnt_start=glbl_chnk_cnt)
        elif test_type == 2:
            # after ingesting Biolink categories, need to ingest OrganismTaxon
            # first!
            for file_name in TEST_2_COMPENDIA:
                file_size = compendia_map_names[file_name].size
                print(f"ingesting compendia file: {file_name}")
                elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                 start_time_sec)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of compendia file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        _ingest_compendia_jsonl_url(babel_compendia_url + file_name,
                                                    conn=conn,
                                                    chunk_size=chunk_size,
                                                    log_work=log_work,
                                                    total_size=file_size,
                                                    insrt_missing_taxa=True,
                                                    glbl_chnk_cnt_start=glbl_chnk_cnt)
        elif test_type == 3:
            # after ingesting Biolink categories, need to ingest OrganismTaxon
            # first!
            for file_name in TEST_3_COMPENDIA:
                file_size = compendia_map_names[file_name].size
                print(f"ingesting compendia file: {file_name}")
                elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                 start_time_sec)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of compendia file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        _ingest_compendia_jsonl_url(babel_compendia_url + file_name,
                                                    conn=conn,
                                                    chunk_size=chunk_size,
                                                    log_work=log_work,
                                                    total_size=file_size,
                                                    insrt_missing_taxa=False,
                                                    glbl_chnk_cnt_start=glbl_chnk_cnt)
            for file_name in TEST_3_CONFLATION:
                print(f"ingesting conflation file: {file_name}")
                assert file_name.endswith(CONFLATION_FILE_SUFFIX), \
                    f"unexpected entry in conflation file index: {file_name}"
                conflation_type_name = file_name[0:(len(file_name) - \
                                                    len(CONFLATION_FILE_SUFFIX))]
                if conflation_type_name not in su.CONFLATION_TYPE_NAMES_IDS:
                    raise ValueError(f"unknown conflation filename: {file_name}")
                file_size = conflation_map_names[file_name].size
                conflation_type_id = su.CONFLATION_TYPE_NAMES_IDS[conflation_type_name]
                elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                 start_time_sec)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of conflation file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        _ingest_conflation_file_url(babel_conflation_url + file_name,
                                                    conn=conn,
                                                    chunk_size=chunk_size,
                                                    conflation_type_id=conflation_type_id,
                                                    log_work=log_work,
                                                    total_size=file_size,
                                                    glbl_chnk_cnt_start=glbl_chnk_cnt)
        else:
            assert test_type is None, f"invalid test_type: {test_type}"
            print(f"ingesting compendia files at: {babel_compendia_url}")
            for file_name in compendia_sorted_files:
                file_size = compendia_map_names[file_name].size
                elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                 start_time_sec)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of compendia file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        _ingest_compendia_jsonl_url(babel_compendia_url +
                                                    file_name,
                                                    conn=conn,
                                                    chunk_size=chunk_size,
                                                    log_work=log_work,
                                                    total_size=file_size,
                                                    insrt_missing_taxa=True,
                                                    glbl_chnk_cnt_start=glbl_chnk_cnt)
            print(f"ingesting conflation files at: {babel_conflation_url}")
            for file_name in conflation_sorted_files:
                assert file_name.endswith(CONFLATION_FILE_SUFFIX), \
                    f"unexpected entry in conflation file index: {file_name}"
                conflation_type_name = file_name[0:(len(file_name) - \
                                                    len(CONFLATION_FILE_SUFFIX))]
                if conflation_type_name not in su.CONFLATION_TYPE_NAMES_IDS:
                    raise ValueError(f"unknown conflation filename: {file_name}")
                file_size = conflation_map_names[file_name].size
                conflation_type_id = su.CONFLATION_TYPE_NAMES_IDS[conflation_type_name]
                elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                 start_time_sec)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of conflation file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        _ingest_conflation_file_url(babel_conflation_url +
                                                    file_name,
                                                    conn=conn,
                                                    chunk_size=chunk_size,
                                                    conflation_type_id=conflation_type_id,
                                                    log_work=log_work,
                                                    total_size=file_size,
                                                    glbl_chnk_cnt_start=glbl_chnk_cnt)
        _set_pragmas_for_querying(conn)
        _set_auto_vacuum(conn, True)
        _do_final_cleanup(conn,
                          log_work,
                          glbl_chnk_cnt,
                          start_time_sec)


if __name__ == "__main__":
    main(**su.namespace_to_dict(_get_args()))
