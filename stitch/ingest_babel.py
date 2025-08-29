"""
ingest_babel.py — Ingest Babel “compendia” and “conflation” files into a
normalized SQLite database, with chunked I/O, progress logging, and
performance-tuned PRAGMAs.

Author: Stephen A. Ramsey (Oregon State University)
Date: February 2025
Python: 3.12

Overview
--------
This script downloads (or reads locally) Babel release artifacts and builds a
SQLite database with the following tables (see constants below for full DDL):
- types, identifiers, cliques, descriptions
- identifiers_descriptions, identifiers_cliques, identifiers_taxa
- conflation_clusters, conflation_members
This script should take approximately 28 hours to run, on an i4i.2xlarge instance.

Compendia files are json-lines (read in chunks); conflation files
are one Python list literal per line. The UMLS compendia (`umls.txt`) has a
different key order and is handled specially.

Key Features
------------
- Creates a fresh database (default) or opens an existing one.
- Applies fast-ingest PRAGMAs (WAL, synchronous=OFF, optimize, wal_autocheckpoint)
  and restores query-mode settings on completion (wal_checkpoint(FULL),
  journal_mode=DELETE, synchronous=FULL).
- Builds indexes and runs ANALYZE periodically after row-count milestones
  (`ROWS_PER_ANALYZE`), then VACUUM/ANALYZE and integrity_check at the end.
- Chunked ingestion (`--lines-per-chunk`, default 100_000) with per-chunk
  progress logging, ETA, and byte-based % complete when file size is known.
- Optional dry-run plan and DDL printing.
- Supports test modes that restrict input files for faster smoke tests.

Command-Line Arguments
----------------------
--babel-compendia-url      URL of the compendia index page
--babel-conflation-url     URL of the conflation index page
--database-file-name       Output SQLite file name (default: babel.sqlite)
--lines-per-chunk          JSON-lines rows per chunk (default: 100_000)
--use-existing-db          Skip schema creation; operate on an existing DB
--test-type                Test scenario {1,2,3,4}; see “Test Modes” below
--test-compendia-file      Local JSONL file for test type 1
--quiet                    Suppress timestamped progress logs
--dry-run                  Print the work plan without executing
--print-ddl                Emit CREATE TABLE statements to stderr, then exit
--temp-dir                 Alternate temp dir (propagated to subprocess via re-exec)
--no-exec                  Internal flag used by the re-exec path (do not set directly)

Test Modes
----------
1: Ingest a single local compendia JSONL specified by --test-compendia-file.
2: Ingest a small, fixed subset of compendia files (smoke test).
3: Ingest a selected compendia subset then a selected conflation file; missing
   taxa insertion is disabled.
4: Ingest UMLS compendia only (different column order).

How To Run (example)
--------------------
Run on a long-lived host inside screen/tmux and stream logs to a file:

    python3.12 -u ingest_babel.py > ingest_babel.log 2>&1
    tail -f ingest_babel.log

You can monitor system memory/IO with tools like `top` in another shell.
There is also a script "instance-memory-tracker.sh", in the stitch
project area, that will record the script's memory usage (see the
main README.md in the stitch project area).

Acknowledgements
----------------
Thank you to Gaurav Vaidya for helpful information about Babel.
"""
import argparse
import ast
import functools
import json
import logging
import math
import os
import sqlite3
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import datetime
from typing import IO, Any, Iterable, Optional, cast, overload
from urllib.parse import urljoin

import numpy
import pandas as pd
import ray
from htmllistparse.htmllistparse import FileEntry, fetch_listing

from stitch import stitchutils as su

ChunkType = pd.DataFrame | list[str]

DEFAULT_BABEL_RELEASE_URL =  'https://stars.renci.org/var/babel_outputs/2025mar31/'
DEFAULT_BABEL_COMPENDIA_URL = urljoin(DEFAULT_BABEL_RELEASE_URL, 'compendia/')
DEFAULT_BABEL_CONFLATION_URL = urljoin(DEFAULT_BABEL_RELEASE_URL, 'conflation/')
DEFAULT_DATABASE_FILE_NAME = 'babel.sqlite'
DEFAULT_TEST_TYPE = None
DEFAULT_COMPENDIA_TEST_FILE = "test-tiny.jsonl"
DEFAULT_LINES_PER_CHUNK = 100_000
WAL_SIZE = 1000
UNKNOWN_TAXON = "unknown taxon"  # this is needed for certain built-in test cases

def _get_args() -> argparse.Namespace:
    arg_parser = \
        argparse.ArgumentParser(description='ingest_babel.py: '
                                'ingest Babel into a sqlite3 database')
    arg_parser.add_argument('--babel-compendia-url', type=str,
                            dest='babel_compendia_url',
                            default=DEFAULT_BABEL_COMPENDIA_URL,
                            help='the URL of the web page containing an HTML '
                            'index listing of Babel compendia files')
    arg_parser.add_argument('--babel-conflation-url', type=str,
                            dest='babel_conflation_url',
                            default=DEFAULT_BABEL_CONFLATION_URL,
                            help='the URL of the web page containing an HTML '
                            'index listing of the Babel conflation files')
    arg_parser.add_argument('--database-file-name', type=str, dest='database_file_name',
                            default=DEFAULT_DATABASE_FILE_NAME,
                            help='the name of the output sqlite3 database '
                            'file')
    arg_parser.add_argument('--lines-per-chunk', type=int, dest='lines_per_chunk',
                            default=DEFAULT_LINES_PER_CHUNK,
                            help='the size of a chunk, in rows of JSON-lines')
    arg_parser.add_argument('--use-existing-db', dest='use_existing_db',
                            default=False, action='store_true',
                            help='do not ingest any compendia files; '
                            'just show the work plan (like \"make -n\")')
    arg_parser.add_argument('--test-type', type=int, dest='test_type',
                            default=DEFAULT_TEST_TYPE,
                            help='if running a test, specify the test type '
                            '(1 or 2)')
    arg_parser.add_argument('--test-compendia-file', type=str,
                            dest='test_compendia_file',
                            default=DEFAULT_COMPENDIA_TEST_FILE,
                            help='the JSON-lines file to be used for testing '
                            '(test type 1 only)')
    arg_parser.add_argument('--quiet', dest='quiet', default=False,
                            action='store_true')
    arg_parser.add_argument('--dry-run', dest='dry_run', default=False,
                            action='store_true',
                            help='do not ingest any compendia files; '
                            'just show the work plan (like \"make -n\")')
    arg_parser.add_argument('--print-ddl', dest='print_ddl', default=False,
                            action='store_true',
                            help='print out the DDL SQL commands for '
                            'creating the database to stderr, and then exit')
    arg_parser.add_argument('--temp-dir', dest='temp_dir', default=None,
                            help='specify an alternate temp directory instead '
                            'of /tmp')
    arg_parser.add_argument('--no-exec', dest='no_exec', default=False,
                            action='store_true',
                            help='this option is not to be directly set by a '
                            'user; only script sets it internally')
    return arg_parser.parse_args()

# this function does not return microseconds
def _cur_datetime_local_no_ms() -> datetime:
    return datetime.now().astimezone().replace(microsecond=0)

def _cur_datetime_local_str() -> str:
    return _cur_datetime_local_no_ms().isoformat()

type _LogPrintImpl = Callable[[str, str], None]
type SetEnabled = Callable[[bool], None]

# Mutable implementation target (assigned below)
_log_print_impl: _LogPrintImpl

@overload
def _log_print(message: str) -> None: ...
@overload
def _log_print(message: str, end: str) -> None: ...
def _log_print(message: str, end: str = "\n") -> None:
    """Public logger function. Overloads make mypy accept 1 or 2 args."""
    _log_print_impl(message, end)

def _make_log_print_controller() -> tuple[_LogPrintImpl, SetEnabled]:
    """Create the underlying implementation and an enable/disable setter."""
    state: dict[str, bool] = {"enabled": False}
    def impl(message: str, end: str = "\n") -> None:
        if state["enabled"]:
            date_time_local = _cur_datetime_local_str()
            print(f"{date_time_local}: {message}",
                  end=end, file=sys.stderr, flush=True)
    def set_enabled(enabled: bool) -> None:
        state["enabled"] = enabled
    return impl, set_enabled

# Initialize the implementation and setter once (no globals/reassignment of the
# function).
_log_print_impl, _set_log_print_enabled = _make_log_print_controller()

def _create_index(table: str,
                  col: str,
                  conn: sqlite3.Connection,
                  print_ddl_file_obj: IO[str] | None = None):
    statement = ('CREATE INDEX '
                 f'idx_{table}_{col} '
                 f'ON {table} ({col});')
    conn.execute(statement)
    _log_print(f"creating index on column \"{col}\" in table \"{table}\"")
    if print_ddl_file_obj is not None:
        print(statement, file=print_ddl_file_obj)

def _do_index_analyze(conn: sqlite3.Connection,
                      log_work: bool):
    _log_print("starting database ANALYZE")
    if log_work:
        analyze_start_time = time.time()
    conn.execute("ANALYZE;")
    _log_print("completed database ANALYZE")
    if log_work:
        analyze_end_time = time.time()
        analyze_elapsed_time = \
            su.format_time_seconds_to_str(analyze_end_time -
                                          analyze_start_time)
        _log_print(f"running ANALYZE took: {analyze_elapsed_time} "
                   "(HHH:MM::SS)")

def _set_auto_vacuum(conn: sqlite3.Connection,
                     auto_vacuum_on: bool):
    switch_str = 'FULL' if auto_vacuum_on else 'NONE'
    _log_print(f"setting auto_vacuum to {switch_str}")
    conn.execute(f"PRAGMA auto_vacuum={switch_str};")

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
                           print_ddl_file_obj: IO[str] | None = None) -> \
                           sqlite3.Connection:
    if os.path.exists(database_file_name):
        os.remove(database_file_name)
    conn = sqlite3.connect(database_file_name)
    _set_auto_vacuum(conn, auto_vacuum_on=False)
    cur = conn.cursor()
    table_creation_statements = (
        ('types', SQL_CREATE_TABLE_TYPES),
        ('identifiers', SQL_CREATE_TABLE_IDENTIFIERS),
        ('cliques', SQL_CREATE_TABLE_CLIQUES),
        ('descriptions', SQL_CREATE_TABLE_DESCRIPTIONS),
        ('identifiers_descriptions', SQL_CREATE_TABLE_IDENTIF_DESCRIP),
        ('identifiers_cliques', SQL_CREATE_TABLE_IDENTIFIERS_CLIQUES),
        ('identifiers_taxa', SQL_CREATE_TABLE_IDENTIFIERS_TAXA),
        ('conflation_clusters', SQL_CREATE_TABLE_CONFLATION_CLUSTERS),
        ('conflation_members', SQL_CREATE_TABLE_CONFLATION_MEMBERS))
    # The `ic` field is the node's "information content", which seems to be
    # assigned by a software program called "UberGraph", and which is a real
    # number between 0 and 100; 100 means that the concept is as specific as it
    # can possibly be, in the relevant ontology (so I suppose it is a leaf node
    # with no subclasses).  A lower `ic` score presumably means it has
    # subclasses; the lower the `ic` score, the more "general" the concept
    # is. So, `ic` seems to me to really be a measure of "semantic specificity"
    for table_name, statement in table_creation_statements:
        cur.execute(statement)
        _log_print(f"creating table: \"{table_name}\"")
        if print_ddl_file_obj is not None:
            print(statement, file=print_ddl_file_obj)
    return conn

def _run_vacuum(conn: sqlite3.Connection):
    _log_print("starting database VACUUM")
    conn.execute("VACUUM;")
    _log_print("completed database VACUUM")

def _get_database(database_file_name: str,
                  from_scratch: bool = True,
                  print_ddl_file_obj: IO[str] | None = None) -> \
                  sqlite3.Connection:
    if from_scratch:
        return _create_empty_database(database_file_name,
                                      print_ddl_file_obj)
    conn = sqlite3.connect(database_file_name)
    _set_auto_vacuum(conn, False)
    _run_vacuum(conn)
    _set_pragmas_for_ingestion(conn, WAL_SIZE)
    return conn

def _first_label(group_df: pd.DataFrame) -> pd.Series:
    return group_df.iloc[0]

def _ingest_biolink_categories(conn: sqlite3.Connection,
                               log_work: bool,
                               biolink_categories: set[str],
                               biolink_ver: str):
    try:
        if log_work:
            len_cat = len(biolink_categories)
            _log_print(f"ingesting {len_cat} Biolink categories")
            _log_print(f"Using Biolink model version {biolink_ver}")
        conn.execute("BEGIN TRANSACTION;")
        conn.cursor().executemany("INSERT INTO types (curie) VALUES (?);",
                                  tuple((i,) for i in sorted(biolink_categories)))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def _byte_count_chunk(chunk: ChunkType) -> int:
    dumpable = chunk.to_dict(orient='records') \
        if isinstance(chunk, pd.core.frame.DataFrame) \
           else chunk
    return len(json.dumps(dumpable))

ROWS_PER_ANALYZE = (1_000_000, 3_000_000, 10_000_000, \
                    30_000_000, 100_000_000, 300_000_000)

def _insert_and_return_id(cursor: sqlite3.Cursor,
                          sql: str,
                          params: tuple) -> int:
    return cursor.execute(sql, params).fetchone()[0]

def _curies_to_pkids(cursor: sqlite3.Cursor,
                     curies: set[str],
                     table_name: str = "temp_curies") -> dict[str, int]:
    cursor.execute(f"CREATE TEMP TABLE {table_name} (curie TEXT PRIMARY KEY)")
    cursor.executemany(f"INSERT INTO {table_name} (curie) VALUES (?)",
                     ((curie,) for curie in curies))
    rows = cursor.execute(f"""
        SELECT identifiers.curie, identifiers.id
        FROM identifiers
        JOIN {table_name} ON identifiers.curie = {table_name}.curie
    """).fetchall()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    return dict(rows)

def _make_conflation_chunk_processor(conn: sqlite3.Connection,
                                     conflation_type_id: int) -> Callable:
    if conflation_type_id not in ALLOWED_CONFLATION_TYPES:
        raise ValueError(f"invalid conflation_type value: {conflation_type_id};"
                         "it must be in the set: {ALLOWED_CONFLATION_TYPES}")
    def process_conflation_chunk(chunk: Iterable[str]):
        cursor = conn.cursor()
        for line in chunk:
            curie_list = ast.literal_eval(line)
            if not curie_list:
                raise ValueError("empty curie_list")
            # make new id
            cluster_id = _insert_and_return_id(cursor,
                                               "INSERT INTO conflation_clusters (type) "
                                               "VALUES (?) RETURNING id;",
                                               (conflation_type_id,))
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
    return process_conflation_chunk

def _flatten_taxa(taxa_col: Iterable[Optional[list[str]]]) -> set[str]:
    return {taxon
            for taxon_list in taxa_col
            if taxon_list
            for taxon in taxon_list}

def _get_pkids_from_curies_with_missing(cursor: sqlite3.Cursor,
                                        curies: set[str]) -> \
                                        dict[str, Optional[int]]:
    curies_to_pkids = dict.fromkeys(curies, None)
    curies_to_pkids.update(_curies_to_pkids(cursor, curies))
    return curies_to_pkids

def _get_taxa_pkids_fill_in_if_necessary(cursor: sqlite3.Cursor,
                                         curies: set[str],
                                         insrt_missing_taxa: bool) -> \
                                         dict[str, Optional[int]]:
    taxa_to_pkids = _get_pkids_from_curies_with_missing(cursor, curies)
    # Build a mapping from taxon CURIE to its id
    for taxon_curie, taxon_id in taxa_to_pkids.items():
        if taxon_id is None:
            if insrt_missing_taxa:
                taxa_to_pkids[taxon_curie] = \
                    _insert_and_return_id(cursor,
                                          'INSERT INTO identifiers '
                                          '(curie, label) '
                                          f'VALUES (?, \'{UNKNOWN_TAXON}\')'
                                          'RETURNING id;',
                                          (taxon_curie,))
            else:
                raise ValueError("taxon missing from database: "
                                 f"{taxon_curie}")
    return taxa_to_pkids

def _get_biolink_type_pkids_from_curies(cursor: sqlite3.Cursor, curies: tuple[str]) \
        -> dict[str, int]:
    placeholders = ', '.join('?' for _ in curies)
    return dict((cursor.execute("SELECT curie, id FROM types "
                                "WHERE curie IN "
                                f"({placeholders})",
                                curies)
                 .fetchall()))

def _make_compendia_chunk_processor(conn: sqlite3.Connection,
                                    insrt_missing_taxa: bool = False,
                                    non_umls_compendia_file: bool = True) -> Callable:
    def process_compendia_chunk(chunk: pd.DataFrame):
        cursor = conn.cursor()
        biolink_curie_to_pkid = \
            _get_biolink_type_pkids_from_curies(cursor, tuple(chunk['type'].unique()))
        curies_and_info = []
        data_to_insert_cliques = []
        primary_curies = []
        for row_id, row in enumerate(chunk.itertuples(index=False, name=None)):
            # For a non-umls compendia file, the order of the "row" tuple is:
            #   biolink_type, ic, identifiers, preferred_name, taxa
            # For the "umls.txt" compendia file, the order of the "row" tuple is:
            #   biolink_type, ic, preferred_name, taxa, identifiers
            if not non_umls_compendia_file:
                row = tuple(row[i] for i in (0, 1, 4, 2, 3))
            primary_curies.append(cast(list[dict[str, Any]], row[2])[0]['i']
                                  if row[2] else None)
            for ci, identif_struct in enumerate(row[2]):
                curies_and_info.append((identif_struct['i'],
                                        identif_struct.get('l'),
                                        ci,
                                        identif_struct.get('t'),
                                        row_id))
            data_to_insert_cliques.append((su.nan_to_none(row[1]),
                                           biolink_curie_to_pkid[row[0]], row[3]))
        chunk['primary_curie'] = primary_curies
        # curies_df has four columns: curie, label, cis, and taxa;
        # each row corresponds to a different identifier in the chunk
        curies_df = \
            pd.DataFrame.from_records(curies_and_info,
                                      columns=('curie', 'label', 'cis', 'taxa',
                                               'chunk_row'))
        curies_df['pkid'] = \
            (curies_df['curie']
             .map(_get_pkids_from_curies_with_missing(cursor,
                                                      set(curies_df.curie))))
        mask = curies_df.pkid.isna()
        curies_df.loc[mask, 'pkid'] = \
            (curies_df.loc[mask,
                           'curie']
             .map({curie_label[0]:
                   _insert_and_return_id(cursor,
                                         "INSERT INTO identifiers "
                                         "(curie, label) VALUES (?, ?) "
                                         "RETURNING id;",
                                         curie_label)
                   for curie_label in
                   (curies_df.loc[(curies_df
                                   .pkid
                                   .isna())][['curie', 'label']]
                    .groupby(by='curie')
                    .apply(_first_label,
                           include_groups=False)
                    .itertuples(index=True,
                                name=None))}))
        curies_to_pkids = \
            dict(curies_df[['curie', 'pkid']].itertuples(index=False, name=None))
        taxa = set(_flatten_taxa(curies_df['taxa']))
        if taxa:
            taxa_to_pkids = \
                _get_taxa_pkids_fill_in_if_necessary(cursor, taxa,
                                                     insrt_missing_taxa)
            cursor.executemany('INSERT INTO identifiers_taxa '
                               '(identifier_id, taxa_identifier_id) '
                               'VALUES (?, ?);',
                               tuple((curies_to_pkids[row_curie],
                                      taxa_to_pkids[t])
                                     for row_curie, _, _, row_taxa, _, _
                                     in curies_df.itertuples(index=False,
                                                             name=None)
                                     for t in cast(list[str], row_taxa)))
        chunk['clique_pkid'] = \
            tuple(_insert_and_return_id(cursor,
                                        'INSERT INTO cliques '
                                        '(primary_identifier_id, ic, type_id, '
                                        'preferred_name) '
                                        'VALUES (?, ?, ?, ?) RETURNING id;',
                                        clique_data)
            for clique_data in tuple((pkid, *data) for pkid, data \
                                     in zip(tuple(curies_to_pkids[primary_id]
                                                  for primary_id in primary_curies),
                                            data_to_insert_cliques)))
        cursor.executemany('INSERT INTO identifiers_cliques '
                           '(identifier_id, clique_id) '
                           'VALUES (?, ?);',
                           tuple((row_pkid,
                                  int(chunk['clique_pkid'].tolist()[chunk_row]))
                                 for _, _, _, _, chunk_row, row_pkid
                                 in curies_df.itertuples(index=False)))
        cursor.executemany('INSERT INTO identifiers_descriptions '
                           '(description_id, identifier_id) '
                           'VALUES (?, ?);',
                           tuple((_insert_and_return_id(cursor,
                                                        'INSERT INTO descriptions '
                                                        '(desc) VALUES (?) '
                                                        'RETURNING id;',
                                                        (description_str,)),
                                  curies_to_pkids[clique_identifier_info['i']])
                                 for clique_info in chunk.identifiers.tolist()
                                 for clique_identifier_info in clique_info
                                 for description_str
                                 in clique_identifier_info.get('d', [])))
    return process_compendia_chunk

def _read_compendia_chunks(url: str,
                           lines_per_chunk: int) -> Iterable[pd.DataFrame]:
    return pd.read_json(url,
                        lines=True,
                        chunksize=lines_per_chunk)

def _read_conflation_chunks(url: str,
                            lines_per_chunk: int) -> Iterable[list[str]]:
    return su.get_line_chunks_from_url(url, lines_per_chunk)

def _do_log_work(start_times: tuple[float, float],
                 progress_info: tuple[int, int],
                 job_info: tuple[Optional[int], Optional[int]]):
    num_chunks, total_size = job_info
    chunk_ctr, glbl_chnk_cnt_start = progress_info
    log_str = f"Loading compendia chunk {chunk_ctr}"
    elapsed_time = time.time() - start_times[0]
    elapsed_time_str = su.format_time_seconds_to_str(elapsed_time)
    chunk_elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                           start_times[1])
    log_str += ("; total chunks processed: "
                f"{glbl_chnk_cnt_start + chunk_ctr}"
                f"; time spent on URL: {elapsed_time_str}; "
                f"spent on chunk: {chunk_elapsed_time_str}")
    if total_size is not None and num_chunks is not None:
        pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
        time_to_complete = elapsed_time * \
            (100.0 - pct_complete)/pct_complete
        time_to_complete_str = \
            su.format_time_seconds_to_str(time_to_complete)
        log_str += (f"; URL {pct_complete:0.2f}% complete"
                    f"; time to complete URL: {time_to_complete_str}")
    _log_print(log_str)

def _make_url_ingester(conn: sqlite3.Connection,
                       lines_per_chunk: int,
                       read_chunks: Callable[[str, int], Iterable[ChunkType]],
                       log_work: bool = False) -> Callable:
    chunks_per_analyze_list: list[int] = [int(x) for x in
                                          numpy.ceil(numpy.array(ROWS_PER_ANALYZE)/\
                                                     lines_per_chunk)]
    def ingest_from_url(url: str,
                        process_chunk: Callable[[ChunkType], None],
                        total_size: Optional[int] = None,
                        glbl_chnk_cnt: int = 0) -> int:
        chunk_ctr = 0
        start_time = None
        for chunk in read_chunks(url, lines_per_chunk):
            chunk_ctr += 1
            try:
                conn.execute("BEGIN TRANSACTION;")
                if log_work and chunk_ctr == 1:
                    start_time = time.time()
                process_chunk(chunk)
                conn.commit()
                if log_work:
                    if total_size is not None:
                        if chunk_ctr == 1:
                            chunk_size = _byte_count_chunk(chunk)
                            num_chunks = math.ceil(total_size / chunk_size)
                        else:
                            assert num_chunks is not None # assigned on 1st iter
                    else:
                        num_chunks = None
                    assert start_time is not None # assigned on 1st iter
                    _do_log_work((start_time,
                                  start_time if chunk_ctr == 1 else time.time()),
                                 (chunk_ctr, glbl_chnk_cnt),
                                 (num_chunks, total_size))
                if any(chunk_ctr + glbl_chnk_cnt == chunks_per_analyze \
                       for chunks_per_analyze in chunks_per_analyze_list):
                    _do_index_analyze(conn, log_work)
            except Exception as e:
                conn.rollback()
                raise e
        return chunk_ctr + glbl_chnk_cnt
    return ingest_from_url

def _create_indices(conn: sqlite3.Connection,
                    print_ddl_file_obj: IO[str] | None = None):
    for table, col in SQL__CREATE_INDEX_WORK_PLAN:
        _create_index(table, col, conn, print_ddl_file_obj)

TEST_2_COMPENDIA = ('OrganismTaxon.txt', 'ComplexMolecularMixture.txt',
                    'Polypeptide.txt', 'PhenotypicFeature.txt')
TEST_3_COMPENDIA = ('Drug.txt', 'ChemicalEntity.txt', 'SmallMolecule.txt.01')
TEST_3_CONFLATION = ('DrugChemical.txt',)
TEST_4_COMPENDIA = ('umls.txt',)
TAXON_FILE = 'OrganismTaxon.txt'
FILE_NAME_SUFFIX_START_NUMBERED = COMPENDIA_FILE_SUFFIX + '.00'

def _create_file_map(file_name: str) -> dict[str, FileEntry]:
    file_size = os.path.getsize(file_name)
    file_modif = os.path.getmtime(file_name)
    file_entry = FileEntry(file_name, file_modif, file_size, "file")
    return {file_name: file_entry}

def _prune_compendia_files(file_list: list[FileEntry]) ->\
    tuple[tuple[str, ...], dict[str, FileEntry]]:
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

def _prune_conflation_files(file_list: list[FileEntry]) ->\
        tuple[tuple[str, ...],
              dict[str, FileEntry]]:
    pairs: tuple[tuple[str, FileEntry], ...] = tuple(
        (fe.name, fe) for fe in file_list if fe.name.endswith(CONFLATION_FILE_SUFFIX))
    name_tuple, entry_tuple = \
        cast(tuple[tuple[str, ...], tuple[FileEntry, ...]],
             tuple(zip(*pairs)) if pairs else ((), ()))
    return name_tuple, dict(zip(name_tuple, entry_tuple))

def _get_compendia_files(compendia_files_index_url: str) ->\
        tuple[tuple[str, ...], dict[str, FileEntry]]:
    compendia_listing: list[FileEntry]
    _, compendia_listing = fetch_listing(compendia_files_index_url)
    compendia_pruned_files, compendia_map_names = \
        _prune_compendia_files(compendia_listing)
    # need to ingest OrganismTaxon compendia file first
    compendia_sorted_files = \
        sorted(compendia_pruned_files,
               key=lambda file_name: 0 if file_name == TAXON_FILE else 1)
    return tuple(compendia_sorted_files), compendia_map_names

def _get_conflation_files(conflation_files_index_url: str) ->\
        tuple[tuple[str, ...], dict[str, FileEntry]]:
    _, conflation_listing = fetch_listing(conflation_files_index_url)
    conflation_pruned_files, conflation_map_names = \
        _prune_conflation_files(conflation_listing)
    conflation_sorted_files = sorted(conflation_pruned_files)
    return tuple(conflation_sorted_files), conflation_map_names

def _set_pragmas_for_ingestion(conn: sqlite3.Connection,
                               wal_size: int):
    for s in ('synchronous = OFF', 'journal_mode = WAL',
              'optimize', f'wal_autocheckpoint = {wal_size}'):
        conn.execute(f"PRAGMA {s}")
        _log_print(f"setting PRAGMA {s}")

def _set_pragmas_for_querying(conn: sqlite3.Connection):
    for s in ('wal_checkpoint(FULL)', 'journal_mode = DELETE'):
        conn.execute(f"PRAGMA {s}")
        _log_print(f"setting PRAGMA {s}")
    _set_auto_vacuum(conn, auto_vacuum_on=True)

def _do_integrity_check(conn: sqlite3.Connection):
    _log_print("running PRAGMA integrity_check")
    result = conn.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        raise RuntimeError(f"Database integrity check failed: {result}")

def _cleanup_indices(conn: sqlite3.Connection,
                     log_work: bool):
    _do_index_analyze(conn, log_work)
    _log_print("setting PRAGMA locking_mode to EXCLUSIVE")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    _run_vacuum(conn)
    _do_integrity_check(conn)

def _do_final_cleanup(conn: sqlite3.Connection,
                      log_work: bool,
                      glbl_chnk_cnt: int,
                      start_time_sec: float):
    if log_work:
        final_cleanup_start_time = time.time()
    _set_pragmas_for_querying(conn)
    _cleanup_indices(conn, log_work)
    if log_work:
        final_cleanup_elapsed_time = \
            su.format_time_seconds_to_str(time.time() -
                                          final_cleanup_start_time)
        _log_print("final cleanup (VACUUM, ANALYZE, and integrity "
                   "check combined) took: "
                   f"{final_cleanup_elapsed_time} (HHH:MM::SS)")
        _log_print(f"Total number of chunks inserted: {glbl_chnk_cnt}")
        elapsed_time_str = \
            su.format_time_seconds_to_str(time.time() - start_time_sec)
        _log_print(f"Finished database ingest. "
                   f"Total elapsed time: {elapsed_time_str} (HHH:MM::SS)")

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
    # the "noqa" is to quiet a Vulture warning and not upset "ruff"
    tempfile.tempdir = temp_dir  # noqa
    if not quiet:
        _log_print(f"Setting temp dir to: {temp_dir}")

def _log_start_of_file(start: float, filetype: str, filename: str, filesize: int):
    elapsed = su.format_time_seconds_to_str(time.time() - start)
    return (f"at elapsed time: {elapsed}; "
            f"starting ingest of {filetype} "
            f"file: {filename}; "
            f"file size: {filesize} bytes")

def _get_conflation_type_id(file_to_id_map: dict[str, int],
                            conflation_file_name: str) -> int:
    assert conflation_file_name.endswith(CONFLATION_FILE_SUFFIX), \
                    f"unexpected entry in conflation file index: {conflation_file_name}"
    conflation_type_name = conflation_file_name[0:(len(conflation_file_name) - \
                                                   len(CONFLATION_FILE_SUFFIX))]
    if conflation_type_name not in file_to_id_map:
        raise ValueError(f"unknown conflation filename: {conflation_file_name}")
    return file_to_id_map[conflation_type_name]

def _get_make_chunkproc_args_conflation(file_to_id_map: dict[str, int],
                                        file_name: str) -> dict[str, Any]:
    return {'conflation_type_id': _get_conflation_type_id(file_to_id_map, file_name)}

def _get_make_chunkproc_args_compendia(insrt_missing_taxa: bool,
                                       file_name:str) -> dict[str, Any]:
    # In Babel, the umls.txt file has a different key order than the
    # other compendia files; need to handle "umls.txt" as a special case
    non_umls_compendia_file = file_name != 'umls.txt'
    return {'insrt_missing_taxa': insrt_missing_taxa,
            'non_umls_compendia_file': non_umls_compendia_file}

def _make_ingest_urls(dry_run: bool) -> Callable:
    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def ingest_urls(file_names: Iterable[str],
                    file_map: dict[str, FileEntry],
                    file_type: str,
                    base_url: str,
                    ingest_url: Callable,
                    start_time_sec: float,
                    make_chunk_processor: Callable,
                    get_make_chunk_processor_args: Callable,
                    glbl_chnk_cnt: int) -> int:
        _log_print(f"ingesting {file_type} files at: " +
                   (base_url if base_url != "" else "(local)"))
        for file_name in file_names:
            file_size = file_map[file_name].size
            elapsed_str = _log_start_of_file(start_time_sec, file_type,
                                             file_name, file_size)
            _log_print(elapsed_str)
            if not dry_run:
                make_chunk_processor_args = get_make_chunk_processor_args(file_name)
                process_chunk = make_chunk_processor(**make_chunk_processor_args)
                glbl_chnk_cnt = ingest_url(urljoin(base_url, file_name),
                                           process_chunk, total_size=file_size,
                                           glbl_chnk_cnt=glbl_chnk_cnt)
        return glbl_chnk_cnt
    return ingest_urls

def _make_get_make_chunkproc_args_compendia(insrt_missing_taxa: bool) -> \
        Callable:
    return functools.partial(_get_make_chunkproc_args_compendia,
                             insrt_missing_taxa)

# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
def _main_args(babel_compendia_url: str,
               babel_conflation_url: str,
               database_file_name: str,
               lines_per_chunk: int,
               use_existing_db: bool,
               test_type: Optional[int],
               test_compendia_file: Optional[str],
               quiet: bool,
               dry_run: bool,
               print_ddl: bool,
               temp_dir: str,
               no_exec: bool):
    log_work = not quiet
    # handle redefining _log_print first, in case any other function uses it
    _set_log_print_enabled(log_work)
    if temp_dir is not None:
        _customize_temp_dir(temp_dir, no_exec, quiet)
    if test_type is not None and test_type == 1 and test_compendia_file is None:
        raise ValueError("for test type 1, you must specify --test_compendia_file")
    _initialize_ray()
    print_ddl_file_obj = sys.stderr if print_ddl else None
    compendia_sorted_files, compendia_map_names = \
        _get_compendia_files(babel_compendia_url)
    conflation_sorted_files, conflation_map_names = \
        _get_conflation_files(babel_conflation_url)
    start_time_sec = time.time()
    _log_print("Starting database ingest")
    if test_type:
        _log_print(f"Running in test mode; test type: {test_type}")
    ingest_urls = _make_ingest_urls(dry_run)
    with _get_database(database_file_name,
                       from_scratch=not use_existing_db,
                       print_ddl_file_obj=print_ddl_file_obj) as conn:
        if not use_existing_db:
            functools.partial(_ingest_biolink_categories, conn, log_work)(
                *su.get_biolink_categories())
            _create_indices(conn, print_ddl_file_obj=print_ddl_file_obj)
        do_ingest_compendia_url = \
            _make_url_ingester(conn, lines_per_chunk,
                               _read_compendia_chunks, log_work)
        do_ingest_conflation_url = \
            _make_url_ingester(conn, lines_per_chunk,
                               _read_conflation_chunks, log_work)
        make_conflation_chunk_processor = \
            functools.partial(_make_conflation_chunk_processor, conn)
        make_compendia_chunk_processor = \
            functools.partial(_make_compendia_chunk_processor, conn)
        glbl_chnk_cnt = 0
        get_make_chunkproc_args_conflation = \
            functools.partial(_get_make_chunkproc_args_conflation,
                              su.CONFLATION_TYPE_NAMES_IDS)
        ingest_args_compendia = \
            {"file_names": compendia_sorted_files,
             "file_map": compendia_map_names,
             "file_type": "compendia",
             "base_url": babel_compendia_url,
             "ingest_url": do_ingest_compendia_url,
             "start_time_sec": start_time_sec,
             "make_chunk_processor": make_compendia_chunk_processor,
             "get_make_chunk_processor_args":
             _make_get_make_chunkproc_args_compendia(insrt_missing_taxa=True),
             "glbl_chnk_cnt": glbl_chnk_cnt}
        ingest_args_conflation = \
            {"file_names": conflation_sorted_files,
             "file_map": conflation_map_names,
             "file_type": "conflation",
             "base_url": babel_conflation_url,
             "ingest_url": do_ingest_conflation_url,
             "start_time_sec": start_time_sec,
             "make_chunk_processor": make_conflation_chunk_processor,
             "get_make_chunk_processor_args":
             get_make_chunkproc_args_conflation,
             "glbl_chnk_cnt": glbl_chnk_cnt}
        if test_type == 1:
            assert test_compendia_file is not None
            ingest_args_compendia.update({
                "file_names": (test_compendia_file,),
                "base_url": "",
                "file_map": _create_file_map(test_compendia_file)})
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
        elif test_type == 2:
            ingest_args_compendia.update({
                "file_names": TEST_2_COMPENDIA})
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
        elif test_type == 3:
            ingest_args_compendia.update({
                "file_names": TEST_3_COMPENDIA,
                "get_make_chunk_processor_args":
                _make_get_make_chunkproc_args_compendia(insrt_missing_taxa=False)})
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
            ingest_args_conflation.update({
                "file_names": TEST_3_CONFLATION,
                "glbl_chnk_cnt": glbl_chnk_cnt})
            glbl_chnk_cnt = ingest_urls(**ingest_args_conflation)
        elif test_type == 4:
            ingest_args_compendia.update({
                "file_names": TEST_4_COMPENDIA,})
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
        elif test_type is None:
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
            ingest_args_conflation.update({"glbl_chnk_cnt": glbl_chnk_cnt})
            glbl_chnk_cnt = ingest_urls(**ingest_args_conflation)
        else:
            assert False, f"invalid test_type: {test_type}; " \
                          "must be one of 1, 2, 3, or None"
        _do_final_cleanup(conn, log_work, glbl_chnk_cnt, start_time_sec)

def _main():
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    _main_args(**su.namespace_to_dict(_get_args()))
