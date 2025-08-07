#!/usr/bin/env python3.12

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
import functools
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
from typing import IO, Any, Callable, Iterable, Optional, cast

import numpy
import pandas as pd
import ray
from htmllistparse import htmllistparse

# The "noqa: F401" for "import swifter" is needed because swifter is somehow
# automagically used once you import it, but the "ruff" lint checker software
# doesn't detect that use of the "swifter" module, so it flags an F401 error.

# make it convenient to run ingest_babel.py using `python3 stitch/ingest_babel.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from stitch import stitchutils as su

ChunkType = pd.DataFrame | list[str]


DEFAULT_BABEL_RELEASE_URL =  'https://stars.renci.org/var/babel_outputs/2025mar31/'
DEFAULT_BABEL_COMPENDIA_URL = urllib.parse.urljoin(DEFAULT_BABEL_RELEASE_URL,
                                                   'compendia/')
DEFAULT_BABEL_CONFLATION_URL = urllib.parse.urljoin(DEFAULT_BABEL_RELEASE_URL,
                                                    'conflation/')
DEFAULT_DATABASE_FILE_NAME = 'babel.sqlite'

DEFAULT_TEST_TYPE = None
DEFAULT_COMPENDIA_TEST_FILE = "test-tiny.jsonl"
DEFAULT_LINES_PER_CHUNK = 100_000
WAL_SIZE = 1000

UNKNOWN_TAXON = "unknown taxon"  # this is ony to be used in testing

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
    arg_parser.add_argument('--lines-per-chunk',
                            type=int,
                            dest='lines_per_chunk',
                            default=DEFAULT_LINES_PER_CHUNK,
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
    arg_parser.add_argument('--test-compendia-file',
                            type=str,
                            dest='test_compendia_file',
                            default=DEFAULT_COMPENDIA_TEST_FILE,
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

def _cur_datetime_local_str() -> str:
    return _cur_datetime_local_no_ms().isoformat()

def _make_log_print(log_work: bool) -> Callable:
    def log_print(message: str,
                  end: str = "\n"):
        if log_work:
            date_time_local = _cur_datetime_local_str()
            print(f"{date_time_local}: " + message, end=end)
    return log_print


# in `main`, _log_print will be redefined as a function
# (returned as a Callable by the function _make_log_print)
# such that the redefined _log_print function will internally
# have access (as a "closure") to the `log_work` variable
def _noop_log_print(_: str,
                    __: str = "\n") -> None:
    pass
_log_print: Callable[..., None] = _noop_log_print

def _create_index(table: str,
                  col: str,
                  conn: sqlite3.Connection,
                  log_work: bool = False,
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
                     auto_vacuum_on: bool,
                     log_work: bool = True):
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
    _set_auto_vacuum(conn, auto_vacuum_on=False, log_work=log_work)
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
        _log_print(f"creating table: \"{table_name}\"")
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
    _run_vacuum(conn, log_work)
    return conn

def _run_vacuum(conn: sqlite3.Connection,
                log_work: bool = False):
    _log_print("starting database VACUUM")
    conn.execute("VACUUM;")
    _log_print("completed database VACUUM")

def _first_label(group_df: pd.DataFrame) -> pd.Series:
    return group_df.iloc[0]

def _ingest_biolink_categories(biolink_categories: set[str],
                               conn: sqlite3.Connection,
                               log_work: bool = False):
    try:
        if log_work:
            len_cat = len(biolink_categories)
            _log_print(f"ingesting {len_cat} Biolink categories")
        # Faster writes, but less safe
        conn.execute("BEGIN TRANSACTION;")
        conn.cursor().executemany("INSERT INTO types (curie) VALUES (?);",
                                  tuple((i,) for i in biolink_categories))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e

def _byte_count_chunk(chunk: ChunkType) -> int:
    dumpable = chunk.to_dict(orient='records') \
        if isinstance(chunk, pd.core.frame.DataFrame) \
           else chunk
    return len(json.dumps(dumpable))

# stop after 300 million rows; not worth the effort anymore
ROWS_PER_ANALYZE = (1_000_000,
                    3_000_000,
                    10_000_000,
                    30_000_000,
                    100_000_000,
                    300_000_000)

def _insert_and_return_id(cursor: sqlite3.Cursor,
                          sql: str,
                          params: tuple) -> int:
    return cursor.execute(sql, params).fetchone()[0]

def _curies_to_pkids(conn: sqlite3.Connection,
                     curies: set[str],
                     table_name: str = "temp_curies") -> dict[str, int]:
    conn.execute(f"CREATE TEMP TABLE {table_name} (curie TEXT PRIMARY KEY)")
    conn.executemany(f"INSERT INTO {table_name} (curie) VALUES (?)",
                     ((curie,) for curie in curies))
    rows = conn.execute(f"""
        SELECT identifiers.curie, identifiers.id
        FROM identifiers
        JOIN {table_name} ON identifiers.curie = {table_name}.curie
    """).fetchall()
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    return {curie: pkid for curie, pkid in rows}


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

# only set `insrt_missing_taxa` to True for testing!
def _make_compendia_chunk_processor(conn: sqlite3.Connection,
                                    insrt_missing_taxa: bool = False) -> Callable:
    def process_compendia_chunk(chunk: pd.DataFrame):
        unique_biolink_categories = list(chunk['type'].unique())
        if unique_biolink_categories:
            # Create a string of placeholders like "?, ?, ?..."
            placeholders = ', '.join('?' for _ in unique_biolink_categories)
            query = ("SELECT curie, id FROM types "
                     f"WHERE curie IN ({placeholders})")
            rows = conn.execute(query, unique_biolink_categories).fetchall()

            # Build dictionary from query results
            biolink_curie_to_pkid = {curie: pkid for curie, pkid in rows}

            # Check for any missing curies
            missing = set(unique_biolink_categories) - biolink_curie_to_pkid.keys()
            if missing:
                raise ValueError(f"Missing CURIEs in types table: {missing}")
        else:
            biolink_curie_to_pkid = {}

        curies_and_info = []
        data_to_insert_cliques = []
        primary_curies = []

        for id, row in enumerate(chunk.itertuples(index=False, name=None)):
            bltype, ic, identifiers, preferred_name, taxa = row
            identifiers = cast(list[dict[str, Any]], identifiers)
            primary = identifiers[0]['i'] if identifiers else None
            primary_curies.append(primary)

            for ci, identif_struct in enumerate(identifiers):
                curies_and_info.append(
                    (identif_struct['i'],
                     identif_struct.get('l'),
                     ci,
                     identif_struct.get('t'),
                     id)
                )

            data_to_insert_cliques.append(
                (su.nan_to_none(ic),
                 biolink_curie_to_pkid[bltype],
                 preferred_name)
            )

        chunk['primary_curie'] = primary_curies
        cliques_identifiers = chunk.identifiers.tolist()

        # curies_df has four columns: curie, label, cis, and taxa;
        # each row corresponds to a different identifier in the chunk
        curies_df = pd.DataFrame.from_records(curies_and_info,
                                              columns=('curie',
                                                       'label',
                                                       'cis',
                                                       'taxa',
                                                       'chunk_row'))

        curies = curies_df.curie.tolist()
        curie_pkids = dict.fromkeys(curies, None)
        curie_pkids.update(_curies_to_pkids(conn, set(curies)))
        curies_df['pkid'] = curies_df['curie'].map(curie_pkids)
        missing_series = curies_df.pkid.isna()
        missing_df = curies_df.loc[missing_series][['curie', 'label']]
        missing_df_gb = missing_df.groupby(by='curie')
        missing_df_dedup = missing_df_gb.apply(_first_label,
                                               include_groups=False)
        missing_df_t = tuple(missing_df_dedup.itertuples(index=True,
                                                         name=None))
        cursor = conn.cursor()

        ids_inserted = {curie_label[0]:
                        _insert_and_return_id(cursor,
                                             "INSERT INTO identifiers "
                                             "(curie, label) "
                                             "VALUES (?, ?) RETURNING id;",
                                             curie_label)
                        for curie_label in missing_df_t}
        mask = curies_df.pkid.isna()
        curies_df.loc[mask, 'pkid'] = curies_df.loc[mask,
                                                    'curie'].map(ids_inserted)

        curies_to_pkids = dict(curies_df[['curie',
                                          'pkid']].itertuples(index=False,
                                                              name=None))

        taxa = tuple(_flatten_taxa(curies_df['taxa']))

        if taxa:
            taxa_to_pkids = dict.fromkeys(set(taxa), None)
            taxa_to_pkids.update(_curies_to_pkids(conn, set(taxa)))

            # Build a mapping from taxon CURIE to its id
            for taxon_curie, taxon_id in taxa_to_pkids.items():
                if taxon_id is None:
                    if insrt_missing_taxa:
                        id = \
                            _insert_and_return_id(cursor,
                                                  'INSERT INTO identifiers '
                                                  '(curie, label) '
                                                  f'VALUES (?, \'{UNKNOWN_TAXON}\')'
                                                  'RETURNING id;',
                                                  (taxon_curie,))
                        taxa_to_pkids[taxon_curie] = id
                    else:
                        raise ValueError("taxon missing from database: "
                                         f"{taxon_curie}")

            insert_data = tuple(
                (curies_to_pkids[row_curie],
                 taxa_to_pkids[t])
                for row_curie, _, _, row_taxa, _, _
                in curies_df.itertuples(index=False, name=None)
                for t in cast(list[str], row_taxa)
            )

            cursor.executemany('INSERT INTO identifiers_taxa '
                               '(identifier_id, taxa_identifier_id) '
                               'VALUES (?, ?);',
                               insert_data)

        pkids_for_cliques = tuple(curies_to_pkids[primary_id]
                                  for primary_id in primary_curies)

        data_to_insert_cliques_final = tuple((pkid, *data) \
                                             for pkid, data in zip(pkids_for_cliques,
                                                                   data_to_insert_cliques))

        clique_pkids = tuple(
            _insert_and_return_id(cursor,
                                  'INSERT INTO cliques '
                                  '(primary_identifier_id, ic, type_id, '
                                  'preferred_name) '
                                  'VALUES (?, ?, ?, ?) '
                                  'RETURNING id;',
                                  clique_data)
            for clique_data in data_to_insert_cliques_final)

        chunk['clique_pkid'] = clique_pkids

        clique_pkid_list = chunk['clique_pkid'].tolist()
        identifiers_cliques_data = tuple(
            (row_pkid, int(clique_pkid_list[chunk_row]))
            for _, _, _, _, chunk_row, row_pkid in curies_df.itertuples(index=False)
        )

        cursor.executemany('INSERT INTO identifiers_cliques '
                           '(identifier_id, clique_id) '
                           'VALUES (?, ?);',
                           identifiers_cliques_data)

        description_ids = tuple(
            (_insert_and_return_id(cursor,
                                   'INSERT INTO descriptions '
                                   '(desc) '
                                   'VALUES (?) '
                                   'RETURNING id;',
                                   (description_str,)),
             curies_to_pkids[clique_identifier_info['i']])
            for clique_info in cliques_identifiers
            for clique_identifier_info in clique_info
            for description_str in clique_identifier_info.get('d', []))

        cursor.executemany('INSERT INTO identifiers_descriptions '
                           '(description_id, identifier_id) '
                           'VALUES (?, ?);',
                           description_ids)
    return process_compendia_chunk

def _read_compendia_chunks(url: str,
                           lines_per_chunk: int) -> Iterable[pd.DataFrame]:
    return pd.read_json(url,
                        lines=True,
                        chunksize=lines_per_chunk)

def _read_conflation_chunks(url: str,
                            lines_per_chunk: int) -> Iterable[list[str]]:
    return su.get_line_chunks_from_url(url, lines_per_chunk)

def _make_url_ingester(conn: sqlite3.Connection,
                       lines_per_chunk: int,
                       read_chunks: Callable[[str, int], Iterable[ChunkType]],
                       log_work: bool = False) -> Callable:
    def ingest_from_url(url: str,
                        process_chunk: Callable[[ChunkType], None],
                        total_size: Optional[int] = None,
                        glbl_chnk_cnt_start: int = 0) -> int:
        chunk_ctr = 0
        chunks_per_analyze_list: list[int] = [int(x) for x in
                                              numpy.ceil(numpy.array(ROWS_PER_ANALYZE)/\
                                                         lines_per_chunk)]
        for chunk in read_chunks(url, lines_per_chunk):
            chunk_ctr += 1
            try:
                log_str = f"Loading compendia chunk {chunk_ctr}"
                conn.execute("BEGIN TRANSACTION;")
                if log_work and chunk_ctr == 1:
                    start_time = time.time()
                    chunk_start_time = start_time
                else:
                    chunk_start_time = time.time()
                process_chunk(chunk)
                conn.commit()
                if log_work:
                    elapsed_time = (time.time() - start_time)
                    elapsed_time_str = su.format_time_seconds_to_str(elapsed_time)
                    chunk_elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                                           chunk_start_time)
                    log_str += (f"; time spent on URL: {elapsed_time_str}; "
                                f"spent on chunk: {chunk_elapsed_time_str}")
                    if total_size is not None:
                        if chunk_ctr == 1:
                            chunk_size = _byte_count_chunk(chunk)
                            num_chunks = math.ceil(total_size / chunk_size)
                        pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
                        time_to_complete = elapsed_time * \
                            (100.0 - pct_complete)/pct_complete
                        time_to_complete_str = \
                            su.format_time_seconds_to_str(time_to_complete)
                        log_str += (f"; URL {pct_complete:0.2f}% complete"
                                    f"; time to complete URL: {time_to_complete_str}")
                    _log_print(log_str)
                if any(chunk_ctr + glbl_chnk_cnt_start == chunks_per_analyze \
                       for chunks_per_analyze in chunks_per_analyze_list):
                    _do_index_analyze(conn, log_work)
            except Exception as e:
                conn.rollback()
                raise e
        return chunk_ctr + glbl_chnk_cnt_start
    return ingest_from_url

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

def _create_file_map(file_name: str) -> dict[str, htmllistparse.FileEntry]:
    file_size = os.path.getsize(file_name)
    file_modif = os.path.getmtime(file_name)
    file_entry = htmllistparse.FileEntry(
        file_name,
        file_modif,
        file_size,
        "file")
    return {file_name: file_entry}

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
    # need to ingest OrganismTaxon compendia file first
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
                               wal_size: int,
                               log_work: bool):
    _log_print("setting PRAGMA synchronous to OFF")
    conn.execute("PRAGMA synchronous = OFF;")
    _log_print("setting PRAGMA journal_mode WAL")
    conn.execute("PRAGMA journal_mode = WAL;")
    _log_print("setting PRAGMA optimize")
    conn.execute("PRAGMA optimize;")
    _log_print("setting PRAGMA wal_autocheckpoint")
    conn.execute(f"PRAGMA wal_autocheckpoint = {wal_size};")

def _set_pragmas_for_querying(conn: sqlite3.Connection,
                              log_work: bool):
    # wal_checkpoint(FULL) is the correct choice for
    # a read-only database in query mode:
    _log_print("setting PRAGMA wal_checkpoint to FULL")
    conn.execute("PRAGMA wal_checkpoint(FULL);")
    # journal_mode=DELETE is the correct choice for
    # a read-only database in query mode:
    _log_print("setting PRAGMA journal_mode to DELETE")
    conn.execute("PRAGMA journal_mode = DELETE;")
    # This last one is unnecessary if DB is read-only:
    _log_print("setting PRAGMA synchronous to FULL")
    conn.execute("PRAGMA synchronous = FULL;")

def _do_integrity_check(conn: sqlite3.Connection,
                        log_work: bool):
    _log_print("running PRAGMA integrity_check")
    result = conn.execute("PRAGMA integrity_check").fetchall()
    if result != [("ok",)]:
        raise RuntimeError(f"Database integrity check failed: {result}")

def _cleanup_indices(conn: sqlite3.Connection,
                     log_work: bool):
    _do_index_analyze(conn, log_work)
    _log_print("setting PRAGMA locking_mode to EXCLUSIVE")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
    _run_vacuum(conn, log_work)
    _do_integrity_check(conn, log_work)

def _do_final_cleanup(conn: sqlite3.Connection,
                      log_work: bool,
                      glbl_chnk_cnt: int,
                      start_time_sec: float):
        if log_work:
            final_cleanup_start_time = time.time()
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
    # the "noqa" is to quiet a Vulture warning
    # while at the same time not upsetting "ruff":
    tempfile.tempdir = temp_dir  # noqa
    if not quiet:
        _log_print(f"Setting temp dir to: {temp_dir}")

def _log_start_of_file(start: float,
                       filetype: str,
                       filename: str,
                       filesize: int):
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
    return {'insrt_missing_taxa': insrt_missing_taxa}

def _make_ingest_urls(dry_run: bool,
                      log_work: bool) -> Callable:
    def ingest_urls(file_names: Iterable[str],
                    file_map: dict[str, htmllistparse.FileEntry],
                    base_url: str,
                    file_type: str,
                    start_time_sec: float,
                    make_chunk_processor: Callable,
                    get_make_chunk_processor_args: Callable,
                    ingest_url: Callable,
                    glbl_chnk_cnt_start: int) -> int:
        ingest_location = base_url if base_url != "" else "(local)"
        _log_print(f"ingesting {file_type} files at: {ingest_location}")
        for file_name in file_names:
            file_size = file_map[file_name].size
            elapsed_str = _log_start_of_file(start_time_sec, file_type,
                                             file_name, file_size)
            _log_print(elapsed_str)
            if not dry_run:
                url = urllib.parse.urljoin(base_url, file_name)
                make_chunk_processor_args = get_make_chunk_processor_args(file_name)
                process_chunk = make_chunk_processor(**make_chunk_processor_args)
                glbl_chnk_cnt = ingest_url(url,
                                           process_chunk,
                                           total_size=file_size,
                                           glbl_chnk_cnt_start=glbl_chnk_cnt_start)
        return glbl_chnk_cnt
    return ingest_urls


def main(babel_compendia_url: str,
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

    # handle redefining _log_print first, in case any other function uses it
    log_work = not quiet
    global _log_print
    _log_print = _make_log_print(log_work)

    if temp_dir is not None:
        _customize_temp_dir(temp_dir, no_exec, quiet)

    if test_type is not None and test_type == 1 and test_compendia_file is None:
        raise ValueError("for test type 1, you must specify --test_compendia_file")

    _initialize_ray()

    print_ddl_file_obj = sys.stderr if print_ddl else None
    from_scratch = not use_existing_db

    compendia_sorted_files, compendia_map_names = \
        _get_compendia_files(babel_compendia_url)
    conflation_sorted_files, conflation_map_names = \
        _get_conflation_files(babel_conflation_url)
    start_time_sec = time.time()
    _log_print("Starting database ingest")
    if test_type:
        _log_print(f"Running in test mode; test type: {test_type}")

    ingest_urls = _make_ingest_urls(dry_run, log_work)

    with _get_database(database_file_name,
                       log_work=log_work,
                       from_scratch=from_scratch,
                       print_ddl_file_obj=print_ddl_file_obj) as conn:

        _set_pragmas_for_ingestion(conn, WAL_SIZE, log_work)

        if from_scratch:
            categ_set, biolink_ver = su.get_biolink_categories()
            _ingest_biolink_categories(categ_set,
                                       conn,
                                       log_work)
            _log_print(f"Using Biolink model version {biolink_ver}")
            _create_indices(conn,
                            log_work,
                            print_ddl_file_obj=print_ddl_file_obj)

        do_ingest_compendia_url = _make_url_ingester(conn,
                                                     lines_per_chunk,
                                                     _read_compendia_chunks,
                                                     log_work)
        do_ingest_conflation_url = _make_url_ingester(conn,
                                                      lines_per_chunk,
                                                      _read_conflation_chunks,
                                                      log_work)

        make_conflation_chunk_processor = \
            functools.partial(_make_conflation_chunk_processor, conn)
        make_compendia_chunk_processor = \
            functools.partial(_make_compendia_chunk_processor, conn)

        glbl_chnk_cnt = 0

        get_make_chunkproc_args_conflation = \
            functools.partial(_get_make_chunkproc_args_conflation,
                              su.CONFLATION_TYPE_NAMES_IDS)
        def make_get_make_chunkproc_args_compendia(insrt_missing_taxa: bool) -> \
                Callable:
            return functools.partial(_get_make_chunkproc_args_compendia,
                                     insrt_missing_taxa)

        ingest_args_compendia = \
            {
                "file_names": compendia_sorted_files,
                "file_map": compendia_map_names,
                "base_url": babel_compendia_url,
                "file_type": "compendia",
                "start_time_sec": start_time_sec,
                "make_chunk_processor": make_compendia_chunk_processor,
                "get_make_chunk_processor_args":
                make_get_make_chunkproc_args_compendia(insrt_missing_taxa=True),
                "ingest_url": do_ingest_compendia_url,
                "glbl_chnk_cnt_start": glbl_chnk_cnt
            }

        ingest_args_conflation = \
            {
                "file_names": conflation_sorted_files,
                "file_map": conflation_map_names,
                "base_url": babel_conflation_url,
                "file_type": "conflation",
                "start_time_sec": start_time_sec,
                "make_chunk_processor": make_conflation_chunk_processor,
                "get_make_chunk_processor_args":
                get_make_chunkproc_args_conflation,
                "ingest_url": do_ingest_conflation_url,
                "glbl_chnk_cnt_start": glbl_chnk_cnt
            }

        if test_type == 1:
            assert test_compendia_file is not None
            ingest_args_compendia.update({
                "file_names": (test_compendia_file,),
                "base_url": "",
                "file_map": _create_file_map(test_compendia_file)
            })
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
        elif test_type == 2:
            ingest_args_compendia.update({
                "file_names": TEST_2_COMPENDIA
            })
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
        elif test_type == 3:
            ingest_args_compendia.update({
                "file_names": TEST_3_COMPENDIA,
                "get_make_chunk_processor_args":
                make_get_make_chunkproc_args_compendia(insrt_missing_taxa=False)
            })
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
            ingest_args_conflation.update({
                "file_names": TEST_3_CONFLATION,
                "glbl_chnk_cnt_start": glbl_chnk_cnt
            })
            glbl_chnk_cnt = ingest_urls(**ingest_args_conflation)
        elif test_type is None:
            glbl_chnk_cnt = ingest_urls(**ingest_args_compendia)
            ingest_args_conflation.update({"glbl_chnk_cnt_start": glbl_chnk_cnt})
            glbl_chnk_cnt = ingest_urls(**ingest_args_conflation)
        else:
            assert False, f"invalid test_type: {test_type}; " \
                          "must be one of 1, 2, 3, or None"

        _set_pragmas_for_querying(conn, log_work)
        _set_auto_vacuum(conn,
                         auto_vacuum_on=True,
                         log_work=log_work)
        _do_final_cleanup(conn,
                          log_work,
                          glbl_chnk_cnt,
                          start_time_sec)

if __name__ == "__main__":
    main(**su.namespace_to_dict(_get_args()))

