#!/usr/bin/env python3

# Stephen A. Ramsey
# Oregon State University
# February 2025

# Empirical testing indicates that this script should be able to complete
# an ingest of Babel in less than 5 days.

# # How to run the Babel ingest
# - `ssh ubuntu@stitch.rtx.ai`
# - `cd stitch`
# - `screen`
# - `source venv/bin/activate`
# - `python3.12 -u ingest_babel.py > ingest_babel.log 2>&1`
# - `ctrl-X D` (to exit the screen session)
# - `tail -f ingest_babel.log` (so you can watch the progress)
# - In another terminal session, watch memory usage using `top`

# Thank you to Gaurav Vaidya for helpful information about Babel!

import argparse
import bmt
from datetime import datetime
from htmllistparse import htmllistparse
import importlib
import json
import logging
import math
import numpy as np
import os
import pandas as pd
import ray
import sqlite3
import subprocess
import sys
import swifter  # noqa: F401
import time
from typing import Optional, IO

DEFAULT_BABEL_COMPENDIA_URL = \
    'https://stars.renci.org/var/babel_outputs/2025jan23/compendia/'
DEFAULT_DATABASE_FILE_NAME = 'babel.sqlite'

DEFAULT_TEST_TYPE = None
DEFAULT_TEST_FILE = "test-tiny.jsonl"
DEFAULT_CHUNK_SIZE = 100000


def get_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser(description='ingest_babel.py: '
                                         'ingest the Babel compendia '
                                         ' files into a sqlite3 database')
    arg_parser.add_argument('--babel-compendia-url',
                            type=str,
                            dest='babel_compendia_url',
                            default=DEFAULT_BABEL_COMPENDIA_URL,
                            help='the URL of the web page containing an HTML '
                            'index listing of Babel compendia files')
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
                            action='store_true',
                            help='do not replace the cpython process with another '
                            'cpython process (via exec() system call); this is '
                            'used internally by the ingest_babel.py script, which '
                            'in order to programmatically set the SQLITE_TMPDIR '
                            'environment variable and have it take effect, must '
                            'exec() python ingest_babel.py to start a new interpreter.')
    return arg_parser.parse_args()


# this function does not return microseconds
def cur_datetime_local() -> datetime:
    return datetime.now().astimezone().replace(microsecond=0)


def create_index(table: str,
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


def set_auto_vacuum(conn: sqlite3.Connection,
                    auto_vacuum_on: bool,
                    quiet: bool = False):
    switch_str = 'FULL' if auto_vacuum_on else 'NONE'
    conn.execute(f"PRAGMA auto_vacuum={switch_str};")
    if not quiet:
        print(f"setting auto_vacuum to: {switch_str}")


def create_empty_database(database_file_name: str,
                          log_work: bool = False,
                          print_ddl_file_obj: IO[str] | None = None) -> \
                          sqlite3.Connection:
    if os.path.exists(database_file_name):
        os.remove(database_file_name)
    conn = sqlite3.connect(database_file_name)
    set_auto_vacuum(conn, False)
    cur = conn.cursor()
    table_creation_statements = (
        ('types',
         '''
         CREATE TABLE types (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         curie TEXT NOT NULL UNIQUE);
         '''),
        ('identifiers',
         '''
         CREATE TABLE identifiers (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         curie TEXT NOT NULL UNIQUE,
         label TEXT);
         '''),
        ('cliques',
         '''
         CREATE TABLE cliques (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         primary_identifier_id INTEGER NOT NULL,
         ic REAL,
         type_id INTEGER NOT NULL,
         preferred_name TEXT NOT NULL,
         FOREIGN KEY(primary_identifier_id) REFERENCES identifiers(id),
         FOREIGN KEY(type_id) REFERENCES types(id));
         '''),
        ('descriptions',
         '''
         CREATE TABLE descriptions (
         id INTEGER PRIMARY KEY AUTOINCREMENT,
         desc TEXT NOT NULL);
         '''),
        ('identifiers_descriptions',
         '''
         CREATE TABLE identifiers_descriptions (
         description_id INTEGER NOT NULL,
         identifier_id INTEGER NOT NULL,
         FOREIGN KEY(description_id) REFERENCES descriptions(id),
         FOREIGN KEY(identifier_id) REFERENCES identifiers(id));
         '''),
        ('identifiers_cliques',
         '''
         CREATE TABLE identifiers_cliques (
         identifier_id INTEGER NOT NULL,
         clique_id INTEGER NOT NULL,
         FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
         FOREIGN KEY(clique_id) REFERENCES cliques(id));
         '''),
        ('identifiers_taxa',
         '''
         CREATE TABLE identifiers_taxa (
         identifier_id INTEGER NOT NULL,
         taxa_identifier_id INTEGER NOT NULL,
         FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
         FOREIGN KEY(taxa_identifier_id) REFERENCES identifiers(id));
         '''))

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


def get_database(database_file_name: str,
                 log_work: bool = False,
                 from_scratch: bool = True,
                 print_ddl_file_obj: IO[str] | None = None) -> \
                 sqlite3.Connection:
    if from_scratch:
        return create_empty_database(database_file_name,
                                     log_work,
                                     print_ddl_file_obj)
    else:
        conn = sqlite3.connect(database_file_name)
        set_auto_vacuum(conn, False)
        conn.execute("VACUUM;")
        return conn


def nan_to_none(o):
    return o if not np.isnan(o) else None


def first_label(group_df: pd.DataFrame):
    return group_df.iloc[0]


def ingest_nodenorm_jsonl_chunk(chunk: pd.core.frame.DataFrame,
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
                             identif_struct.get('t', None))
                            for clique_info in chunk.identifiers.tolist()
                            for ci, identif_struct in enumerate(clique_info))
    curies_df = pd.DataFrame.from_records(curies_and_info,
                                          columns=('curie',
                                                   'label',
                                                   'cis',
                                                   'taxa'))
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
    missing_df_dedup = missing_df_gb.apply(first_label,
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
        (clique['primary_curie'],
         nan_to_none(clique['ic']),
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
        (curies_to_pkids[clique_data['primary_curie']],
         clique_data['clique_pkid'])
        for id, clique_data in chunk.iterrows())

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


def get_biolink_categories(log_work: bool = False) -> tuple[str]:
    tk = bmt.Toolkit()
    if log_work:
        ver = tk.get_model_version()
        print(f"loading Biolink model version: {ver}")
    return tuple(tk.get_all_classes(formatted=True))


def ingest_biolink_categories(biolink_categories: tuple[str],
                              conn: sqlite3.Connection,
                              log_work: bool = False):
    try:
        # Faster writes, but less safe
        conn.execute("BEGIN TRANSACTION;")
        cursor = conn.cursor()
        if log_work:
            print("ingesting Biolink categories")
        cursor.executemany("INSERT INTO types (curie) VALUES (?);",
                           tuple((i,) for i in biolink_categories))
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def byte_count_df(df: pd.core.frame.DataFrame) -> int:
    return len(json.dumps(df.to_dict(orient='records'))) + \
        df.shape[0]


SECS_PER_MIN = 60
SECS_PER_HOUR = 3600


def convert_sec(seconds: float) -> str:
    hours: int = int(seconds // SECS_PER_HOUR)
    minutes: int = int((seconds % SECS_PER_HOUR) // SECS_PER_MIN)
    remaining_seconds: float = seconds % SECS_PER_MIN
    return f"{hours:03d}:{minutes:02d}:{remaining_seconds:02.0f}"


ROWS_PER_ANALYZE = 20000000
ROWS_PER_VACUUM = 10 * ROWS_PER_ANALYZE  # left operand must be integer > 0


def ingest_jsonl_url(url: str,
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
            print(f"  Loading chunk {chunk_ctr}", end=end_str)
            conn.execute("BEGIN TRANSACTION;")
            if log_work and chunk_ctr == 1:
                start_time = time.time()
                chunk_start_time = start_time
            else:
                chunk_start_time = time.time()
            ingest_nodenorm_jsonl_chunk(chunk,
                                        conn,
                                        insrt_missing_taxa=insrt_missing_taxa)
            conn.commit()
            if log_work:
                sub_end_str = "" if total_size is not None else "\n"
                chunk_end_time = time.time()
                elapsed_time = (chunk_end_time - start_time)
                elapsed_time_str = convert_sec(elapsed_time)
                chunk_elapsed_time_str = convert_sec(chunk_end_time -
                                                     chunk_start_time)
                print(f"; time spent on URL: {elapsed_time_str}; "
                      f"spent on chunk: {chunk_elapsed_time_str}",
                      end=sub_end_str)
                if total_size is not None:
                    if chunk_ctr == 1:
                        chunk_size = byte_count_df(chunk)
                        num_chunks = math.ceil(total_size / chunk_size)
                    pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
                    time_to_complete = elapsed_time * \
                        (100.0 - pct_complete)/pct_complete
                    time_to_complete_str = convert_sec(time_to_complete)
                    print(f"; {pct_complete:0.2f}% complete"
                          f"; time to complete: {time_to_complete_str}")
            if (chunk_ctr + glbl_chnk_cnt_start) % \
               chunks_per_analyze == 0:
                if log_work:
                    analyze_start_time = time.time()
                conn.execute("ANALYZE")
                if log_work:
                    analyze_end_time = time.time()
                    analyze_elapsed_time = convert_sec(analyze_end_time -
                                                       analyze_start_time)
                    print(f"running ANALYZE took: {analyze_elapsed_time} "
                          "(HHH:MM::SS)")
                if (chunk_ctr + glbl_chnk_cnt_start) % \
                   chunks_per_vacuum == 0:
                    if log_work:
                        vacuum_start_time = time.time()
                    conn.execute("VACUUM")
                    if log_work:
                        vacuum_end_time = time.time()
                        vacuum_elapsed_time = convert_sec(vacuum_end_time -
                                                          vacuum_start_time)
                        print(f"running VACUUM took: {vacuum_elapsed_time} "
                              "(HHH:MM::SS)")
        except Exception as e:
            conn.rollback()
            raise e

    return chunk_ctr + glbl_chnk_cnt_start


def create_indices(conn: sqlite3.Connection,
                   log_work: bool = False,
                   print_ddl_file_obj: IO[str] | None = None):
    work_plan = (('cliques',                  'type_id'),
                 ('cliques',                  'primary_identifier_id'),
                 ('identifiers_descriptions', 'description_id'),
                 ('identifiers_descriptions', 'identifier_id'),
                 ('identifiers_cliques',      'identifier_id'),
                 ('identifiers_cliques',      'clique_id'),
                 ('identifiers_taxa',         'identifier_id'),
                 ('identifiers_taxa',         'taxa_identifier_id'))

    for table, col in work_plan:
        create_index(table, col, conn, log_work, print_ddl_file_obj)


TEST_2_COMPENDIA = ('OrganismTaxon.txt',
                    'ComplexMolecularMixture.txt',
                    'Polypeptide.txt',
                    'PhenotypicFeature.txt')
TAXON_FILE = 'OrganismTaxon.txt'

MAX_FILE_SIZE_BEFORE_SPLIT_BYTES = 10000000000
FILE_NAME_SUFFIX_START_NUMBERED = '.txt.00'


def prune_files(file_list: list[htmllistparse.FileEntry]) ->\
        tuple[list[htmllistparse.FileEntry],
              dict[str, htmllistparse.FileEntry]]:
    use_names: list[str] = []
    map_names = {fe.name: fe for fe in file_list}
    for file_entry in file_list:
        file_name = file_entry.name
        if '.txt' in file_name:
            if file_name.endswith(FILE_NAME_SUFFIX_START_NUMBERED):
                file_name_start_numbered_ind = \
                    file_name.find(FILE_NAME_SUFFIX_START_NUMBERED)
                file_name_prefix = \
                    file_name[0:file_name_start_numbered_ind]
                use_names.remove(file_name_prefix + '.txt')
            use_names.append(file_name)
        else:
            print(f"Warning: unrecognized file name {file_name}",
                  file=sys.stderr)
    return [map_names[file_name] for file_name in use_names], \
        {file_name: map_names[file_name] for file_name in use_names}


def namespace_to_dict(namespace):
    return {
        k: namespace_to_dict(v) if isinstance(v, argparse.Namespace) else v
        for k, v in vars(namespace).items()
    }


def main(babel_compendia_url: str,
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
        if not quiet:
            print(f"For Ray and sqlite3, setting temp dir to: {temp_dir}")
        os.environ["RAY_TMPDIR"] = temp_dir
        os.environ["SQLITE_TMP"] = temp_dir
        if sys.platform == "darwin":
            # The reload is necessary because sqlite3 reads the environment
            # variable at import and there is seemingly no way to change it
            # after that:
            importlib.reload(sqlite3)
        elif sys.platform == "linux" or sys.platform == "linux2":
            # here, need to restart python
            if not no_exec:
                subprocess.run([sys.executable] +
                               (sys.argv + ['--no-exec']),
                               env=os.environ.copy())
        else:
            raise ValueError("this script does not run in Windows")

    # initialize Ray after any changes to tmp dir location
    logging.getLogger("ray").setLevel(logging.ERROR)
    ray.init(logging_level=logging.ERROR)

    print_ddl_file_obj: IO[str] | None
    if print_ddl:
        print_ddl_file_obj = sys.stderr
    else:
        print_ddl_file_obj = None
    log_work = not quiet
    from_scratch = not use_existing_db
    listing: list[htmllistparse.FileEntry]
    _, listing = htmllistparse.fetch_listing(babel_compendia_url)
    pruned_files, map_names = prune_files(listing)
    sorted_files = sorted(pruned_files,
                          key=lambda i: 0 if i.name == TAXON_FILE else 1)

    start_time_sec = time.time()
    date_time_local = cur_datetime_local().isoformat()
    print(f"Starting database ingest at: {date_time_local}")
    if test_type:
        print(f"Running in test mode; test type: {test_type}")
    with get_database(database_file_name,
                      log_work=log_work,
                      from_scratch=from_scratch,
                      print_ddl_file_obj=print_ddl_file_obj) as conn:
        conn.execute("PRAGMA synchronous = OFF;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA wal_autocheckpoint = 1000;")

        if from_scratch:
            ingest_biolink_categories(get_biolink_categories(log_work),
                                      conn,
                                      log_work)
            create_indices(conn,
                           log_work,
                           print_ddl_file_obj=print_ddl_file_obj)
        glbl_chnk_cnt = 0
        if test_type == 1:
            if test_file is None:
                raise ValueError("test_file cannot be None")
            print(f"ingesting file: {test_file}")
            glbl_chnk_cnt = \
                ingest_jsonl_url(test_file,
                                 conn=conn,
                                 chunk_size=chunk_size,
                                 log_work=log_work,
                                 insrt_missing_taxa=True,
                                 glbl_chnk_cnt_start=glbl_chnk_cnt)
        elif test_type == 2:
            # after ingesting Biolink categories, need to ingest OrganismTaxon
            # first!
            start_time = time.time()
            for file_name in TEST_2_COMPENDIA:
                file_entry = map_names[file_name]
                file_size = file_entry.size
                print(f"ingesting file: {file_name}")
                cur_time = time.time()
                elapsed_time_str = convert_sec(cur_time - start_time)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        ingest_jsonl_url(babel_compendia_url + file_name,
                                         conn=conn,
                                         chunk_size=chunk_size,
                                         log_work=log_work,
                                         total_size=file_size,
                                         insrt_missing_taxa=True,
                                         glbl_chnk_cnt_start=glbl_chnk_cnt)
        else:
            assert test_type is None, f"invalid test_type: {test_type}"
            start_time = time.time()
            print(f"ingesting compendia files at: {babel_compendia_url}")
            for file_entry in sorted_files:
                file_name = file_entry.name
                file_size = file_entry.size
                cur_time = time.time()
                elapsed_time_str = convert_sec(cur_time - start_time)
                print(f"at elapsed time: {elapsed_time_str}; "
                      f"starting ingest of file: {file_name}; "
                      f"file size: {file_size} bytes")
                if not dry_run:
                    glbl_chnk_cnt = \
                        ingest_jsonl_url(babel_compendia_url +
                                         file_name,
                                         conn=conn,
                                         chunk_size=chunk_size,
                                         log_work=log_work,
                                         total_size=file_size,
                                         insrt_missing_taxa=True,
                                         glbl_chnk_cnt_start=glbl_chnk_cnt)
        conn.execute("PRAGMA wal_checkpoint(FULL);")
        conn.execute("PRAGMA journal_mode = DELETE;")
        set_auto_vacuum(conn, True)
        if log_work:
            final_cleanup_start_time = time.time()
        conn.execute("ANALYZE")
        conn.execute("VACUUM")
        if log_work:
            final_cleanup_end_time = time.time()
            final_cleanup_elapsed_time = convert_sec(final_cleanup_end_time -
                                                     final_cleanup_start_time)
            print("running ANALYZE and VACUUM (final cleanup) took: "
                  f"{final_cleanup_elapsed_time} (HHH:MM::SS)")
    date_time_local = cur_datetime_local().isoformat()
    print(f"Finished database ingest at: {date_time_local}")
    print(f"Total number of chunks inserted: {glbl_chnk_cnt}")
    elapsed_time_str = convert_sec(time.time() - start_time_sec)
    print(f"Elapsed time for Babel ingest: {elapsed_time_str} (HHH:MM::SS)")


if __name__ == "__main__":
    main(**namespace_to_dict(get_args()))
