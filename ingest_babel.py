#!/usr/bin/env python3

# Stephen A. Ramsey
# Oregon State University
# February 2025

# Empirical testing indicates that this script, on an MBP on M1 Max
# (ARM64) running MacOS Sonoma and running CPython 3.12, can ingest
# about 10,000 rows of JSON-lines (from the Babel Compendia files)
# per second. Since there are an estimated 482 million rows overall
# in Babel (very rough estimate based on row character count sizes
# in OrganismTaxon only!), this script should be able to complete
# in less than 24 hours, assuming that all SELECTs are using indexes
# as was intended (use of indexes needs to be empirically verified).

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

import bmt
import datetime
import htmllistparse
import io
import math
import numpy as np
import os
import pandas as pd
# import pprint
import sqlite3
# import sys
import time


DATABASE_FILE_NAME = 'babel.sqlite'
CHUNK_SIZE = 100000
BIOLINK_PREFIX = 'biolink:'
TAXON_NAME = 'OrganismTaxon'
TAXON_FILE = TAXON_NAME + '.txt'
TAXON_TYPE = BIOLINK_PREFIX + TAXON_NAME
SECS_PER_MIN = 60
SECS_PER_HOUR = 3600
BABEL_COMPENDIA_URL = \
    'https://stars.renci.org/var/babel_outputs/2025jan23/compendia/'

TEST_TYPE = 1
TEST_FILE = "test-tiny.jsonl"
LOG_WORK = True


# this function does not return microseconds
def cur_datetime_local() -> datetime.datetime:
    return datetime.datetime.now().astimezone().replace(microsecond=0)


def create_index(table: str,
                 col: str,
                 conn: sqlite3.Connection,
                 log_work: bool = False,
                 print_ddl: io.TextIOBase = None):
    statement = 'CREATE INDEX ' +\
        f'idx_{table}_{col} ' +\
        f'ON {table} ({col});'
    conn.execute(statement)
    if log_work:
        print(f"creating index on column \"{col}\" in table \"{table}\"")
    if print_ddl is not None:
        print(statement, file=print_ddl)


def create_empty_database(database_file_name: str,
                          log_work: bool = False,
                          print_ddl: io.TextIOBase = None) -> \
                          sqlite3.Connection:
    if os.path.exists(database_file_name):
        os.remove(database_file_name)
    conn = sqlite3.connect(database_file_name)
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
        if print_ddl is not None:
            print(statement, file=print_ddl)

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
    missing_df_gb = missing_df.groupby(by='curie')
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
                  for taxon in taxon_list})

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
                              in enumerate(row.iloc[2])
                              if subidx == 0]

    data_to_insert_cliques = tuple(
        (clique['primary_curie'],
         nan_to_none(clique['ic']),
         biolink_curie_to_pkid[clique['type']])
        for _, clique in chunk.iterrows())

    clique_pkids = tuple(
        cursor.execute('INSERT INTO cliques '
                       '(primary_identifier_id, ic, type_id) '
                       'VALUES (?, ?, ?) '
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
        conn.execute("PRAGMA synchronous = OFF;")
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
    finally:
        conn.execute("PRAGMA synchronous = FULL;")


def byte_count_df(df: pd.core.frame.DataFrame) -> int:
    return len(df.to_json(orient='records',
                          lines=True).encode('utf-8'))


def convert_seconds(seconds: float) -> str:
    hours: int = int(seconds // SECS_PER_HOUR)
    minutes: int = int((seconds % SECS_PER_HOUR) // SECS_PER_MIN)
    remaining_seconds: float = seconds % SECS_PER_MIN
    return f"{hours:03d}:{minutes:02d}:{remaining_seconds:02.0f}"


def ingest_jsonl_url(url: str,
                     conn: sqlite3.Connection,
                     log_work: bool = False,
                     total_size: int = None,
                     insrt_missing_taxa: bool = False):
    if log_work:
        chunk_ctr = 1
    for chunk in pd.read_json(url,
                              lines=True,
                              chunksize=CHUNK_SIZE):
        try:
            end_str = "" if log_work else "\n"
            print(f"  Loading chunk {chunk_ctr}", end=end_str)
            conn.execute("PRAGMA synchronous = OFF;")
            conn.execute("BEGIN TRANSACTION;")
            if log_work and chunk_ctr == 1:
                start_time = time.time()
            ingest_nodenorm_jsonl_chunk(chunk,
                                        conn,
                                        insrt_missing_taxa=insrt_missing_taxa)
            conn.commit()

            if log_work:
                sub_end_str = "" if total_size is not None else "\n"
                elapsed_time = (time.time() - start_time)
                elapsed_time_str = convert_seconds(elapsed_time)
                print(f"; time spent on URL: {elapsed_time_str}",
                      end=sub_end_str)
                if total_size is not None:
                    if chunk_ctr == 1:
                        chunk_size = byte_count_df(chunk)
                        num_chunks = math.ceil(total_size / chunk_size)
                    pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
                    time_to_complete = elapsed_time * \
                        (100.0 - pct_complete)/pct_complete
                    time_to_complete_str = convert_seconds(time_to_complete)
                    print(f"; {pct_complete:0.2f}% complete"
                          f"; time to complete: {time_to_complete_str}")
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.execute("PRAGMA synchronous = FULL;")
        chunk_ctr += 1


def create_indices(conn: sqlite3.Connection,
                   log_work: bool = False,
                   print_ddl: io.TextIOBase = None):
    work_plan = (('cliques',                  'type_id'),
                 ('cliques',                  'primary_identifier_id'),
                 ('identifiers_descriptions', 'description_id'),
                 ('identifiers_descriptions', 'identifier_id'),
                 ('identifiers_cliques',      'identifier_id'),
                 ('identifiers_cliques',      'clique_id'),
                 ('identifiers_taxa',         'identifier_id'),
                 ('identifiers_taxa',         'taxa_identifier_id'))

    for table, col in work_plan:
        create_index(table, col, conn, log_work, print_ddl)


def do_ingest(babel_compendia_url: str,
              database_file_name: str,
              test_type: int = None,
              test_file: str = None,
              log_work: bool = False,
              print_ddl: io.TextIOBase = None):
    start_time_sec = time.time()
    date_time_local = cur_datetime_local().isoformat()
    print(f"Starting database ingest at: {date_time_local}")
    with create_empty_database(database_file_name,
                               log_work=log_work) as conn:
        ingest_biolink_categories(get_biolink_categories(log_work),
                                  conn,
                                  log_work)
        create_indices(conn, log_work)
        if test_type == 1:
            print(f"ingesting file: {test_file}")
            ingest_jsonl_url(test_file,
                             conn=conn,
                             log_work=log_work,
                             insrt_missing_taxa=True)
        elif test_type == 2:
            # after ingesting Biolink categories, need to ingest OrganismTaxon
            # first!
            for file_prefix in ('OrganismTaxon',
                                'ComplexMolecularMixture',
                                'Polypeptide',
                                'PhenotypicFeature'):
                print(f"ingesting file: {file_prefix}")
                ingest_jsonl_url(babel_compendia_url +
                                 file_prefix + ".txt",
                                 conn=conn,
                                 log_work=log_work)
        else:
            assert test_type is None, f"invalid test_type: {test_type}"
            start_time = time.time()
            _, listing = htmllistparse.fetch_listing(babel_compendia_url)
            files_info = {list_entry.name: list_entry.size
                          for list_entry in listing}
            pruned_listing = tuple(li for li in files_info.keys()
                                   if li.endswith('.txt'))
            sorted_listing = sorted(pruned_listing,
                                    key=lambda i: 0 if i == TAXON_FILE else 1)
            print(f"ingesting compendia files at: {babel_compendia_url}")
            for file_name in sorted_listing:
                cur_time = time.time()
                elapsed_time_str = convert_seconds(cur_time - start_time)
                print(f"ingesting file: {file_name} "
                      f"size: {files_info[file_name]} bytes; "
                      f"elapsed: {elapsed_time_str}")
                ingest_jsonl_url(babel_compendia_url +
                                 file_name,
                                 conn=conn,
                                 log_work=log_work,
                                 total_size=files_info[file_name],
                                 insrt_missing_taxa=True)
    date_time_local = cur_datetime_local().isoformat()
    print(f"Finished database ingest at: {date_time_local}")
    elapsed_time_str = convert_seconds(time.time() - start_time_sec)
    print(f"Elapsed time for Babel ingest: {elapsed_time_str} (HHH:MM::SS)")


do_ingest(BABEL_COMPENDIA_URL,
          DATABASE_FILE_NAME,
          TEST_TYPE,
          TEST_FILE,
          LOG_WORK)
