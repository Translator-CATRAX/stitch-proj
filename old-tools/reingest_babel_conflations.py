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

DEFAULT_BABEL_RELEASE_URL =  'https://stars.renci.org/var/babel_outputs/2025sep1/'
DEFAULT_BABEL_CONFLATION_URL = urljoin(DEFAULT_BABEL_RELEASE_URL, 'conflation/')
DEFAULT_DATABASE_FILE_NAME = 'babel.sqlite'
DEFAULT_LINES_PER_CHUNK = 100_000
WAL_SIZE = 100_000
CACHE_SIZE = -8_000_000

def _get_args() -> argparse.Namespace:
    arg_parser = \
        argparse.ArgumentParser(description='ingest_babel.py: '
                                'ingest Babel into a sqlite3 database')
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

def _cur_datetime_local_no_ms() -> datetime:  # does not return microseconds
    return datetime.now().astimezone().replace(microsecond=0)

def _cur_datetime_local_str() -> str:
    return _cur_datetime_local_no_ms().isoformat()

type _LogPrintImpl = Callable[[str, str], None]
type SetEnabled = Callable[[bool], None]

_log_print_impl: _LogPrintImpl  # Mutable implementation target (assigned below)

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

# Initialize the implementation and setter once (no globals/reassignment)
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
                      log_work: bool,
                      mandatory_analyze: bool = False):
    if mandatory_analyze:
        command_str = 'ANALYZE'
    else:
        command_str = 'PRAGMA optimize'
    _log_print(f"running {command_str}")
    if log_work:
        analyze_start_time = time.time()
    conn.execute(command_str)
    _log_print(f"completed {command_str}")
    if log_work:
        analyze_end_time = time.time()
        analyze_elapsed_time = \
            su.format_time_seconds_to_str(analyze_end_time -
                                          analyze_start_time)
        _log_print(f"running {command_str} took: {analyze_elapsed_time} "
                   "(HHH:MM::SS)")

def _set_auto_vacuum(conn: sqlite3.Connection,
                     auto_vacuum_on: bool):
    switch_str = 'FULL' if auto_vacuum_on else 'NONE'
    _log_print(f"setting auto_vacuum to {switch_str}")
    conn.execute(f"PRAGMA auto_vacuum={switch_str};")

def _merge_ints_to_str(t: Iterable[int], delim: str) -> str:
    return delim.join(map(str, t))

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
        is_canonical INTEGER NOT NULL CHECK (is_canonical in (1, 0)),
        FOREIGN KEY(cluster_id) REFERENCES conflation_clusters(id),
        FOREIGN KEY(identifier_id) REFERENCES identifiers(id),
        UNIQUE(cluster_id, identifier_id));
    '''

SQL__CREATE_INDEX_WORK_PLAN = (
    ('conflation_members',       'identifier_id'),
    ('conflation_clusters',      'type')
)

def _create_missing_tables(database_file_name: str,
                           print_ddl_file_obj: IO[str] | None = None) -> \
                           sqlite3.Connection:
    conn = sqlite3.connect(database_file_name)
    _set_auto_vacuum(conn, auto_vacuum_on=False)
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS conflation_members;")
    cur.execute("DROP TABLE IF EXISTS conflation_clusters;")
    table_creation_statements = (
        ('conflation_clusters', SQL_CREATE_TABLE_CONFLATION_CLUSTERS),
        ('conflation_members', SQL_CREATE_TABLE_CONFLATION_MEMBERS)
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

def _run_vacuum(conn: sqlite3.Connection):
    _log_print("starting database VACUUM")
    conn.execute("VACUUM;")
    _log_print("completed database VACUUM")

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

def _make_conflation_chunk_processor(conn: sqlite3.Connection,
                                     conflation_type_id: int) -> Callable:
    if conflation_type_id not in ALLOWED_CONFLATION_TYPES:
        raise ValueError(f"invalid conflation_type value: {conflation_type_id};"
                         "it must be in the set: {ALLOWED_CONFLATION_TYPES}")
    def process_conflation_chunk(chunk: Iterable[str]):
        cursor = conn.cursor()
        # Create temp table once per connection (outside loop if possible)
        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_curie_list (
                curie TEXT PRIMARY KEY,
                ord   INTEGER NOT NULL
            ) WITHOUT ROWID;
        """)

        for line in chunk:
            curie_list = ast.literal_eval(line)
            if not isinstance(curie_list, (list, tuple)):
                raise ValueError("expected list/tuple")
            if not all(isinstance(x, str) for x in curie_list):
                raise ValueError("expected all strings")

            if not curie_list:
                raise ValueError("empty curie_list")

            curie_list_set = set(curie_list)
            if len(curie_list_set) != len(curie_list):
                first_curie = curie_list[0]
                curie_list = [first_curie] + list(curie_list_set - {first_curie})

            canonical_curie = curie_list[0]
            n = len(curie_list)

            cluster_id = _insert_and_return_id(
                cursor,
                "INSERT INTO conflation_clusters (type) VALUES (?) RETURNING id;",
                (conflation_type_id,),
            )

            # Clear from previous iteration
            cursor.execute("DELETE FROM tmp_curie_list;")

            # Bulk insert current cluster
            cursor.executemany(
                "INSERT INTO tmp_curie_list (curie, ord) VALUES (?, ?);",
                ((curie, i) for i, curie in enumerate(curie_list))
            )

            # Join against identifiers and preserve order
            query = """
                SELECT t.curie, i.id
                FROM tmp_curie_list AS t
                JOIN identifiers AS i
                  ON i.curie = t.curie
                ORDER BY t.ord;
            """

            rows = cursor.execute(query).fetchall()

            curies_found = {curie for (curie, _) in rows}

            if canonical_curie not in curies_found:
                raise ValueError("canonical CURIE not found in identifiers: "
                                 f"{canonical_curie}")

            if len(rows) != n:
                missing = set(curie_list) - curies_found
                raise ValueError(
                    "missing CURIE(s) in identifiers; the first 10 (or fewer) "
                    f"identifiers are: {sorted(missing)[:10]}"
                )

            insert_data = tuple(
                (cluster_id, curie_id, 1 if curie == canonical_curie else 0)
                for (curie, curie_id) in rows
            )

            cursor.executemany(
                "INSERT INTO conflation_members "
                "(cluster_id, identifier_id, is_canonical) "
                "VALUES (?, ?, ?);",
                insert_data,
            )

    return process_conflation_chunk

def _read_conflation_chunks(url: str,
                            lines_per_chunk: int) -> Iterable[list[str]]:
    return su.get_line_chunks_from_url(url, lines_per_chunk)

def _do_log_work(start_times: tuple[float, float],
                 progress_info: tuple[int, int],
                 job_info: tuple[Optional[int], Optional[int]]):
    num_chunks, total_size = job_info
    chunk_ctr, glbl_chnk_cnt_start = progress_info
    log_str = f"Loading chunk {chunk_ctr}"
    elapsed_time = time.time() - start_times[0]
    elapsed_time_str = su.format_time_seconds_to_str(elapsed_time)
    chunk_elapsed_time_str = su.format_time_seconds_to_str(time.time() -
                                                           start_times[1])
    log_str += ("; total chunks processed: "
                f"{glbl_chnk_cnt_start + chunk_ctr}"
                f"; time spent on URL: {elapsed_time_str}; "
                f"spent on chunk: {chunk_elapsed_time_str}")
    if total_size is not None and num_chunks is not None:
        pct_complete: float
        if num_chunks > 0:
            pct_complete = min(100.0, 100.0 * (chunk_ctr / num_chunks))
            pct_complete_str = f"{pct_complete:0.2f}"
        else:
            pct_complete_str = "UNKNOWN"
            pct_complete = 0.0
        if pct_complete > 0.0:
            time_to_complete = elapsed_time * \
                (100.0 - pct_complete)/pct_complete
            time_to_complete_str = \
                su.format_time_seconds_to_str(time_to_complete)
        else:
            time_to_complete_str = "UNKNOWN"
        log_str += (f"; URL {pct_complete_str}% complete"
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
                chunk_start_time = time.time()
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
                    _do_log_work((start_time, chunk_start_time),
                                 (chunk_ctr, glbl_chnk_cnt),
                                 (num_chunks, total_size))
                if any(chunk_ctr + glbl_chnk_cnt == chunks_per_analyze \
                       for chunks_per_analyze in chunks_per_analyze_list):
                    _do_index_analyze(conn, log_work, True)
            except Exception as e:
                conn.rollback()
                raise e
        return chunk_ctr + glbl_chnk_cnt
    return ingest_from_url

def _create_missing_indices(
        conn: sqlite3.Connection,
        print_ddl_file_obj: IO[str] | None = None
):
    for table, col in SQL__CREATE_INDEX_WORK_PLAN:
        _create_index(table, col, conn, print_ddl_file_obj)
    statement = \
        """
        CREATE UNIQUE INDEX one_canonical_per_cluster
        ON conflation_members(cluster_id)
        WHERE is_canonical = 1;
        """
    conn.execute(statement)

def _prune_conflation_files(file_list: list[FileEntry]) ->\
        tuple[tuple[str, ...],
              dict[str, FileEntry]]:
    pairs: tuple[tuple[str, FileEntry], ...] = tuple(
        (fe.name, fe) for fe in file_list if fe.name.endswith(CONFLATION_FILE_SUFFIX))
    name_tuple, entry_tuple = \
        cast(tuple[tuple[str, ...], tuple[FileEntry, ...]],
             tuple(zip(*pairs)) if pairs else ((), ()))
    return name_tuple, dict(zip(name_tuple, entry_tuple))

def _get_conflation_files(conflation_files_index_url: str) ->\
        tuple[tuple[str, ...], dict[str, FileEntry]]:
    _, conflation_listing = fetch_listing(conflation_files_index_url)
    conflation_pruned_files, conflation_map_names = \
        _prune_conflation_files(conflation_listing)
    conflation_sorted_files = sorted(conflation_pruned_files)
    return tuple(conflation_sorted_files), conflation_map_names

def _set_pragmas_for_ingestion(conn: sqlite3.Connection,
                               wal_size: int,
                               cache_size: int):
    for s in (
            'synchronous = OFF',
            'journal_mode = WAL',
            'temp_store = MEMORY',
            f'cache_size = {cache_size}',
            f'wal_autocheckpoint = {wal_size}',
            'foreign_keys=ON'
    ):
        conn.execute(f"PRAGMA {s}")
        _log_print(f"setting PRAGMA {s}")

def _set_pragmas_for_querying(conn: sqlite3.Connection):
    for s in ('wal_checkpoint(FULL)',
              'journal_mode = DELETE'):
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
    _do_index_analyze(conn, log_work, True)
    _log_print("setting PRAGMA locking_mode to EXCLUSIVE")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")
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


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-statements
def _main_args(babel_conflation_url: str,
               database_file_name: str,
               lines_per_chunk: int,
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
    _initialize_ray()
    print_ddl_file_obj = sys.stderr if print_ddl else None
    conflation_sorted_files, conflation_map_names = \
        _get_conflation_files(babel_conflation_url)
    start_time_sec = time.time()
    _log_print("Starting conflation ingest")
    ingest_urls = _make_ingest_urls(dry_run)
    with _create_missing_tables(database_file_name,
                                print_ddl_file_obj) as conn:
        _set_pragmas_for_ingestion(conn, WAL_SIZE, CACHE_SIZE)
        do_ingest_conflation_url = \
            _make_url_ingester(conn, lines_per_chunk,
                               _read_conflation_chunks, log_work)
        make_conflation_chunk_processor = \
            functools.partial(_make_conflation_chunk_processor, conn)
        glbl_chnk_cnt = 0
        get_make_chunkproc_args_conflation = \
            functools.partial(_get_make_chunkproc_args_conflation,
                              su.CONFLATION_TYPE_NAMES_IDS)
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
        ingest_args_conflation.update({"glbl_chnk_cnt": glbl_chnk_cnt})
        glbl_chnk_cnt = ingest_urls(**ingest_args_conflation)
        _create_missing_indices(conn, print_ddl_file_obj=print_ddl_file_obj)
        _do_final_cleanup(conn, log_work, glbl_chnk_cnt, start_time_sec)

def main():
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    _main_args(**su.namespace_to_dict(_get_args()))

if __name__ == "__main__":
    main()
