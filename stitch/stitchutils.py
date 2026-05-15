"""
Utility functions for working with Biolink Model categories, argument parsing,
time formatting, and file/URL text streaming.

This module provides:

- **Biolink utilities**
  - `get_biolink_categories`: Retrieve all Biolink Model categories and the
    model version.

- **Argument parsing**
  - `namespace_to_dict`: Recursively convert an `argparse.Namespace` to a
    standard Python `dict`.

- **Data normalization**
  - `nan_to_none`: Convert NaN floats to `None` for serialization or database
    insertion.

- **Time utilities**
  - `format_time_seconds_to_str`: Convert a floating-point seconds value into
    `HHH:MM:SS` formatted string.

- **Logging**
  - `make_log_print_controller`: Build a timestamped, toggleable stderr
    logger (returns an `(impl, set_enabled)` pair).

- **Iterators and chunking**
  - `chunked`: Yield successive chunks of a specified size from an iterator.

- **URL and file I/O**
  - `url_to_local_path`: Convert `file://` URLs to local filesystem paths.
  - `list_local_directory`: List the files in a `file://` directory URL as
    `FileEntry` items.
  - `list_dir`: Return a directory listing for either an HTTP(S) index URL
    (via `htmllistparse.fetch_listing`) or a `file://` URL.
  - `read_line_chunks_from_url`: Download (or open) a URL and yield its
    lines in fixed-size chunks.

Constants
---------
CONFLATION_TYPE_NAMES_IDS : dict[str, int]
    Mapping of conflation type names to integer IDs.

SECS_PER_MIN : int
    Number of seconds in one minute (60).
SECS_PER_HOUR : int
    Number of seconds in one hour (3600).

External dependencies
---------------------
- `bmt` (Biolink Model Toolkit) for querying Biolink classes and version.
- `numpy` for NaN detection.
- `requests` for robust streaming of HTTP(S) resources.

Typical use cases include preparing Biolink category sets, normalizing data
prior to database insertion, chunking large text files for batch ingestion,
and handling both local and remote line-oriented resources in a consistent
way.
"""
import argparse
import itertools
import json
import os
import sys
import tempfile
import time
import urllib
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, TypeVar, Union, cast

import bmt
import numpy as np
import pandas as pd
import requests
from htmllistparse.htmllistparse import FileEntry, fetch_listing
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CONFLATION_TYPE_NAMES_IDS = \
    {'DrugChemical': 1,
     'GeneProtein': 2}

def get_biolink_categories() -> tuple[set[str], str]:
    """
    Return the set of all Biolink Model categories and the model version.

    Uses `bmt.Toolkit` to load the current Biolink Model (from GitHub) and
    returns a tuple of allowed Biolink category identifiers, as CURIEs
    (for example, `biolink:Gene`), as well as a string identifier of the
    current Biolink model version.

    Returns
    -------
    tuple[set[str], str]
        A 2-tuple of:
        - set of category identifiers (formatted strings)
        - Biolink Model version string
    """
    tk = bmt.Toolkit()
    ver = tk.get_model_version()
    return (set(tk.get_all_classes(formatted=True)), ver)


def namespace_to_dict(namespace: argparse.Namespace) -> dict[str, Any]:
    """
    Convert an `argparse.Namespace` (recursively) to a plain `dict`.

    Nested namespaces are converted recursively so the result is JSON-like.

    Parameters
    ----------
    namespace : argparse.Namespace
        The namespace produced by `argparse`.

    Returns
    -------
    dict[str, Any]
        A dictionary with the same keys/values as the namespace, where any
        nested `Namespace` instances are themselves converted to `dict`s.
    """
    return {
        k: namespace_to_dict(v) if isinstance(v, argparse.Namespace) else v
        for k, v in vars(namespace).items()
    }


T = TypeVar("T", bound=object)
def nan_to_none(o: Union[float, T]) -> Union[None, T]:
    """
    Convert floating-point NaN to `None`, otherwise return the input unchanged.

    This is convenient when preparing values for serialization or database
    insertion where NaN is not supported.

    Parameters
    ----------
    o : float | T
        Value to normalize. If `o` is a `float` and `math.isnan(o)` is true,
        `None` is returned; otherwise `o` is returned as-is.

    Returns
    -------
    None | T
        `None` if the input is a NaN float; otherwise the original value.
    """
    if isinstance(o, float) and np.isnan(o):
        return None
    return cast(T, o)

SECS_PER_MIN = 60
SECS_PER_HOUR = 3600

def format_time_seconds_to_str(seconds: float) -> str:
    """
    Format a duration in seconds as `HHH:MM:SS`.

    Hours are zero-padded to width 3, minutes to 2, and seconds are rounded
    to the nearest integer and zero-padded to 2.

    Parameters
    ----------
    seconds : float
        Duration in seconds (must be non-negative).

    Returns
    -------
    str
        A string like `"003:07:05"` for 3 hours, 7 minutes, 5 seconds.

    Raises
    ------
    ValueError
        If `seconds` is negative.
    """
    if seconds < 0:
        raise ValueError(f"invalid value for parameter 'seconds': {seconds}; cannot be negative")
    hours: int = int(seconds // SECS_PER_HOUR)
    minutes: int = int((seconds % SECS_PER_HOUR) // SECS_PER_MIN)
    remaining_seconds: float = seconds % SECS_PER_MIN
    return f"{hours:03d}:{minutes:02d}:{remaining_seconds:02.0f}"

def chunked(iterator: Iterator[str], size: int) -> Iterable[list[str]]:
    """
    Yield successive lists of up to `size` lines from an iterator.

    The final chunk may contain fewer than `size` elements if the iterator
    is exhausted.

    Parameters
    ----------
    iterator : Iterator[str]
        Source of lines/items to be chunked.
    size : int
        Desired chunk size (must be >= 1).

    Yields
    ------
    list[str]
        Consecutive chunks of items from the iterator.

    Raises
    ------
    ValueError
        If `size` is less than 1.
    """
    if size < 1:
        raise ValueError(f"invalid value for parameter 'size': {size}")
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            break
        yield chunk

def _download_to_temp_file(url: str, temp_dir: Path) -> Path:
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Named temp file so we can reopen it later
    with tempfile.NamedTemporaryFile(
        mode="wb",
        delete=False,
        dir=temp_dir,
        prefix="jsonl_",
        suffix=".tmp",
    ) as tmp_file:

        temp_path = Path(tmp_file.name)

        with _make_session().get(url, stream=True, timeout=(30, 600)) as resp:
            resp.raise_for_status()

            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp_file.write(chunk)

    return temp_path

def url_to_local_path(url: str) -> str:
    """
    Convert a `file://` URL into a local filesystem path.

    Handles forms like `file:///path/to/file` on Unix and `file://host/path`
    (combining netloc and path when needed). Percent-encoded characters are
    unquoted.

    Parameters
    ----------
    url : str
        A URL beginning with `file://`.

    Returns
    -------
    str
        The corresponding local filesystem path.

    Raises
    ------
    ValueError
        If `url` is not a `file://` URL.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == 'file':
        # Combine netloc and path (for local files, netloc is often empty on Unix)
        if parsed.netloc and parsed.path:
            path = f"/{parsed.netloc}{parsed.path}"
        elif parsed.netloc:
            path = parsed.netloc
        else:
            path = parsed.path
        return urllib.parse.unquote(path)
    raise ValueError(f"Not a file:// URL: {url}")

def list_local_directory(file_url: str) -> list[FileEntry]:
    """
    List the regular files in a directory referenced by a ``file://`` URL.

    Parallel to :func:`htmllistparse.fetch_listing` for the limited subset
    of fields the rest of the package reads (``name``, ``size``; ``mtime``
    is taken from ``stat()``).  Subdirectories and non-file entries are
    skipped.

    Parameters
    ----------
    file_url : str
        A URL beginning with ``file://`` pointing at a directory.

    Returns
    -------
    list[FileEntry]
        One :class:`htmllistparse.htmllistparse.FileEntry` per regular file
        in the directory, sorted by path.

    Raises
    ------
    ValueError
        If ``file_url`` is not a ``file://`` URL (propagated from
        :func:`url_to_local_path`).
    """
    directory = Path(url_to_local_path(file_url))
    entries: list[FileEntry] = []
    for child in sorted(directory.iterdir()):
        if not child.is_file():
            continue
        stat = child.stat()
        entries.append(FileEntry(child.name, stat.st_mtime, stat.st_size, "file"))
    return entries

def list_dir(index_url: str) -> list[FileEntry]:
    """
    Return the directory listing for ``index_url``.

    HTTP(S) URLs are scraped via :func:`htmllistparse.fetch_listing`;
    ``file://`` URLs are read locally via :func:`list_local_directory`.
    """
    if index_url.startswith("file://"):
        return list_local_directory(index_url)
    _, listing = fetch_listing(index_url)
    return listing

def read_line_chunks_from_url(
        url: str, chunk_size: int, temp_dir: Path
) -> Iterator[list[str]]:
    """
    Read lines from a URL and yield them in fixed-size chunks.

    For `file://` URLs the referenced local file is read directly. For
    HTTP(S) URLs the content is first downloaded to a temporary file in
    `temp_dir`, and that file is deleted after iteration completes (or on
    error). Yielded lines have their trailing newline stripped.

    Parameters
    ----------
    url : str
        A `file://` URL or HTTP(S) URL to read from.
    chunk_size : int
        The number of lines per chunk (must be >= 1).
    temp_dir : Path
        Directory in which to place the downloaded temp file (HTTP(S) only).

    Yields
    ------
    list[str]
        Consecutive chunks of up to `chunk_size` lines (the last chunk may
        be smaller).
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    if url.startswith("file://"):
        path = Path(url_to_local_path(url))
        with path.open("r", encoding="utf-8") as f:
            yield from chunked((line.rstrip("\n") for line in f), chunk_size)
        return

    temp_path = _download_to_temp_file(url, temp_dir)
    try:
        with temp_path.open("r", encoding="utf-8") as f:
            yield from chunked((line.rstrip("\n") for line in f), chunk_size)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception:
            # Don't let cleanup errors mask real failures
            pass

def log_start_of_file(start: float, filetype: str, filename: str, filesize: int):
    elapsed = format_time_seconds_to_str(time.time() - start)
    return (f"at elapsed time: {elapsed}; "
            f"starting ingest of {filetype} "
            f"file: {filename}; "
            f"file size: {filesize} bytes")

def merge_ints_to_str(t: Iterable[int], delim: str) -> str:
    return delim.join(map(str, t))

def validate_curie_list(curie_list: list[str]) -> None:
    if not isinstance(curie_list, (list, tuple)):
        raise ValueError("expected list/tuple")
    if not all(isinstance(x, str) for x in curie_list):
        raise ValueError("expected all strings")
    if not curie_list:
        raise ValueError("empty curie_list")

def _cur_datetime_local_no_ms() -> datetime:  # does not return microseconds
    return datetime.now().astimezone().replace(microsecond=0)

def cur_datetime_local_str() -> str:
    return _cur_datetime_local_no_ms().isoformat()

type LogPrintImpl = Callable[[str, str], None]
type SetEnabled = Callable[[bool], None]

def make_log_print_controller() -> tuple[LogPrintImpl, SetEnabled]:
    """
    Build a timestamped, toggleable stderr logger.

    Returns a `(impl, set_enabled)` pair: `impl(message, end)` prints
    `"{ISO-local-datetime}: {message}"` to stderr (flushed) iff logging is
    currently enabled; `set_enabled(bool)` flips that flag. Logging starts
    disabled.
    """
    state: dict[str, bool] = {"enabled": False}
    def impl(message: str, end: str = "\n") -> None:
        if state["enabled"]:
            date_time_local = cur_datetime_local_str()
            print(f"{date_time_local}: {message}",
                  end=end, file=sys.stderr, flush=True)
    def set_enabled(enabled: bool) -> None:
        state["enabled"] = enabled
    return impl, set_enabled

def _make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def read_json_lines_from_url(
    url: str,
    lines_per_chunk: int,
    temp_dir: Path
) -> Iterable[pd.DataFrame]:
    """
    Yield a JSON-lines file in pandas DataFrame chunks.

    For `file://` URLs the referenced local file is read directly. For
    HTTP(S) URLs the content is first downloaded to a temporary file in
    `temp_dir`, and that file is deleted after iteration completes (or
    on error).
    """
    if lines_per_chunk <= 0:
        raise ValueError("lines_per_chunk must be > 0")

    if url.startswith("file://"):
        path_to_read = Path(url_to_local_path(url))
        cleanup_path: Path | None = None
    else:
        path_to_read = _download_to_temp_file(url, temp_dir)
        cleanup_path = path_to_read

    try:
        chunk_records: list[dict] = []

        with path_to_read.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    chunk_records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    preview = line[:200]
                    raise ValueError(
                        f"Invalid JSON on line {line_num} in {url}: {exc}; "
                        f"line preview: {preview!r}"
                    ) from exc

                if len(chunk_records) >= lines_per_chunk:
                    yield pd.DataFrame.from_records(chunk_records)
                    chunk_records.clear()

        if chunk_records:
            yield pd.DataFrame.from_records(chunk_records)

    finally:
        # Only the downloaded temp file is ours to delete; file:// inputs stay.
        if cleanup_path is not None:
            try:
                cleanup_path.unlink(missing_ok=True)
            except Exception:
                # Don’t let cleanup errors mask real failures
                pass
