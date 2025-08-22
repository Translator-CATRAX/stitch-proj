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

- **Iterators and chunking**
  - `chunked`: Yield successive chunks of a specified size from an iterator.

- **URL and file I/O**
  - `url_to_local_path`: Convert `file://` URLs to local filesystem paths.
  - `get_lines_from_url`: Yield text lines from a local file or remote URL.
  - `get_line_chunks_from_url`: Convenience wrapper to return line chunks
    directly from a URL.

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
import tempfile
import urllib
from typing import Any, Iterable, Iterator, TypeVar, Union, cast

import bmt
import numpy as np
import requests

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

def get_lines_from_url(url_or_path: str) -> Iterator[str]:
    """
    Stream text lines from a `file://` URL or HTTP(S) URL.

    For `file://` URLs, this opens the referenced local file. For remote
    URLs, the content is downloaded to a temporary file (to avoid
    `ChunkedEncodingError`) and then read as UTF-8 text.

    Each yielded line has its trailing newline removed.

    Parameters
    ----------
    url_or_path : str
        A `file://` URL for local files, or an HTTP(S) URL compatible with
        `requests.get`.

    Yields
    ------
    str
        Lines of text with no trailing `\\n`.

    Raises
    ------
    requests.exceptions.RequestException
        If the HTTP(S) request fails.
    OSError
        If the local file cannot be opened/read.
    """
    if url_or_path.startswith("file://"):
        path = url_to_local_path(url_or_path)
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                yield line.rstrip('\n')
    else:
        # Download to a temporary file first, to avoid a ChunkedEncodingError
        with tempfile.NamedTemporaryFile(mode='wb+', delete=True) as tmp_file:
            with requests.get(url_or_path, stream=True, timeout=(10, 300)) as response:
                response.raise_for_status()
                for chunk in response.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
            tmp_file.flush()
            tmp_file.seek(0)

            # Now read from the temp file as text
            with open(tmp_file.name, 'r', encoding='utf-8') as f:
                for line in f:
                    yield line.rstrip('\n')

def get_line_chunks_from_url(url: str, chunk_size: int) -> Iterable[list[str]]:
    """
    Read lines from a URL and yield them in fixed-size chunks.

    This is a convenience wrapper over `get_lines_from_url` + `chunked`.

    Parameters
    ----------
    url : str
        A `file://` URL or HTTP(S) URL to read from.
    chunk_size : int
        The number of lines per chunk (must be >= 1).

    Returns
    -------
    Iterable[list[str]]
        An iterable over lists of lines, where each list contains up to
        `chunk_size` lines (the last chunk may be smaller).
    """
    lines = get_lines_from_url(url)
    return chunked(lines, chunk_size)
