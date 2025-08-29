#!/usr/bin/env python3
"""
Scrape an Apache-style HTML index page, find all *.txt files, download only the
first line from each file, parse that line as JSON, and print the "type" field.

Usage:
    python get_types_from_index.py [INDEX_URL]

If INDEX_URL is omitted, a sensible default is used (the RENCI Stars path
provided in the request).

Notes:
- Only the first non-empty line is read from each *.txt file to minimize data
  transfer.
- If a file's first line isn't valid JSON or doesn't contain "type", it is
  reported and skipped gracefully.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


DEFAULT_INDEX_URL = (
    "https://stars.renci.org/var/babel_outputs/2025mar31/compendia/"
)


@dataclass(frozen=True)
class TxtEntry:
    name: str   # filename as displayed in the index (e.g., "foo.txt")
    url: str    # absolute URL to the file


def fetch_index_html(url: str, timeout: float = 30.0) -> str:
    """Fetch the HTML for the index page."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def parse_txt_links(index_url: str, html: str) -> Iterator[TxtEntry]:
    """
    Parse the index HTML and yield absolute URLs for all links ending in '.txt'.
    """
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # Skip parent directory and non-.txt links
        if not href or href in {"../", "./"}:
            continue
        if href.lower().endswith(".txt"):
            yield TxtEntry(name=href.split("/")[-1], url=urljoin(index_url, href))


def first_nonempty_line(url: str, timeout: float = 30.0) -> Optional[str]:
    """
    Stream the file and return the first non-empty line as a decoded string.
    Returns None if no such line exists.
    """
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue
            line = raw_line.strip()
            if line:
                return line
    return None


def extract_type_from_first_line_json(url: str) -> Optional[str]:
    """
    Read the first non-empty line from the given URL, parse as JSON, and
    return the value of the 'type' field if present, else None.
    """
    line = first_nonempty_line(url)
    if line is None:
        return None
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    value = obj.get("type")
    # Optionally convert non-string types to JSON for display
    if value is None:
        return None
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def main(index_url: str) -> int:
    try:
        html = fetch_index_html(index_url)
    except Exception as e:
        print(f"ERROR: failed to fetch index: {e}", file=sys.stderr)
        return 2

    entries = list(parse_txt_links(index_url, html))
    if not entries:
        print("No .txt files found at the index URL.", file=sys.stderr)
        return 1

    exit_code = 0
    for entry in entries:
        try:
            type_value = extract_type_from_first_line_json(entry.url)
            if type_value is None:
                print(f"{entry.name}\t<MISSING OR INVALID JSON TYPE>")
                exit_code = max(exit_code, 1)
            else:
                print(f"{entry.name}\t{type_value}")
        except Exception as e:
            print(f"{entry.name}\t<ERROR: {e}>", file=sys.stderr)
            exit_code = max(exit_code, 2)

    return exit_code


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INDEX_URL
    sys.exit(main(url))
