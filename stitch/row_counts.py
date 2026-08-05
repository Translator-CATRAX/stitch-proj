#!/usr/bin/env python3.12
"""
row_counts.py

A command-line utility for printing the row counts of all tables in a
local Babel SQLite database.

This script connects to a specified SQLite database file, queries the
row counts of every table (via the `local_babel.get_table_row_counts`
function), and pretty-prints the results. By default, the database
filename is assumed to be `babel.sqlite`, but a different filename can
be supplied as a positional argument.

Usage:
    python row_counts.py [--pretty] babel.sqlite

Arguments:
    filename (str): Path to the local Babel SQLite database.

Options:
    --pretty: print the counts as an aligned table, with commas as
      thousands-place separators.

Dependencies:
    - local_babel: provides `get_table_row_counts`, which returns a
      dictionary of table names mapped to their row counts.
    - stitchutils (as `su`): provides `namespace_to_dict` for parsing
      command-line arguments into keyword arguments.

Example:
    $ python row_counts.py babel.sqlite
    {'nodes': 103442,
     'edges': 284390,
     'cliques': 9021,
     ...}

    $ python row_counts.py --pretty babel.sqlite
    nodes      103,442
    edges      284,390
    cliques      9,021
    ...
"""
import argparse
import pprint

from stitch import local_babel
from stitch import stitchutils as su


def _get_args() -> argparse.Namespace:
    arg_parser = argparse.ArgumentParser(description='row_counts.py: '
                                         'print the row counts of all '
                                         'tables in the local Babel '
                                         'sqlite database')
    arg_parser.add_argument('filename',
                            type=str,
                            default='babel.sqlite',
                            help='the local Babel sqlite database, like babel.sqlite')
    arg_parser.add_argument('--pretty', dest='pretty', default=False,
                            action='store_true',
                            help='print the row counts as an aligned table, '
                            'with commas as thousands-place separators')
    return arg_parser.parse_args()

def _print_pretty(row_counts: dict[str, int]):
    if not row_counts:
        return
    name_width = max(map(len, row_counts.keys()))
    count_width = max(len(f"{count:,}") for count in row_counts.values())
    for name, count in row_counts.items():
        print(f"{name:<{name_width}}  {count:>{count_width},}")

def _main(filename: str, pretty: bool):
    with local_babel.connect_to_db_read_only(filename) as conn:
        row_counts = local_babel.get_table_row_counts(conn)
    if pretty:
        _print_pretty(row_counts)
    else:
        pprint.pprint(row_counts)

if __name__ == "__main__":
    _main(**su.namespace_to_dict(_get_args()))
