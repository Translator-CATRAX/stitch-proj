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
    python row_counts.py babel.sqlite

Arguments:
    filename (str): Path to the local Babel SQLite database.

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
"""
import argparse
import pprint
import sqlite3

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
    return arg_parser.parse_args()

def _main(filename: str):
    with sqlite3.Connection(filename) as conn:
        pprint.pprint(local_babel.get_table_row_counts(conn))

if __name__ == "__main__":
    _main(**su.namespace_to_dict(_get_args()))
