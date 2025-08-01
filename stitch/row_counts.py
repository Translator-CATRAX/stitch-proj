#!/usr/bin/env python3.12

import argparse
import pprint
import sqlite3

import local_babel
import stitchutils as su


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

def main(filename: str):
    with sqlite3.Connection(filename) as conn:
        pprint.pprint(local_babel.get_table_row_counts(conn))

if __name__ == "__main__":
    main(**su.namespace_to_dict(_get_args()))

