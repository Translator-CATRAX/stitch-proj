import os
import sqlite3

from stitch import ingest_babel


def test_create_empty_database():
    c = ingest_babel._create_empty_database("tests/test.sqlite",
                                            print_ddl_file_obj=None)
    assert type(c) is sqlite3.Connection
    c.close()
    os.unlink("tests/test.sqlite")


def test_create_indices():
    c = ingest_babel._create_empty_database("tests/test.sqlite",
                                            print_ddl_file_obj=None)
    ingest_babel._create_indices(c, print_ddl_file_obj=None)
    c.close()
    os.unlink("tests/test.sqlite")
