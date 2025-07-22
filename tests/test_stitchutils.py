import argparse

import numpy as np
from stitch import stitchutils as su


def test_nan_to_none():
    assert su.nan_to_none(np.nan) is None
    assert su.nan_to_none(1.0) is not None
    assert su.nan_to_none(np.float64(1.0)) is not None

def test_get_biolink_categories():
    c = su.get_biolink_categories()
    assert 'biolink:Protein' in c

def test_namespace_to_dict():
    ns = argparse.Namespace(foo=1, bar='abc')
    result = su.namespace_to_dict(ns)
    assert result == {'foo': 1, 'bar': 'abc'}

def test_format_time_seconds_to_str():
    assert su.format_time_seconds_to_str(60) == '000:01:00'
    assert su.format_time_seconds_to_str(3660) == '001:01:00'

def test_url_to_local_path():
    assert su.url_to_local_path("file://foo") == "foo"

