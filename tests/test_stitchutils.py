import numpy as np
from stitch import stitchutils as su


def test_nan_to_none():
    assert su.nan_to_none(np.nan) is None
    assert su.nan_to_none(1.0) is not None
    assert su.nan_to_none(np.float64(1.0)) is not None


