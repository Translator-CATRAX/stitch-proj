"""
Pytest configuration for the test suite.

This conftest.py file sets the multiprocessing start method to
"spawn" (or "forkserver") for all tests, ensuring compatibility
across different platforms (Linux, macOS, Windows). The fixture
is session-scoped and autouse, so it is applied automatically.
"""
import multiprocessing as mp

import pytest


@pytest.fixture(scope="session", autouse=True)  # noqa
def _mp_start_method_for_tests():
    try:
        mp.set_start_method("spawn", force=True)  # or "forkserver"
    except RuntimeError:
        pass
