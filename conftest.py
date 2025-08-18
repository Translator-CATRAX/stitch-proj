import multiprocessing as mp
import pytest

@pytest.fixture(scope="session", autouse=True)
def _mp_start_method_for_tests():
    try:
        mp.set_start_method("spawn", force=True)  # or "forkserver"
    except RuntimeError:
        pass

                
