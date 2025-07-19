import argparse
from typing import Any, TypeVar, Union, cast

import bmt
import numpy as np


def get_biolink_categories(log_work: bool = False) -> tuple[str]:
    tk = bmt.Toolkit()
    if log_work:
        ver = tk.get_model_version()
        print(f"loading Biolink model version: {ver}")
    return tuple(tk.get_all_classes(formatted=True))


def namespace_to_dict(namespace: argparse.Namespace) -> dict[str, Any]:
    return {
        k: namespace_to_dict(v) if isinstance(v, argparse.Namespace) else v
        for k, v in vars(namespace).items()
    }


T = TypeVar("T", bound=object)
def nan_to_none(o: Union[float, T]) -> Union[None, T]:
    if isinstance(o, float) and np.isnan(o):
        return None
    return cast(T, o)
