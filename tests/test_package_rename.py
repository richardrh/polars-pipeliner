from __future__ import annotations

import importlib


def test_renamed_package_is_importable() -> None:
    package = importlib.import_module("polars_pipeliner")

    assert package.__name__ == "polars_pipeliner"
