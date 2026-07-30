from __future__ import annotations

import importlib
from importlib.metadata import metadata


def test_renamed_package_is_importable() -> None:
    package = importlib.import_module("polars_pipeliner")

    assert package.__name__ == "polars_pipeliner"


def test_distribution_name_matches_product_name() -> None:
    assert metadata("polars-pipeliner")["Name"] == "polars-pipeliner"
