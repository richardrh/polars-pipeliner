from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from polars_pipeliner import QueryExecutionError, discover

pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")
iceberg_catalog = pytest.importorskip("pyiceberg.catalog")
Catalog = iceberg_catalog.Catalog
SqlCatalog = pytest.importorskip("pyiceberg.catalog.sql").SqlCatalog
Schema = pytest.importorskip("pyiceberg.schema").Schema
iceberg_types = pytest.importorskip("pyiceberg.types")
LongType = iceberg_types.LongType
NestedField = iceberg_types.NestedField

SCHEMA = "pl.Schema({'id': pl.Int64, 'value': pl.Int64})"


def write(root: Path, relative: str, contents: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def write_pipeline(root: Path, rows: dict[str, list[int]], output: str) -> None:
    write(
        root,
        "sources/rows.py",
        f"""import polars as pl
from polars_pipeliner import SourceModel
SCHEMA = {SCHEMA}
class Rows(SourceModel):
    output_schema = SCHEMA
    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({rows!r}, schema=SCHEMA)
""",
    )
    write(
        root,
        "marts/rows.py",
        f"""import polars as pl
from polars_pipeliner import Input, MartModel, Output
SCHEMA = {SCHEMA}
class Rows(MartModel):
    rows = Input('sources.rows', schema=SCHEMA)
    output_schema = SCHEMA
    output = {output}
    def transform(self, rows: pl.LazyFrame) -> pl.LazyFrame:
        return rows
""",
    )


def configured_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str = "test"
) -> Catalog:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        name,
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{warehouse}",
    )

    def load_catalog(actual: str) -> Catalog:
        assert actual == name
        return catalog

    monkeypatch.setattr(iceberg_catalog, "load_catalog", load_catalog)
    return catalog


def create_orders_table(catalog: Catalog) -> None:
    catalog.create_namespace("analytics")
    catalog.create_table(
        "analytics.orders",
        schema=Schema(
            NestedField(1, "id", LongType(), required=False),
            NestedField(2, "value", LongType(), required=False),
        ),
    )


def rows_in(catalog: Catalog) -> list[dict[str, int]]:
    table = catalog.load_table("analytics.orders")
    return pl.scan_iceberg(table).collect().sort("id").to_dicts()


def test_iceberg_append_and_overwrite_round_trip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    catalog = configured_catalog(tmp_path, monkeypatch)
    create_orders_table(catalog)
    write_pipeline(
        tmp_path / "initial",
        {"id": [1, 2], "value": [10, 20]},
        "Output.iceberg('analytics.orders', catalog='test')",
    )

    assert discover(tmp_path / "initial").run() == {
        "marts.rows": "test:analytics.orders"
    }
    assert rows_in(catalog) == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ]

    write_pipeline(
        tmp_path / "append",
        {"id": [3], "value": [30]},
        "Output.iceberg('analytics.orders', catalog='test', mode='append')",
    )
    discover(tmp_path / "append").run()
    assert rows_in(catalog) == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
    ]

    write_pipeline(
        tmp_path / "overwrite",
        {"id": [4], "value": [40]},
        "Output.iceberg('analytics.orders', catalog='test', mode='overwrite')",
    )
    discover(tmp_path / "overwrite").run()
    assert rows_in(catalog) == [{"id": 4, "value": 40}]


def test_iceberg_catalog_is_only_loaded_during_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "project"
    write_pipeline(
        root,
        {"id": [1], "value": [10]},
        "Output.iceberg('analytics.orders', catalog='unavailable')",
    )
    project = discover(root)

    project.build()
    project.validate()

    with pytest.raises(QueryExecutionError, match="Could not load Iceberg catalog"):
        project.run()


def test_iceberg_missing_table_is_a_structured_run_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configured_catalog(tmp_path, monkeypatch)
    root = tmp_path / "project"
    write_pipeline(
        root,
        {"id": [1], "value": [10]},
        "Output.iceberg('analytics.missing', catalog='test')",
    )

    with pytest.raises(QueryExecutionError, match="analytics.missing"):
        discover(root).run()

    event_names = [
        json.loads(line)["event"]
        for path in (root / "target/runs").glob("*.jsonl")
        for line in path.read_text().splitlines()
    ]
    assert "run_failed" in event_names
    assert "output_written" not in event_names
    assert "run_succeeded" not in event_names
