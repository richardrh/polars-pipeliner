from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
import pytest

from polars_pipeliner import discover

boto3 = pytest.importorskip("boto3")

ENDPOINT = os.environ.get("POLARS_PIPELINER_S3_ENDPOINT")
pytestmark = pytest.mark.skipif(
    ENDPOINT is None,
    reason="POLARS_PIPELINER_S3_ENDPOINT is not configured",
)

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


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> tuple[str, str, dict[str, str]]:
    assert ENDPOINT is not None
    access_key = "test-access-key"
    secret_key = "test-secret-key"
    region = "us-east-1"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", access_key)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret_key)
    monkeypatch.setenv("AWS_DEFAULT_REGION", region)
    bucket = f"polars-pipeliner-{uuid4().hex}"
    client = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    client.create_bucket(Bucket=bucket)
    return (
        ENDPOINT,
        bucket,
        {
            "endpoint_url": ENDPOINT,
            "allow_http": "true",
        },
    )


@pytest.mark.parametrize(
    ("factory", "extension", "reader"),
    [
        ("parquet", "parquet", "scan_parquet"),
        ("csv", "csv", "scan_csv"),
        ("ipc", "arrow", "scan_ipc"),
        ("ndjson", "ndjson", "scan_ndjson"),
    ],
)
def test_file_output_round_trip_on_s3(
    tmp_path: Path,
    s3: tuple[str, str, dict[str, str]],
    factory: str,
    extension: str,
    reader: str,
) -> None:
    _, bucket, options = s3
    uri = f"s3://{bucket}/marts/orders.{extension}"
    root = tmp_path / "project"
    write_pipeline(
        root,
        {"id": [1, 2], "value": [10, 20]},
        f"Output.{factory}({uri!r}, storage_options={options!r})",
    )

    assert discover(root).run() == {"marts.rows": uri}
    assert getattr(pl, reader)(uri, storage_options=options).collect().sort(
        "id"
    ).to_dicts() == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ]


def test_delta_append_and_upsert_on_s3(
    tmp_path: Path, s3: tuple[str, str, dict[str, str]]
) -> None:
    pytest.importorskip("deltalake")
    _, bucket, options = s3
    uri = f"s3://{bucket}/delta/orders"
    write_pipeline(
        tmp_path / "initial",
        {"id": [1, 2], "value": [10, 20]},
        f"Output.delta({uri!r}, storage_options={options!r})",
    )
    discover(tmp_path / "initial").run()

    write_pipeline(
        tmp_path / "upsert",
        {"id": [2, 3], "value": [25, 30]},
        f"Output.delta_upsert({uri!r}, keys=('id',), storage_options={options!r})",
    )
    discover(tmp_path / "upsert").run()

    assert pl.read_delta(uri, storage_options=options).sort("id").to_dicts() == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 25},
        {"id": 3, "value": 30},
    ]


def test_iceberg_append_on_s3_warehouse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    s3: tuple[str, str, dict[str, str]],
) -> None:
    pytest.importorskip("pyarrow")
    iceberg_catalog = pytest.importorskip("pyiceberg.catalog")
    schema_module = pytest.importorskip("pyiceberg.schema")
    types_module = pytest.importorskip("pyiceberg.types")
    sql_module = pytest.importorskip("pyiceberg.catalog.sql")
    endpoint, bucket, _ = s3
    iceberg_options = {
        "s3.endpoint": endpoint,
        "s3.access-key-id": "test-access-key",
        "s3.secret-access-key": "test-secret-key",
        "s3.region": "us-east-1",
        "s3.force-virtual-addressing": "false",
        "allow_http": "true",
    }
    catalog = sql_module.SqlCatalog(
        "test",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"s3://{bucket}/iceberg",
        **iceberg_options,
    )
    catalog.create_namespace("analytics")
    catalog.create_table(
        "analytics.orders",
        schema=schema_module.Schema(
            types_module.NestedField(1, "id", types_module.LongType(), required=False),
            types_module.NestedField(
                2, "value", types_module.LongType(), required=False
            ),
        ),
    )

    def load_catalog(name: str) -> Any:
        assert name == "test"
        return catalog

    monkeypatch.setattr(iceberg_catalog, "load_catalog", load_catalog)
    write_pipeline(
        tmp_path / "project",
        {"id": [1, 2], "value": [10, 20]},
        "Output.iceberg('analytics.orders', catalog='test')",
    )

    assert discover(tmp_path / "project").run() == {
        "marts.rows": "test:analytics.orders"
    }
    table = catalog.load_table("analytics.orders")
    assert pl.scan_iceberg(table, storage_options=iceberg_options).collect().sort(
        "id"
    ).to_dicts() == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ]
