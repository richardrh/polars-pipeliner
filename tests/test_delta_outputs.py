from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from polars_pipeliner import DiscoveryError, discover

pytest.importorskip("deltalake")

SCHEMA = "pl.Schema({'id': pl.Int64, 'value': pl.Int64})"


def write(root: Path, relative: str, contents: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def write_pipeline(
    root: Path,
    rows: dict[str, list[int]],
    output: str,
    *,
    schema: str = SCHEMA,
) -> None:
    write(
        root,
        "sources/rows.py",
        f"""import polars as pl
from polars_pipeliner import SourceModel
SCHEMA = {schema}
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
SCHEMA = {schema}
class Rows(MartModel):
    rows = Input('sources.rows', schema=SCHEMA)
    output_schema = SCHEMA
    output = {output}
    def transform(self, rows: pl.LazyFrame) -> pl.LazyFrame:
        return rows
""",
    )


def rows_at(target: Path) -> list[dict[str, int]]:
    return pl.read_delta(target).sort("id").to_dicts()


def test_delta_append_and_overwrite_round_trip(tmp_path: Path) -> None:
    initial_root = tmp_path / "initial"
    target = tmp_path / "warehouse/orders"
    write_pipeline(
        initial_root,
        {"id": [1, 2], "value": [10, 20]},
        f"Output.delta({str(target)!r})",
    )

    assert discover(initial_root).run() == {"marts.rows": target}
    assert rows_at(target) == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
    ]

    write_pipeline(
        tmp_path / "append",
        {"id": [3], "value": [30]},
        f"Output.delta({str(target)!r}, mode='append')",
    )
    discover(tmp_path / "append").run()
    assert rows_at(target) == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
    ]

    write_pipeline(
        tmp_path / "overwrite",
        {"id": [4], "value": [40]},
        f"Output.delta({str(target)!r}, mode='overwrite')",
    )
    discover(tmp_path / "overwrite").run()
    assert rows_at(target) == [{"id": 4, "value": 40}]


def test_delta_upsert_updates_and_inserts_rows(tmp_path: Path) -> None:
    initial_root = tmp_path / "initial"
    target = tmp_path / "warehouse/orders"
    write_pipeline(
        initial_root,
        {"id": [1, 2], "value": [10, 20]},
        f"Output.delta({str(target)!r})",
    )
    discover(initial_root).run()

    write_pipeline(
        tmp_path / "upsert",
        {"id": [2, 3], "value": [25, 30]},
        f"Output.delta_upsert({str(target)!r}, keys=('id',))",
    )

    assert discover(tmp_path / "upsert").run() == {"marts.rows": target}
    assert rows_at(target) == [
        {"id": 1, "value": 10},
        {"id": 2, "value": 25},
        {"id": 3, "value": 30},
    ]


def test_delta_upsert_supports_composite_keys(tmp_path: Path) -> None:
    schema = "pl.Schema({'id': pl.Int64, 'line': pl.Int64, 'value': pl.Int64})"
    target = tmp_path / "warehouse/order-lines"
    write_pipeline(
        tmp_path / "initial",
        {"id": [1, 1], "line": [1, 2], "value": [10, 20]},
        f"Output.delta({str(target)!r})",
        schema=schema,
    )
    discover(tmp_path / "initial").run()

    write_pipeline(
        tmp_path / "upsert",
        {"id": [1, 1], "line": [2, 3], "value": [25, 30]},
        f"Output.delta_upsert({str(target)!r}, keys=('id', 'line'))",
        schema=schema,
    )
    discover(tmp_path / "upsert").run()

    assert pl.read_delta(target).sort(["id", "line"]).to_dicts() == [
        {"id": 1, "line": 1, "value": 10},
        {"id": 1, "line": 2, "value": 25},
        {"id": 1, "line": 3, "value": 30},
    ]


def test_delta_upsert_keys_must_exist_in_output_schema(tmp_path: Path) -> None:
    root = tmp_path / "project"
    target = tmp_path / "warehouse/orders"
    write_pipeline(
        root,
        {"id": [1], "value": [10]},
        f"Output.delta_upsert({str(target)!r}, keys=('missing',))",
    )

    with pytest.raises(
        DiscoveryError,
        match="Delta upsert key.*missing from output_schema",
    ):
        discover(root)
