from __future__ import annotations

import sys
from inspect import Parameter, signature
from pathlib import Path
from types import ModuleType
from typing import cast

import polars as pl
import pytest

from polars_pipeliner import (
    CsvSource,
    DiscoveryError,
    ParquetSource,
    QueryBuildError,
    QueryExecutionError,
    QueryMetadata,
    QuerySource,
    QueryValidationError,
    discover,
)


def write_model(root: Path, relative: str, source: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)


def model_source(
    parameters: str = "",
    *,
    inputs: str = "{}",
    body: str = "return pl.LazyFrame({'value': [1]})",
    schema: str = "pl.Schema({'value': pl.Int64})",
) -> str:
    return f"""import polars as pl
from pathlib import Path
from polars_pipeliner import (
    CsvSource,
    ParquetSource,
    PolarsModel,
    QueryMetadata,
    QuerySource,
)
SCHEMA = {schema}
class Model(PolarsModel):
    metadata = QueryMetadata(inputs={inputs}, output_schema=SCHEMA)
    @classmethod
    def transform(cls, {parameters}) -> pl.LazyFrame:
        {body}
"""


def test_metadata_is_immutable_and_typed() -> None:
    inputs = {
        "orders": CsvSource(path="orders.csv", schema=pl.Schema({"id": pl.Int64}))
    }
    metadata = QueryMetadata(inputs=inputs, output_schema=pl.Schema({"id": pl.Int64}))
    inputs["orders"] = ParquetSource(
        path="orders.parquet", schema=pl.Schema({"id": pl.Int64})
    )

    assert isinstance(metadata.inputs["orders"], CsvSource)
    with pytest.raises(TypeError):
        cast(dict[str, object], metadata.inputs)["other"] = object()
    for contract in (CsvSource, ParquetSource, QuerySource, QueryMetadata):
        assert all(
            parameter.kind is Parameter.KEYWORD_ONLY
            for parameter in signature(contract).parameters.values()
        )
    metadata_parameters = signature(QueryMetadata).parameters
    assert tuple(metadata_parameters) == ("inputs", "output_schema")
    metadata_defaults = tuple(
        parameter.default for parameter in metadata_parameters.values()
    )
    assert all(default is Parameter.empty for default in metadata_defaults)


def test_path_ids_private_files_and_metadata_contract(tmp_path: Path) -> None:
    write_model(tmp_path, "staging/orders.py", model_source())
    write_model(tmp_path, "marts/daily.py", model_source())
    write_model(tmp_path, "_private.py", model_source())
    write_model(tmp_path, "nested/_private/ignored.py", model_source())

    project = discover(tmp_path)

    assert project.node_ids == ("marts.daily", "staging.orders")
    assert project.graph == {"marts.daily": (), "staging.orders": ()}
    write_model(
        tmp_path,
        "missing.py",
        model_source().replace(
            "metadata = QueryMetadata", "metadata_missing = QueryMetadata"
        ),
    )
    with pytest.raises(DiscoveryError, match="own QueryMetadata"):
        discover(tmp_path)


def test_signature_missing_dependency_and_cycle_validation(tmp_path: Path) -> None:
    write_model(tmp_path, "wrong.py", model_source("unexpected"))
    with pytest.raises(DiscoveryError, match="do not exactly match"):
        discover(tmp_path)

    tmp_path.joinpath("wrong.py").unlink()
    write_model(
        tmp_path,
        "missing.py",
        model_source(
            "upstream",
            inputs="{'upstream': QuerySource(node_id='none', schema=SCHEMA)}",
        ),
    )
    with pytest.raises(QueryValidationError, match="missing node"):
        discover(tmp_path)

    tmp_path.joinpath("missing.py").unlink()
    write_model(
        tmp_path,
        "a.py",
        model_source("b", inputs="{'b': QuerySource(node_id='b', schema=SCHEMA)}"),
    )
    write_model(
        tmp_path,
        "b.py",
        model_source("a", inputs="{'a': QuerySource(node_id='a', schema=SCHEMA)}"),
    )
    with pytest.raises(QueryValidationError, match="Cycle detected"):
        discover(tmp_path)


def test_query_source_schema_mismatch_fails_before_transform(tmp_path: Path) -> None:
    write_model(tmp_path, "producer.py", model_source())
    mismatched_schema = "pl.Schema({'other': pl.Int64})"
    source_prefix = "{'producer': QuerySource(node_id='producer', schema="
    mismatched_source = f"{source_prefix}{mismatched_schema})}}"
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "producer",
            inputs=mismatched_source,
            body="raise AssertionError('transform must not run')",
        ),
    )

    with pytest.raises(QueryValidationError, match="expects schema"):
        discover(tmp_path)


def test_physical_source_is_scanned_once_and_shared_by_consumers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orders = tmp_path / "orders.csv"
    orders.write_text("value\n1\n")
    probe = ModuleType("source_probe")
    probe.__dict__["frames"] = []
    sys.modules["source_probe"] = probe
    try:
        sources = (
            repr(str(orders)),
            f"Path({str(orders)!r})",
            repr(f"{orders.parent}/./{orders.name}"),
        )
        for name, source in zip(("left", "right", "third"), sources, strict=True):
            write_model(
                tmp_path,
                f"{name}.py",
                model_source(
                    "orders",
                    inputs=f"{{'orders': CsvSource(path={source}, schema=SCHEMA)}}",
                    body=(
                        "from source_probe import frames\n"
                        "        frames.append(orders)\n"
                        "        return orders"
                    ),
                ),
            )
        calls = 0
        original = CsvSource.scan

        def spy(source: CsvSource) -> pl.LazyFrame:
            nonlocal calls
            calls += 1
            return original(source)

        monkeypatch.setattr(CsvSource, "scan", spy)
        built = discover(tmp_path).build(["left", "right", "third"])
    finally:
        sys.modules.pop("source_probe", None)

    assert calls == 1
    assert probe.__dict__["frames"] == [
        built.frames["left"],
        built.frames["right"],
        built.frames["third"],
    ]
    assert probe.__dict__["frames"][0] is probe.__dict__["frames"][1]
    assert probe.__dict__["frames"][1] is probe.__dict__["frames"][2]


def test_local_source_paths_are_normalized_for_contract_identity(
    tmp_path: Path,
) -> None:
    orders = tmp_path / "orders.csv"
    schema = pl.Schema({"value": pl.Int64})
    declared = CsvSource(path=str(orders), schema=schema)
    path_declared = CsvSource(path=orders, schema=schema)
    alternate_spelling = CsvSource(
        path=f"{orders.parent}/./{orders.name}", schema=schema
    )

    assert declared.path == str(orders.resolve())
    assert declared == path_declared == alternate_spelling
    assert declared.identity == path_declared.identity == alternate_spelling.identity


def test_uri_source_paths_preserve_full_identity_without_local_normalization() -> None:
    schema = pl.Schema({"value": pl.Int64})
    signed_url = "s3://bucket/orders.csv?signature=first#fragment"
    source = CsvSource(path=signed_url, schema=schema)
    other_source = CsvSource(
        path="s3://bucket/orders.csv?signature=second#fragment", schema=schema
    )

    assert source.path == signed_url
    assert source.identity == ("csv", signed_url)
    assert source.identity != other_source.identity


def test_conflicting_physical_source_contracts_are_rejected(tmp_path: Path) -> None:
    orders = tmp_path / "orders.csv"
    orders.write_text("value\n1\n")
    path = repr(str(orders))
    write_model(
        tmp_path,
        "first.py",
        model_source(
            "orders",
            inputs=(
                f"{{'orders': CsvSource(path={path}, schema=SCHEMA, separator=',')}}"
            ),
            body="return orders",
        ),
    )
    write_model(
        tmp_path,
        "second.py",
        model_source(
            "orders",
            inputs=(
                f"{{'orders': CsvSource(path={path}, schema=SCHEMA, separator=';')}}"
            ),
            body="return orders",
        ),
    )

    with pytest.raises(QueryValidationError, match="Conflicting declarations"):
        discover(tmp_path).build(["first", "second"])


def test_csv_source_rejects_same_width_wrong_header_at_build_time(
    tmp_path: Path,
) -> None:
    orders = tmp_path / "orders.csv"
    orders.write_text("wrong_name\n1\n")
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "orders",
            inputs=(f"{{'orders': CsvSource(path={str(orders)!r}, schema=SCHEMA)}}"),
            body="return orders",
        ),
    )

    with pytest.raises(QueryBuildError, match="output schema mismatch") as error:
        discover(tmp_path).build(["consumer"])

    assert "wrong_name" in str(error.value)


def test_csv_malformed_values_remain_an_execution_time_failure(tmp_path: Path) -> None:
    orders = tmp_path / "orders.csv"
    orders.write_text("value\nnot-an-integer\n")
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "orders",
            inputs=(f"{{'orders': CsvSource(path={str(orders)!r}, schema=SCHEMA)}}"),
            body="return orders",
        ),
    )
    project = discover(tmp_path)

    project.build(["consumer"])
    with pytest.raises(QueryExecutionError, match="Failed to collect"):
        project.run(["consumer"])


def test_headerless_csv_source_applies_declared_schema(tmp_path: Path) -> None:
    orders = tmp_path / "orders.csv"
    orders.write_text("1\n")
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "orders",
            inputs=(
                f"{{'orders': CsvSource(path={str(orders)!r}, schema=SCHEMA, "
                "has_header=False)}"
            ),
            body="return orders",
        ),
    )

    result = discover(tmp_path).run(["consumer"])["consumer"]

    assert result.schema == pl.Schema({"value": pl.Int64})
    assert result["value"].to_list() == [1]


def test_parquet_source_schema_mismatch_fails_at_build_time(tmp_path: Path) -> None:
    orders = tmp_path / "orders.parquet"
    pl.DataFrame({"wrong_name": [1]}).write_parquet(orders)
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "orders",
            inputs=(
                f"{{'orders': ParquetSource(path={str(orders)!r}, schema=SCHEMA)}}"
            ),
            body="return orders",
        ),
    )

    with pytest.raises(QueryBuildError, match="output schema mismatch") as error:
        discover(tmp_path).build(["consumer"])

    assert "wrong_name" in str(error.value)


def test_physical_source_failure_has_source_context_and_cause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    orders = tmp_path / "orders.csv"
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "orders",
            inputs=(f"{{'orders': CsvSource(path={str(orders)!r}, schema=SCHEMA)}}"),
            body="return orders",
        ),
    )

    def broken_scan(source: CsvSource) -> pl.LazyFrame:
        raise OSError("source unavailable")

    monkeypatch.setattr(CsvSource, "scan", broken_scan)
    with pytest.raises(
        QueryBuildError, match="Failed to build physical source"
    ) as error:
        discover(tmp_path).build(["consumer"])

    assert str(orders) in str(error.value)
    assert isinstance(error.value.__cause__, OSError)


def test_source_failure_redacts_uri_credentials_and_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    signed_url = (
        "s3://access-key:secret@bucket/orders.csv?signature=signed-value#fragment"
    )
    write_model(
        tmp_path,
        "consumer.py",
        model_source(
            "orders",
            inputs=f"{{'orders': CsvSource(path={signed_url!r}, schema=SCHEMA)}}",
            body="return orders",
        ),
    )

    def broken_scan(source: CsvSource) -> pl.LazyFrame:
        raise OSError(f"source unavailable: {signed_url}")

    monkeypatch.setattr(CsvSource, "scan", broken_scan)
    with pytest.raises(QueryBuildError) as error:
        discover(tmp_path).build(["consumer"])

    message = str(error.value)
    assert "s3://bucket/orders.csv" in message
    for secret in ("access-key", "secret", "signature", "signed-value", "fragment"):
        assert secret not in message
    assert isinstance(error.value.__cause__, OSError)
    assert signed_url not in str(error.value.__cause__)


def test_source_contract_conflict_redacts_uri_credentials_and_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    signed_url = (
        "s3://access-key:secret@bucket/orders.csv?signature=signed-value#fragment"
    )
    for name, separator in (("first", ","), ("second", ";")):
        write_model(
            tmp_path,
            f"{name}.py",
            model_source(
                "orders",
                inputs=(
                    f"{{'orders': CsvSource(path={signed_url!r}, schema=SCHEMA, "
                    f"separator={separator!r})}}"
                ),
                body="return orders",
            ),
        )

    monkeypatch.setattr(CsvSource, "scan", lambda source: pl.LazyFrame({"value": [1]}))
    with pytest.raises(QueryValidationError, match="Conflicting declarations") as error:
        discover(tmp_path).build(["first", "second"])

    message = str(error.value)
    assert "s3://bucket/orders.csv" in message
    for secret in ("access-key", "secret", "signature", "signed-value", "fragment"):
        assert secret not in message


def test_collect_schema_validates_output_without_collecting_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_model(tmp_path, "model.py", model_source())
    calls = 0
    original = pl.LazyFrame.collect_schema

    def spy(frame: pl.LazyFrame) -> pl.Schema:
        nonlocal calls
        calls += 1
        return original(frame)

    monkeypatch.setattr(pl.LazyFrame, "collect_schema", spy)
    discover(tmp_path).build(["model"])

    assert calls == 1


def test_run_collects_requested_targets_together(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_model(tmp_path, "left.py", model_source())
    write_model(tmp_path, "right.py", model_source())
    calls: list[int] = []
    original = pl.collect_all

    def spy(frames: list[pl.LazyFrame]) -> list[pl.DataFrame]:
        calls.append(len(frames))
        return original(frames)

    monkeypatch.setattr(pl, "collect_all", spy)
    results = discover(tmp_path).run(["left", "right"])

    assert list(results) == ["left", "right"]
    assert calls == [2]


def test_collection_failure_redacts_uri_credentials_and_components(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    signed_url = (
        "https://access-key:secret@storage.example/orders.csv?signature=signed-value"
        "#fragment"
    )
    write_model(tmp_path, "consumer.py", model_source())

    def failed_collection(frames: list[pl.LazyFrame]) -> list[pl.DataFrame]:
        raise OSError(f"remote source unavailable: {signed_url}")

    monkeypatch.setattr(pl, "collect_all", failed_collection)
    with pytest.raises(QueryExecutionError) as error:
        discover(tmp_path).run(["consumer"])

    for message in (str(error.value), str(error.value.__cause__)):
        assert "https://storage.example/orders.csv" in message
        for secret in ("access-key", "secret", "signature", "signed-value", "fragment"):
            assert secret not in message
    assert isinstance(error.value.__cause__, OSError)
