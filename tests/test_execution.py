from __future__ import annotations

import builtins
import importlib
import json
from pathlib import Path
from typing import cast

import polars as pl
import pytest

import polars_pipeliner.executor as executor
from polars_pipeliner import QueryBuildError, QueryExecutionError, discover


def write(root: Path, relative: str, contents: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def source(contents: str = "return pl.LazyFrame({'value': [1]})") -> str:
    return f"""import polars as pl
from polars_pipeliner import SourceModel
class Orders(SourceModel):
    output_schema = pl.Schema({{"value": pl.Int64}})
    def source(self) -> pl.LazyFrame:
        {contents}
"""


def mart(name: str, output: str, body: str = "return orders") -> str:
    return f"""import polars as pl
from polars_pipeliner import Input, MartModel, Output
SCHEMA = pl.Schema({{'value': pl.Int64}})
class {name}(MartModel):
    orders = Input('sources.orders', schema=SCHEMA)
    output_schema = SCHEMA
    output = {output}
    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        {body}
"""


def test_source_is_called_once_and_all_marts_materialize_in_one_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(
        tmp_path,
        "sources/orders.py",
        source(
            "from probe import calls\n        calls.append(1)\n        return pl.LazyFrame({'value': [1]})"
        ),
    )
    write(
        tmp_path,
        "marts/first.py",
        mart("First", "Output.parquet('target/first.parquet')"),
    )
    write(
        tmp_path, "marts/second.py", mart("Second", "Output.csv('target/second.csv')")
    )
    (tmp_path / "probe.py").write_text("calls = []\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    probe = importlib.import_module("probe")

    calls: list[int] = []
    original = pl.collect_all

    def spy(frames: list[pl.LazyFrame]) -> list[pl.DataFrame]:
        calls.append(len(frames))
        return original(frames)

    monkeypatch.setattr(pl, "collect_all", spy)
    manifest = discover(tmp_path).run()

    assert cast(list[int], probe.__dict__["calls"]) == [1]
    assert calls == [2]
    assert manifest == {
        "marts.first": tmp_path / "target/first.parquet",
        "marts.second": tmp_path / "target/second.csv",
    }
    assert (tmp_path / "target/first.parquet").is_file()
    assert (tmp_path / "target/second.csv").is_file()


def test_discovery_does_not_instantiate_or_invoke_models(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    monkeypatch.setattr(builtins, "_pipeline_probe", events, raising=False)
    write(
        tmp_path,
        "sources/orders.py",
        """import builtins
import polars as pl
from polars_pipeliner import SourceModel
class Orders(SourceModel):
    output_schema = pl.Schema({'value': pl.Int64})
    def __init__(self) -> None:
        builtins._pipeline_probe.append('source_init')
    def source(self) -> pl.LazyFrame:
        builtins._pipeline_probe.append('source')
        return pl.LazyFrame({'value': [1]})
""",
    )
    write(
        tmp_path,
        "marts/orders.py",
        """import builtins
import polars as pl
from polars_pipeliner import Input, MartModel, Output
SCHEMA = pl.Schema({'value': pl.Int64})
class Orders(MartModel):
    orders = Input('sources.orders', schema=SCHEMA)
    output_schema = SCHEMA
    output = Output.parquet('out.parquet')
    def __init__(self) -> None:
        builtins._pipeline_probe.append('mart_init')
    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        builtins._pipeline_probe.append('transform')
        return orders
""",
    )

    project = discover(tmp_path)

    assert events == []
    project.run()
    assert events == [
        "source_init",
        "source",
        "mart_init",
        "transform",
    ]


@pytest.mark.parametrize(
    ("factory", "filename", "reader"),
    [
        ("parquet", "out.parquet", "read_parquet"),
        ("csv", "out.csv", "read_csv"),
        ("ipc", "out.ipc", "read_ipc"),
        ("ndjson", "out.ndjson", "read_ndjson"),
    ],
)
def test_composable_file_outputs_are_written(
    tmp_path: Path, factory: str, filename: str, reader: str
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(
        tmp_path, "marts/orders.py", mart("Orders", f"Output.{factory}('{filename}')")
    )

    manifest = discover(tmp_path).run()

    destination = tmp_path / filename
    assert manifest == {"marts.orders": destination}
    assert getattr(pl, reader)(destination).to_dict(as_series=False) == {"value": [1]}


def test_bad_return_schema_and_no_marts_fail_before_writes(tmp_path: Path) -> None:
    write(tmp_path, "sources/orders.py", source("return pl.DataFrame({'value': [1]})"))
    write(tmp_path, "marts/orders.py", mart("Orders", "Output.parquet('out.parquet')"))
    with pytest.raises(QueryBuildError, match="not polars.LazyFrame"):
        discover(tmp_path).run()
    assert not (tmp_path / "out.parquet").exists()
    (tmp_path / "sources/orders.py").unlink()
    (tmp_path / "marts/orders.py").unlink()
    write(
        tmp_path,
        "staging/orders.py",
        """import polars as pl
from polars_pipeliner import Model
class Orders(Model):
    output_schema = pl.Schema({'value': pl.Int64})
    def transform(self) -> pl.LazyFrame:
        return pl.LazyFrame({'value': [1]})
""",
    )
    with pytest.raises(QueryBuildError, match="No MartModel"):
        discover(tmp_path).run()


@pytest.mark.parametrize(
    ("source_body", "mart_body", "message"),
    [
        ("return pl.LazyFrame({'other': [1]})", "return orders", "schema mismatch"),
        ("return pl.LazyFrame({'value': [1]})", "return orders.collect()", "LazyFrame"),
    ],
)
def test_actual_source_schemas_and_transform_returns_are_validated(
    tmp_path: Path, source_body: str, mart_body: str, message: str
) -> None:
    write(tmp_path, "sources/orders.py", source(source_body))
    write(
        tmp_path,
        "marts/orders.py",
        mart("Orders", "Output.parquet('out.parquet')", mart_body),
    )

    with pytest.raises(QueryBuildError, match=message):
        discover(tmp_path).run()
    assert not (tmp_path / "out.parquet").exists()


def test_source_schema_overrides_do_not_mask_wrong_csv_headers(tmp_path: Path) -> None:
    (tmp_path / "orders.csv").write_text("wrong_header\n1\n")
    write(
        tmp_path,
        "sources/orders.py",
        f"""import polars as pl
from polars_pipeliner import SourceModel
class Orders(SourceModel):
    output_schema = pl.Schema({{'value': pl.Int64}})
    def source(self) -> pl.LazyFrame:
        return pl.scan_csv({str(tmp_path / "orders.csv")!r}, schema_overrides=self.output_schema)
""",
    )
    write(tmp_path, "marts/orders.py", mart("Orders", "Output.parquet('out.parquet')"))

    with pytest.raises(QueryBuildError, match="schema mismatch") as raised:
        discover(tmp_path).run()
    assert "wrong_header" in str(raised.value)


def test_uri_manifest_and_errors_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri = "s3://key:secret@bucket/orders.parquet?token=value#fragment"
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("Orders", f"Output.parquet({uri!r})"))

    def broken(frames: list[pl.LazyFrame]) -> list[pl.DataFrame]:
        raise OSError(uri)

    monkeypatch.setattr(pl, "collect_all", broken)
    with pytest.raises(QueryExecutionError) as raised:
        discover(tmp_path).run()
    message = str(raised.value) + str(raised.value.__cause__)
    assert "s3://bucket/orders.parquet" in message
    for secret in ("key", "secret", "token", "value", "fragment"):
        assert secret not in message


def test_object_store_options_are_forwarded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri = "s3://bucket/orders.parquet"
    options = {"region": "us-east-1", "endpoint_url": "http://localhost:9000"}
    write(tmp_path, "sources/orders.py", source())
    write(
        tmp_path,
        "marts/orders.py",
        mart(
            "Orders",
            f"Output.parquet({uri!r}, storage_options={options!r})",
        ),
    )
    calls: list[tuple[str | Path, dict[str, object]]] = []

    def sink(
        self: pl.LazyFrame, destination: str | Path, **kwargs: object
    ) -> pl.LazyFrame:
        calls.append((destination, kwargs))
        return self

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", sink)

    assert discover(tmp_path).run() == {"marts.orders": uri}
    assert calls == [
        (
            uri,
            {
                "compression": "zstd",
                "storage_options": options,
                "lazy": True,
            },
        )
    ]


def test_object_store_options_are_redacted_from_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = "never-log-this-secret"
    write(tmp_path, "sources/orders.py", source())
    write(
        tmp_path,
        "marts/orders.py",
        mart(
            "Orders",
            "Output.parquet("
            "'s3://bucket/orders.parquet', "
            f"storage_options={{'secret_access_key': {secret!r}}})",
        ),
    )

    def broken_sink(
        self: pl.LazyFrame, destination: str | Path, **kwargs: object
    ) -> pl.LazyFrame:
        raise OSError(f"credential {secret} failed")

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", broken_sink)

    with pytest.raises(QueryExecutionError) as raised:
        discover(tmp_path).run()

    message = str(raised.value) + str(raised.value.__cause__)
    assert secret not in message
    assert "***" in message


def test_grouped_output_failure_identifies_all_affected_destinations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/first.py", mart("First", "Output.parquet('first.parquet')"))
    write(tmp_path, "marts/second.py", mart("Second", "Output.csv('second.csv')"))
    monkeypatch.setattr(
        pl, "collect_all", lambda frames: (_ for _ in ()).throw(OSError("sink failed"))
    )

    with pytest.raises(QueryExecutionError, match="grouped outputs") as raised:
        discover(tmp_path).run()
    message = str(raised.value)
    assert "marts.first" in message
    assert str(tmp_path / "first.parquet") in message
    assert "marts.second" in message
    assert str(tmp_path / "second.csv") in message


def test_output_preparation_failure_is_attributed_to_its_mart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("Orders", "Output.parquet('out.parquet')"))

    def broken_sink(
        self: pl.LazyFrame, *args: object, **kwargs: object
    ) -> pl.LazyFrame:
        raise OSError("cannot prepare sink")

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", broken_sink)
    with pytest.raises(QueryExecutionError) as raised:
        discover(tmp_path).run()
    assert f"marts.orders to {tmp_path / 'out.parquet'}" in str(raised.value)


def test_uri_manifest_redacts_credentials_query_and_fragment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uri = "s3://key:secret@bucket/orders?token=value#fragment"
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("Orders", f"Output.delta({uri!r})"))
    monkeypatch.setattr(
        pl.LazyFrame,
        "sink_delta",
        lambda self, target, **kwargs: None,
        raising=False,
    )

    assert discover(tmp_path).run() == {"marts.orders": "s3://bucket/orders"}


def test_relative_delta_destination_resolves_from_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(
        tmp_path,
        "marts/orders.py",
        mart("Orders", "Output.delta('relative-output')"),
    )
    destinations: list[str | Path] = []

    def sink(self: pl.LazyFrame, destination: str | Path, **kwargs: object) -> None:
        destinations.append(destination)

    monkeypatch.setattr(pl.LazyFrame, "sink_delta", sink, raising=False)
    assert discover(tmp_path).run() == {"marts.orders": tmp_path / "relative-output"}
    assert destinations == [tmp_path / "relative-output"]


@pytest.mark.parametrize(
    ("output", "module", "extra"),
    [
        ("Output.delta('table')", "deltalake", "delta"),
        (
            "Output.iceberg('analytics.orders', catalog='production')",
            "pyiceberg",
            "iceberg",
        ),
    ],
)
def test_missing_output_extra_has_an_actionable_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output: str,
    module: str,
    extra: str,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("Orders", output))
    real_find_spec = executor.importlib.util.find_spec
    monkeypatch.setattr(
        executor.importlib.util,
        "find_spec",
        lambda name: None if name == module else real_find_spec(name),
    )

    with pytest.raises(
        QueryExecutionError,
        match=rf"install 'polars-pipeliner\[{extra}\]'",
    ):
        discover(tmp_path).run()


def test_second_direct_output_failure_is_attributed_to_its_mart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/first.py", mart("First", "Output.delta('first')"))
    write(tmp_path, "marts/second.py", mart("Second", "Output.delta('second')"))
    calls = 0

    def sink(self: pl.LazyFrame, destination: str | Path, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("second failed")

    monkeypatch.setattr(pl.LazyFrame, "sink_delta", sink, raising=False)
    with pytest.raises(QueryExecutionError) as raised:
        discover(tmp_path).run()
    assert f"marts.second to {tmp_path / 'second'}" in str(raised.value)


def test_unsupported_output_spec_fails_without_success(tmp_path: Path) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(
        tmp_path,
        "marts/orders.py",
        mart("Orders", "UnsupportedOutput(destination='unsupported')").replace(
            "from polars_pipeliner import Input, MartModel, Output",
            "from dataclasses import dataclass\n"
            "from polars_pipeliner import Input, MartModel\n"
            "from polars_pipeliner.output import OutputSpec\n"
            "@dataclass(frozen=True, kw_only=True)\n"
            "class UnsupportedOutput(OutputSpec):\n"
            "    destination: str",
        ),
    )

    with pytest.raises(QueryExecutionError, match="not a supported output"):
        discover(tmp_path).run()

    assert not (tmp_path / "unsupported").exists()
    log_path = next((tmp_path / "target/runs").glob("*.jsonl"))
    event_names = [
        json.loads(line)["event"] for line in log_path.read_text().splitlines()
    ]
    assert "run_failed" in event_names
    assert "output_written" not in event_names
    assert "run_succeeded" not in event_names
