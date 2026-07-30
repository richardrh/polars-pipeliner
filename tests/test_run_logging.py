from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from polars_pipeliner import (
    ProjectConfig,
    QueryBuildError,
    QueryValidationError,
    discover,
)

SCHEMA = "pl.Schema({'value': pl.Int64})"


def write(root: Path, relative: str, contents: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def source() -> str:
    return f"""import polars as pl
from polars_pipeliner import SourceModel
class Orders(SourceModel):
    output_schema = {SCHEMA}
    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({{'value': [1]}})
"""


def mart(body: str = "return orders") -> str:
    return f"""import polars as pl
from polars_pipeliner import Input, MartModel, Output
class Orders(MartModel):
    orders = Input('sources.orders', schema={SCHEMA})
    output_schema = {SCHEMA}
    output = Output.parquet('target/orders.parquet')
    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        {body}
"""


def logged_events(log_file: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in log_file.read_text().splitlines()]


def log_files(root: Path) -> list[Path]:
    return sorted((root / "target/runs").glob("*.jsonl"))


def test_python_success_writes_ordered_jsonl_manifest_without_touching_root_logger(
    tmp_path: Path,
) -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())

    project = discover(tmp_path)
    assert not (tmp_path / "target/runs").exists()
    manifest = project.run()
    [log_file] = log_files(tmp_path)
    events = logged_events(log_file)

    assert manifest == {"marts.orders": tmp_path / "target/orders.parquet"}
    assert [event["event"] for event in events] == [
        "run_started",
        "model_validated",
        "model_validated",
        "output_written",
        "run_succeeded",
    ]
    assert events[-1]["manifest"] == {
        "marts.orders": str(tmp_path / "target/orders.parquet")
    }
    assert all(
        {"timestamp", "run_id", "level", "event"} <= event.keys() for event in events
    )
    assert list(root_logger.handlers) == original_handlers


def test_python_transform_schema_failure_logs_context_and_writes_no_mart(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("return orders.rename({'value': 'other'})"))

    with pytest.raises(QueryBuildError) as raised:
        discover(tmp_path).run()

    [log_file] = log_files(tmp_path)
    failure = logged_events(log_file)[-1]
    assert failure["event"] == "run_failed"
    assert failure["node_id"] == "marts.orders"
    assert failure["path"] == str(tmp_path / "marts/orders.py")
    assert failure["expected_schema"] == {"value": "Int64"}
    assert failure["actual_schema"] == {"other": "Int64"}
    assert "output schema mismatch" in str(failure["message"])
    assert raised.value.__cause__ is not None
    assert not (tmp_path / "target/orders.parquet").exists()


def test_python_discovery_input_schema_failure_writes_jsonl_without_invoking_models(
    tmp_path: Path,
) -> None:
    write(
        tmp_path,
        "sources/orders.py",
        source().replace(
            "return pl.LazyFrame({'value': [1]})", "raise AssertionError('called')"
        ),
    )
    write(
        tmp_path,
        "staging/orders.py",
        mart()
        .replace("MartModel", "Model")
        .replace(
            "from polars_pipeliner import Input, Model, Output",
            "from polars_pipeliner import Input, Model",
        )
        .replace("    output = Output.parquet('target/orders.parquet')\n", "")
        .replace(
            "schema=pl.Schema({'value': pl.Int64})",
            "schema=pl.Schema({'other': pl.Int64})",
        ),
    )

    with pytest.raises(QueryValidationError):
        discover(tmp_path)

    [log_file] = log_files(tmp_path)
    failure = logged_events(log_file)[-1]
    assert failure["event"] == "run_failed"
    assert failure["node_id"] == "staging.orders"
    assert failure["argument"] == "orders"
    assert failure["producer"] == "sources.orders"
    assert failure["expected_schema"] == {"other": "Int64"}
    assert failure["actual_schema"] == {"value": "Int64"}


def test_python_jsonl_redacts_uri_secrets_and_repeated_runs_do_not_duplicate_events(
    tmp_path: Path,
) -> None:
    secret_uri = "s3://key:secret@bucket/data?token=value#fragment"
    write(
        tmp_path,
        "sources/orders.py",
        source().replace(
            "return pl.LazyFrame({'value': [1]})", f"raise RuntimeError({secret_uri!r})"
        ),
    )
    write(tmp_path, "marts/orders.py", mart())
    project = discover(tmp_path)

    for _ in range(2):
        with pytest.raises(QueryBuildError):
            project.run()

    files = log_files(tmp_path)
    assert len(files) == 2
    event_runs = [logged_events(log_file) for log_file in files]
    assert all(
        [event["event"] for event in events] == ["run_started", "run_failed"]
        for events in event_runs
    )
    run_ids = {events[0]["run_id"] for events in event_runs}
    assert len(run_ids) == 2
    logged_text = json.dumps(event_runs)
    assert "s3://bucket/data" in logged_text
    for secret in ("key", "secret", "token", "value", "fragment"):
        assert secret not in logged_text


def test_warning_log_level_suppresses_node_events_but_keeps_boundaries(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())

    discover(tmp_path, config=ProjectConfig(log_level="WARNING")).run()

    [log_file] = log_files(tmp_path)
    assert [event["event"] for event in logged_events(log_file)] == [
        "run_started",
        "run_succeeded",
    ]
