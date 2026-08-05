from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import cast

import polars as pl
import pytest

from polars_pipeliner import ProjectConfig, QueryBuildError, discover

SCHEMA = "pl.Schema({'value': pl.Int64})"


def write(root: Path, relative: str, contents: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents)


def source(body: str = "return pl.LazyFrame({'value': [1]})") -> str:
    return f"""import polars as pl
from polars_pipeliner import SourceModel
class Orders(SourceModel):
    output_schema = {SCHEMA}
    def source(self) -> pl.LazyFrame:
        {body}
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


def event_files(root: Path) -> list[Path]:
    return sorted((root / "target/runs").glob("*.jsonl"))


def events(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def cli(root: Path, command: str = "validate") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "polars_pipeliner.cli", command, str(root)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_python_validate_returns_immutable_schemas_and_writes_one_validation_log(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())

    schemas = discover(tmp_path).validate()

    assert schemas == {
        "sources.orders": pl.Schema({"value": pl.Int64}),
        "marts.orders": pl.Schema({"value": pl.Int64}),
    }
    with pytest.raises(TypeError):
        cast(dict[str, pl.Schema], schemas)["other"] = pl.Schema()
    [log_file] = event_files(tmp_path)
    logged = events(log_file)
    assert [event["event"] for event in logged] == [
        "validation_started",
        "model_validated",
        "model_validated",
        "validation_succeeded",
    ]
    assert logged[-1]["schemas"] == {
        "sources.orders": {"value": "Int64"},
        "marts.orders": {"value": "Int64"},
    }
    assert logged[-1]["summary"] == {
        "models_found": 2,
        "models_verified": 2,
        "models_failed": 0,
        "failed_models": [],
    }
    assert not (tmp_path / "target/orders.parquet").exists()


def test_validate_never_collects_rows_or_prepares_sinks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())
    monkeypatch.setattr(
        pl,
        "collect_all",
        lambda frames: (_ for _ in ()).throw(AssertionError("collect_all called")),
    )
    monkeypatch.setattr(
        pl.LazyFrame,
        "sink_parquet",
        lambda self, *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("sink called")
        ),
    )

    discover(tmp_path).validate()

    assert not (tmp_path / "target/orders.parquet").exists()


def test_repeated_python_validate_calls_have_distinct_files_and_ids(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())
    project = discover(tmp_path, config=ProjectConfig(log_level="WARNING"))

    project.validate()
    project.validate()

    files = event_files(tmp_path)
    assert len(files) == 2
    runs = [events(log_file) for log_file in files]
    assert all(
        [event["event"] for event in run]
        == ["validation_started", "validation_succeeded"]
        for run in runs
    )
    assert len({run[0]["run_id"] for run in runs}) == 2
    assert all(
        run[-1]["summary"]
        == {
            "models_found": 2,
            "models_verified": 2,
            "models_failed": 0,
            "failed_models": [],
        }
        for run in runs
    )


def test_cli_validate_success_is_stdout_jsonl_without_logs_or_outputs(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())

    result = cli(tmp_path)
    logged = [json.loads(line) for line in result.stdout.splitlines()]

    assert result.returncode == 0
    assert result.stderr == ""
    assert [event["event"] for event in logged] == [
        "validation_started",
        "model_validated",
        "model_validated",
        "validation_succeeded",
    ]
    assert logged[-1]["summary"] == {
        "models_found": 2,
        "models_verified": 2,
        "models_failed": 0,
        "failed_models": [],
    }
    assert not (tmp_path / "target/runs").exists()
    assert not (tmp_path / "target/orders.parquet").exists()


def test_cli_validate_contract_and_schema_failures_are_structured_and_safe(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source("raise AssertionError('called')"))
    write(
        tmp_path,
        "staging/orders.py",
        """import polars as pl
from polars_pipeliner import Input, Model
class Orders(Model):
    orders = Input('sources.orders', schema=pl.Schema({'other': pl.Int64}))
    output_schema = pl.Schema({'other': pl.Int64})
    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        raise AssertionError('called')
""",
    )

    contract_failure = events_from_cli(cli(tmp_path))
    assert contract_failure["event"] == "validation_failed"
    assert contract_failure["node_id"] == "staging.orders"
    assert contract_failure["expected_schema"] == {"other": "Int64"}
    assert contract_failure["actual_schema"] == {"value": "Int64"}
    assert contract_failure["summary"] == {
        "models_found": 2,
        "models_verified": 0,
        "models_failed": 1,
        "failed_models": ["staging.orders"],
    }

    (tmp_path / "staging/orders.py").unlink()
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("return orders.rename({'value': 'other'})"))
    schema_failure = events_from_cli(cli(tmp_path))
    assert schema_failure["event"] == "validation_failed"
    assert schema_failure["node_id"] == "marts.orders"
    assert schema_failure["expected_schema"] == {"value": "Int64"}
    assert schema_failure["actual_schema"] == {"other": "Int64"}
    assert schema_failure["summary"] == {
        "models_found": 2,
        "models_verified": 1,
        "models_failed": 1,
        "failed_models": ["marts.orders"],
    }
    assert not (tmp_path / "target/orders.parquet").exists()


def events_from_cli(result: subprocess.CompletedProcess[str]) -> dict[str, object]:
    assert result.returncode != 0
    assert result.stderr == ""
    return json.loads(result.stdout.splitlines()[-1])


def test_python_validate_failure_redacts_uri_secrets(tmp_path: Path) -> None:
    uri = "s3://key:secret@bucket/data?token=value#fragment"
    write(tmp_path, "sources/orders.py", source(f"raise RuntimeError({uri!r})"))
    write(tmp_path, "marts/orders.py", mart())

    with pytest.raises(QueryBuildError):
        discover(tmp_path).validate()

    logged = json.dumps(events(event_files(tmp_path)[0]))
    assert "s3://bucket/data" in logged
    for secret in ("key", "secret", "token", "value", "fragment"):
        assert secret not in logged


def test_python_transform_failure_summary_is_fail_fast(tmp_path: Path) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart("return orders.rename({'value': 'other'})"))

    with pytest.raises(QueryBuildError):
        discover(tmp_path).validate()

    failure = events(event_files(tmp_path)[0])[-1]
    assert failure["event"] == "validation_failed"
    assert failure["summary"] == {
        "models_found": 2,
        "models_verified": 1,
        "models_failed": 1,
        "failed_models": ["marts.orders"],
    }


def test_python_warning_summary_counts_hidden_model_events(tmp_path: Path) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())

    discover(tmp_path, config=ProjectConfig(log_level="WARNING")).validate()

    logged = events(event_files(tmp_path)[0])
    assert [event["event"] for event in logged] == [
        "validation_started",
        "validation_succeeded",
    ]
    assert logged[-1]["summary"] == {
        "models_found": 2,
        "models_verified": 2,
        "models_failed": 0,
        "failed_models": [],
    }


def test_cli_discovery_failure_summary_identifies_failed_model(
    tmp_path: Path,
) -> None:
    model_path = tmp_path / "sources/orders.py"
    write(
        tmp_path,
        "sources/orders.py",
        """import polars as pl
from polars_pipeliner import SourceModel

class Orders(SourceModel):
    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({"value": [1]})
""",
    )

    failure = events_from_cli(cli(tmp_path))

    assert failure["event"] == "validation_failed"
    assert failure["node_id"] == "sources.orders"
    assert failure["path"] == str(model_path)
    assert failure["summary"] == {
        "models_found": 1,
        "models_verified": 0,
        "models_failed": 1,
        "failed_models": ["sources.orders"],
    }
