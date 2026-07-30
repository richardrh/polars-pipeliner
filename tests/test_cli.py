from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

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


def mart() -> str:
    return f"""import polars as pl
from polars_pipeliner import Input, MartModel, Output
class Orders(MartModel):
    orders = Input('sources.orders', schema={SCHEMA})
    output_schema = {SCHEMA}
    output = Output.parquet('target/orders.parquet')
    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
"""


def run_cli(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "polars_pipeliner.cli", "run", str(root)],
        check=False,
        capture_output=True,
        text=True,
    )


def test_cli_success_emits_only_jsonl_to_stdout_without_python_log_file(
    tmp_path: Path,
) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())

    result = run_cli(tmp_path)
    events = [json.loads(line) for line in result.stdout.splitlines()]

    assert result.returncode == 0
    assert result.stderr == ""
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
    assert not (tmp_path / "target/runs").exists()


def test_cli_input_schema_failure_emits_structured_final_jsonl_without_traceback(
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

    result = run_cli(tmp_path)
    events = [json.loads(line) for line in result.stdout.splitlines()]
    failure = events[-1]

    assert result.returncode != 0
    assert result.stderr == ""
    assert failure["event"] == "run_failed"
    assert failure["node_id"] == "staging.orders"
    assert failure["argument"] == "orders"
    assert failure["producer"] == "sources.orders"
    assert failure["expected_schema"] == {"other": "Int64"}
    assert failure["actual_schema"] == {"value": "Int64"}


def test_cli_applies_warning_log_level_to_node_events(tmp_path: Path) -> None:
    write(tmp_path, "sources/orders.py", source())
    write(tmp_path, "marts/orders.py", mart())
    config = tmp_path / "polars-pipeliner.toml"
    config.write_text('[polars-pipeliner]\nLOG_LEVEL = "WARNING"\n')

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "polars_pipeliner.cli",
            "run",
            str(tmp_path),
            "--config",
            str(config),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert [json.loads(line)["event"] for line in result.stdout.splitlines()] == [
        "run_started",
        "run_succeeded",
    ]
