from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from polars_pipeliner.events import RunEvents, schema_fields


def test_run_events_are_lazy_jsonl_and_redact_uri_secrets(tmp_path: Path) -> None:
    events = RunEvents.for_project(tmp_path, Path("target/runs"))
    log_dir = tmp_path / "target/runs"

    assert not log_dir.exists()
    events.emit("run_started", root=tmp_path)
    events.emit(
        "run_failed",
        level="ERROR",
        message="s3://key:secret@bucket/data?token=value#fragment",
    )

    [log_file] = list(log_dir.glob("*.jsonl"))
    lines = [json.loads(line) for line in log_file.read_text().splitlines()]
    assert [line["event"] for line in lines] == ["run_started", "run_failed"]
    assert all(
        {"timestamp", "run_id", "level", "event"} <= line.keys() for line in lines
    )
    assert lines[-1]["message"] == "s3://bucket/data"


def test_schema_fields_are_json_objects() -> None:
    assert schema_fields(pl.Schema({"id": pl.Int64, "name": pl.String})) == {
        "id": "Int64",
        "name": "String",
    }
