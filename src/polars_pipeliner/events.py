"""Minimal JSONL run-event emission without global logging side effects."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

import polars as pl

from .errors import redact_text

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
_BOUNDARY_EVENTS = frozenset(
    {
        "run_started",
        "run_succeeded",
        "run_failed",
        "validation_started",
        "validation_succeeded",
        "validation_failed",
    }
)


def schema_fields(schema: pl.Schema) -> dict[str, str]:
    """Return a stable JSON-friendly representation of a Polars schema."""
    return {name: str(dtype) for name, dtype in schema.items()}


def json_value(value: object) -> object:
    """Convert event values to redacted JSON-compatible primitives."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pl.Schema):
        return schema_fields(value)
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    return value


class RunEvents:
    """Emit one run's events to a lazily created JSONL destination."""

    def __init__(
        self,
        destination: Path | None = None,
        stream: TextIO | None = None,
        log_level: str = "INFO",
    ) -> None:
        self.run_id = uuid.uuid4().hex
        self._destination = destination
        self._stream = stream
        self._log_level = log_level
        self.emitted_failure = False

    @classmethod
    def for_project(
        cls, root: Path, run_log_dir: Path, log_level: str = "INFO"
    ) -> RunEvents:
        directory = run_log_dir if run_log_dir.is_absolute() else root / run_log_dir
        run_id = uuid.uuid4().hex
        events = cls(directory / f"{run_id}.jsonl", log_level=log_level)
        events.run_id = run_id
        return events

    def set_log_level(self, log_level: str) -> None:
        self._log_level = log_level

    def emit(self, event: str, *, level: str = "INFO", **fields: object) -> None:
        if event not in _BOUNDARY_EVENTS and _LEVELS[level] < _LEVELS[self._log_level]:
            return
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "run_id": self.run_id,
            "level": level,
            "event": event,
        }
        payload.update({key: json_value(value) for key, value in fields.items()})
        line = json.dumps(payload, sort_keys=True, default=str) + "\n"
        if event in {"run_failed", "validation_failed"}:
            self.emitted_failure = True
        if self._stream is not None:
            self._stream.write(line)
            self._stream.flush()
            return
        if self._destination is None:
            return
        self._destination.parent.mkdir(parents=True, exist_ok=True)
        with self._destination.open("a", encoding="utf-8") as output:
            output.write(line)


def failure_fields(error: Exception) -> dict[str, object]:
    """Extract redacted structured context from a chained pipeline error."""
    fields: dict[str, object] = {
        "error_type": type(error).__name__,
        "message": redact_text(str(error)),
    }
    current: BaseException | None = error
    while isinstance(current, Exception):
        context = getattr(current, "context", None)
        if isinstance(context, Mapping):
            fields.update(context)
        current = current.__cause__
    return fields
