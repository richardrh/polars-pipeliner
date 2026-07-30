"""Public exception types and contextual message factories."""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

URI_PATTERN = re.compile(r"\b[a-zA-Z][a-zA-Z0-9+.-]*://\S+")


def _redact_uris(text: str) -> str:
    def redact(match: re.Match[str]) -> str:
        parsed = urlsplit(match.group())
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc.rsplit("@", maxsplit=1)[-1],
                parsed.path,
                "",
                "",
            )
        )

    return URI_PATTERN.sub(redact, text)


def redact_text(text: str) -> str:
    """Redact URI credentials, queries, and fragments in public text."""
    return _redact_uris(text)


def redact_exception(error: Exception) -> Exception:
    """Return an equivalent exception whose text does not expose URI secrets."""
    detail = _redact_uris(str(error))
    if detail == str(error):
        return error
    try:
        return type(error)(detail)
    except Exception:
        return RuntimeError(detail)


class QueryError(Exception):
    """Base exception for model-project failures."""

    def __init__(
        self, message: str, *, context: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.context = context or {}


class ConfigError(QueryError):
    """A project configuration file is missing or invalid."""

    @classmethod
    def invalid_log_level(cls, actual: str, valid: Iterable[str]) -> ConfigError:
        return cls(
            f"Invalid LOG_LEVEL {actual!r}; expected one of: {', '.join(sorted(valid))}"
        )

    @classmethod
    def invalid_run_log_dir(cls, actual: object) -> ConfigError:
        return cls(f"Invalid RUN_LOG_DIR {actual!r}; expected a path")

    @classmethod
    def missing_file(cls, path: Path) -> ConfigError:
        return cls(f"Configuration file does not exist: {path}")

    @classmethod
    def malformed_file(cls, path: Path, detail: Exception) -> ConfigError:
        return cls(f"Malformed configuration file {path}: {detail}")

    @classmethod
    def unreadable_file(cls, path: Path, detail: Exception) -> ConfigError:
        return cls(f"Could not read configuration file {path}: {detail}")

    @classmethod
    def missing_table(cls, path: Path, table: str) -> ConfigError:
        return cls(f"Configuration file {path} must contain [{table}]")

    @classmethod
    def unknown_settings(cls, path: Path, settings: Iterable[str]) -> ConfigError:
        return cls(
            f"Configuration file {path} has unknown setting(s): {', '.join(settings)}"
        )

    @classmethod
    def missing_setting(cls, path: Path, table: str, setting: str) -> ConfigError:
        return cls(f"Configuration file {path} is missing {table}.{setting}")

    @classmethod
    def non_string_setting(cls, path: Path, setting: str) -> ConfigError:
        return cls(f"Configuration file {path} has non-string {setting}")

    @classmethod
    def invalid_setting(cls, path: Path, detail: Exception) -> ConfigError:
        return cls(f"Configuration file {path}: {detail}")

    @classmethod
    def conflicting_sources(cls) -> ConfigError:
        return cls("Pass either config or config_path, not both")


class DiscoveryError(QueryError):
    """A model file could not be imported or did not declare a valid model."""

    @classmethod
    def invalid_root(cls, root: Path) -> DiscoveryError:
        return cls(f"Project root is not a directory: {root}")

    @classmethod
    def import_spec(cls, path: Path) -> DiscoveryError:
        return cls(f"Cannot create an import specification for {path}")

    @classmethod
    def import_failed(
        cls, node_id: str, path: Path, detail: Exception
    ) -> DiscoveryError:
        return cls(f"Could not import model {node_id} ({path}): {detail}")

    @classmethod
    def model_count(cls, node_id: str, path: Path, count: int) -> DiscoveryError:
        return cls(
            f"Model {node_id} ({path}) must define exactly one local concrete "
            f"model class; found {count}"
        )

    @classmethod
    def missing_definition(
        cls, node_id: str, path: Path, definition: str
    ) -> DiscoveryError:
        return cls(f"Model {node_id} ({path}) must define its own {definition}")

    @classmethod
    def invalid_parameter_kind(
        cls, node_id: str, path: Path, method: str
    ) -> DiscoveryError:
        return cls(f"Model {node_id} ({path}) {method} may only use named parameters")

    @classmethod
    def binding_signature_mismatch(
        cls,
        node_id: str,
        path: Path,
        method: str,
        actual: Iterable[str],
        expected: Iterable[str],
    ) -> DiscoveryError:
        return cls(
            f"Model {node_id} ({path}) {method} parameters {sorted(actual)!r} do not "
            f"exactly match declared bindings {sorted(expected)!r}"
        )

    @classmethod
    def invalid_binding(
        cls, node_id: str, path: Path, label: str, kind: str
    ) -> DiscoveryError:
        return cls(f"Model {node_id} ({path}) has an invalid {label} {kind}")

    @classmethod
    def legacy_inputs_mapping(cls, node_id: str, path: Path) -> DiscoveryError:
        return cls(
            f"Model {node_id} ({path}) must declare each Input directly as a class "
            "attribute; inputs = {...} is not supported"
        )

    @classmethod
    def source_inputs(cls, node_id: str, path: Path) -> DiscoveryError:
        return cls(f"Source model {node_id} ({path}) may not declare Input attributes")

    @classmethod
    def invalid_source(cls, node_id: str, path: Path) -> DiscoveryError:
        return cls(f"Model {node_id} ({path}) has an invalid input source")

    @classmethod
    def invalid_placement(
        cls, node_id: str, path: Path, model: str, directory: str
    ) -> DiscoveryError:
        return cls(
            f"Model {node_id} ({path}) is a {model} and must be under {directory}/"
        )

    @classmethod
    def invalid_method(cls, node_id: str, path: Path, method: str) -> DiscoveryError:
        return cls(
            f"Model {node_id} ({path}) must define ordinary instance method {method}"
        )


class QueryValidationError(QueryError):
    """The set of discovered models is not a valid DAG."""

    @classmethod
    def duplicate_node_id(cls, node_id: str, path: Path) -> QueryValidationError:
        return cls(f"Duplicate node ID {node_id!r}: {path}")

    @classmethod
    def missing_dependencies(
        cls, node_id: str, path: Path, dependencies: Iterable[str]
    ) -> QueryValidationError:
        return cls(
            f"Model {node_id} ({path}) depends on missing node(s): "
            f"{', '.join(dependencies)}",
            context={"node_id": node_id, "path": path},
        )

    @classmethod
    def cycle(cls, nodes: Iterable[str] | None = None) -> QueryValidationError:
        if nodes is None:
            return cls("Cycle detected in model graph")
        return cls(f"Cycle detected: {' -> '.join(nodes)}")

    @classmethod
    def query_source_schema_mismatch(
        cls,
        node_id: str,
        path: Path,
        argument: str,
        producer: str,
        expected: object,
        actual: object,
    ) -> QueryValidationError:
        return cls(
            f"Model {node_id} ({path}) input {argument!r} expects schema {expected} "
            f"from {producer!r}, but its declared output schema is {actual}",
            context={
                "node_id": node_id,
                "path": path,
                "argument": argument,
                "producer": producer,
                "expected_schema": expected,
                "actual_schema": actual,
            },
        )


class QueryBuildError(QueryError):
    """A selected query failed while its LazyFrame plan was being built."""

    @classmethod
    def model_failed(
        cls, node_id: str, path: Path, detail: Exception
    ) -> QueryBuildError:
        return cls(
            f"Failed to build {node_id} ({path}): {detail}",
            context={"node_id": node_id, "path": path},
        )

    @classmethod
    def source_failed(cls, location: str, detail: Exception) -> QueryBuildError:
        return cls(
            f"Failed to build physical source {_redact_uris(location)}: "
            f"{_redact_uris(str(detail))}"
        )


class QueryExecutionError(QueryError):
    """A mart output failed during executor-owned materialization."""

    @classmethod
    def output_failed(
        cls, node_id: str, destination: str, detail: Exception
    ) -> QueryExecutionError:
        return cls(
            f"Failed to materialize {node_id} to {_redact_uris(destination)}: {_redact_uris(str(detail))}"
        )

    @classmethod
    def grouped_outputs_failed(
        cls, outputs: Iterable[tuple[str, str]], detail: Exception
    ) -> QueryExecutionError:
        destinations = ", ".join(
            f"{node_id} to {_redact_uris(destination)}"
            for node_id, destination in outputs
        )
        return cls(
            f"Failed to materialize grouped outputs ({destinations}): "
            f"{_redact_uris(str(detail))}"
        )


class ModelValidationError(ValueError):
    """A model returned an invalid lazy plan or output schema."""

    def __init__(
        self, message: str, *, context: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.context = context or {}

    @classmethod
    def schema_resolution_failed(
        cls, model: type[object], detail: Exception
    ) -> ModelValidationError:
        return cls(f"{model.__qualname__}: could not resolve output schema: {detail}")

    @classmethod
    def schema_mismatch(
        cls, model: object, expected: object, actual: object
    ) -> ModelValidationError:
        return cls(
            f"{type(model).__qualname__}: output schema mismatch; "
            f"expected {expected}, got {actual}",
            context={"expected_schema": expected, "actual_schema": actual},
        )

    @classmethod
    def non_lazy_frame(cls, model: object, actual: object) -> ModelValidationError:
        return cls(
            f"{type(model).__qualname__}: model method returned "
            f"{type(actual).__name__}, not polars.LazyFrame"
        )
