"""Complete-graph lazy plan construction and mart materialization."""

from __future__ import annotations

import importlib.util
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit

import polars as pl

from .errors import (
    ModelValidationError,
    QueryBuildError,
    QueryExecutionError,
    redact_exception,
)
from .events import RunEvents
from .graph import ModelGraph
from .model import MartModel, SourceModel, _declared_inputs
from .output import (
    CsvOutput,
    DeltaOutput,
    DeltaUpsertOutput,
    DestinationOutput,
    IcebergOutput,
    IpcOutput,
    NdjsonOutput,
    OutputSpec,
    ParquetOutput,
    StorageOutput,
)

_ICEBERG_STORAGE_OPTION_NAMES = {
    "s3.endpoint": "aws_endpoint_url",
    "s3.access-key-id": "aws_access_key_id",
    "s3.secret-access-key": "aws_secret_access_key",
    "s3.session-token": "aws_session_token",
    "s3.region": "aws_region",
    "s3.proxy-uri": "proxy_url",
    "s3.connect-timeout": "connect_timeout",
    "s3.request-timeout": "timeout",
    "s3.force-virtual-addressing": "aws_virtual_hosted_style_request",
    "adls.account-name": "azure_storage_account_name",
    "adls.account-key": "azure_storage_account_key",
    "adls.sas-token": "azure_storage_sas_key",
    "adls.tenant-id": "azure_storage_tenant_id",
    "adls.client-id": "azure_storage_client_id",
    "adls.client-secret": "azure_storage_client_secret",
    "adls.account-host": "azure_storage_authority_host",
    "adls.token": "azure_storage_token",
    "gcs.oauth2.token": "bearer_token",
}


@dataclass(frozen=True, slots=True)
class BuildResult:
    """Validated lazy plans for the complete discovered graph."""

    frames: Mapping[str, pl.LazyFrame]
    schemas: Mapping[str, pl.Schema]


def build_models(
    graph: ModelGraph,
    events: RunEvents | None = None,
    *,
    include_event_schema: bool = False,
    validated_nodes: set[str] | None = None,
) -> BuildResult:
    """Call every source/transform once and validate every resulting schema."""
    frames: dict[str, pl.LazyFrame] = {}
    schemas: dict[str, pl.Schema] = {}
    for node_id in graph.resolve():
        node = graph.nodes[node_id]
        started = time.monotonic()
        try:
            model = node.model()
            if isinstance(model, SourceModel):
                frame = model.source()
            else:
                transform = cast(Any, model).transform
                frame = transform(
                    **{
                        name: frames[binding.node_id]
                        for name, binding in _declared_inputs(type(model)).items()
                    }
                )
            if not isinstance(frame, pl.LazyFrame):
                raise ModelValidationError.non_lazy_frame(model, frame)
            actual = frame.collect_schema()
            if actual != model.output_schema:
                raise ModelValidationError.schema_mismatch(
                    model, model.output_schema, actual
                )
        except Exception as error:
            raise QueryBuildError.model_failed(
                node_id, node.path, error
            ) from redact_exception(error)
        frames[node_id] = frame
        schemas[node_id] = actual
        if validated_nodes is not None:
            validated_nodes.add(node_id)
        if events is not None:
            model_type = (
                "source"
                if issubclass(node.model, SourceModel)
                else "mart"
                if issubclass(node.model, MartModel)
                else "transform"
            )
            if include_event_schema:
                events.emit(
                    "model_validated",
                    node_id=node_id,
                    path=node.path,
                    type=model_type,
                    schema=actual,
                    duration=round(time.monotonic() - started, 6),
                )
            else:
                events.emit(
                    "model_validated",
                    node_id=node_id,
                    path=node.path,
                    type=model_type,
                    duration=round(time.monotonic() - started, 6),
                )
    return BuildResult(MappingProxyType(frames), MappingProxyType(schemas))


def _destination(spec: DestinationOutput, root: Path) -> str | Path:
    value = spec.destination
    if isinstance(value, Path):
        return value if value.is_absolute() else root / value
    if urlsplit(value).scheme:
        return value
    return root / value


def _manifest_destination(destination: str | Path) -> str | Path:
    if isinstance(destination, Path):
        return destination
    parsed = urlsplit(destination)
    return (
        urlunsplit(
            (parsed.scheme, parsed.netloc.rsplit("@", 1)[-1], parsed.path, "", "")
        )
        if parsed.scheme
        else destination
    )


def _storage_options(spec: StorageOutput) -> dict[str, str] | None:
    return dict(spec.storage_options) if spec.storage_options is not None else None


def _output_secrets(spec: OutputSpec) -> tuple[str, ...]:
    if isinstance(spec, StorageOutput) and spec.storage_options is not None:
        return tuple(spec.storage_options.values())
    return ()


def _iceberg_storage_options(properties: Mapping[str, str]) -> dict[str, str] | None:
    options = {
        translated: value
        for key, value in properties.items()
        if (
            translated := _ICEBERG_STORAGE_OPTION_NAMES.get(
                key, key if "." not in key else ""
            )
        )
    }
    return options or None


def _make_parent(destination: str | Path) -> None:
    if isinstance(destination, Path):
        destination.parent.mkdir(parents=True, exist_ok=True)


def _lazy_sink(
    frame: pl.LazyFrame, spec: OutputSpec, destination: str | Path
) -> pl.LazyFrame:
    _make_parent(destination)
    if isinstance(spec, ParquetOutput):
        result = frame.sink_parquet(
            destination,
            compression=spec.compression,
            storage_options=_storage_options(spec),
            lazy=True,
        )
    elif isinstance(spec, CsvOutput):
        result = frame.sink_csv(
            destination,
            separator=spec.separator,
            include_header=spec.include_header,
            storage_options=_storage_options(spec),
            lazy=True,
        )
    elif isinstance(spec, IpcOutput):
        result = frame.sink_ipc(
            destination,
            compression=spec.compression,
            storage_options=_storage_options(spec),
            lazy=True,
        )
    elif isinstance(spec, NdjsonOutput):
        result = frame.sink_ndjson(
            destination,
            storage_options=_storage_options(spec),
            lazy=True,
        )
    else:
        raise TypeError(f"{type(spec).__name__} is not a composable file output")
    if not isinstance(result, pl.LazyFrame):
        raise RuntimeError("installed Polars does not support lazy file sink plans")
    return result


def _require_optional_dependency(module: str, extra: str, label: str) -> None:
    if importlib.util.find_spec(module) is None:
        raise RuntimeError(
            f"{label} output requires optional dependencies; "
            f"install 'polars-pipeliner[{extra}]'"
        )


def _delta_reference(alias: str, column: str) -> str:
    return f'{alias}."{column.replace('"', '""')}"'


def _write_delta(
    frame: pl.LazyFrame,
    spec: DeltaOutput | DeltaUpsertOutput,
    destination: str | Path,
) -> None:
    _require_optional_dependency("deltalake", "delta", "Delta")
    options = _storage_options(spec)
    if isinstance(spec, DeltaOutput):
        frame.sink_delta(
            destination,
            mode=spec.mode,
            storage_options=options,
        )
        return
    source_alias = "source"
    target_alias = "target"
    predicate = " AND ".join(
        f"{_delta_reference(source_alias, key)} = {_delta_reference(target_alias, key)}"
        for key in spec.keys
    )
    merger = frame.sink_delta(
        destination,
        mode="merge",
        storage_options=options,
        delta_merge_options={
            "predicate": predicate,
            "source_alias": source_alias,
            "target_alias": target_alias,
        },
    )
    if merger is None:
        raise RuntimeError("installed Polars did not return a Delta table merger")
    (merger.when_matched_update_all().when_not_matched_insert_all().execute())


def _write_iceberg(frame: pl.LazyFrame, spec: IcebergOutput) -> None:
    _require_optional_dependency("pyiceberg", "iceberg", "Iceberg")
    from pyiceberg.catalog import load_catalog

    try:
        catalog = load_catalog(spec.catalog)
    except Exception:
        raise RuntimeError(f"Could not load Iceberg catalog {spec.catalog!r}") from None
    try:
        table = catalog.load_table(spec.table)
    except Exception:
        raise RuntimeError(
            f"Could not load Iceberg table {spec.table!r} from catalog {spec.catalog!r}"
        ) from None
    try:
        frame.sink_iceberg(
            table,
            mode=spec.mode,
            storage_options=_iceberg_storage_options(catalog.properties),
        )
    except Exception as error:
        raise redact_exception(error, catalog.properties.values()) from None


def run_models(
    graph: ModelGraph, root: Path, events: RunEvents | None = None
) -> Mapping[str, str | Path]:
    """Validate all plans before writing every mart, then return its manifest."""
    built = build_models(graph, events)
    marts = [
        (node_id, node)
        for node_id, node in graph.nodes.items()
        if issubclass(node.model, MartModel)
    ]
    if not marts:
        raise QueryBuildError("No MartModel instances were discovered")
    manifest: dict[str, str | Path] = {}
    file_outputs: list[tuple[str, str | Path, pl.LazyFrame, OutputSpec]] = []
    direct: list[tuple[str, pl.LazyFrame, OutputSpec, str | Path]] = []
    for node_id, node in marts:
        model = cast(type[MartModel], node.model)
        spec = model.output
        try:
            if isinstance(spec, IcebergOutput):
                target = spec.table
                manifest[node_id] = f"{spec.catalog}:{spec.table}"
                direct.append((node_id, built.frames[node_id], spec, target))
            elif isinstance(spec, DestinationOutput):
                destination = _destination(spec, root)
                manifest[node_id] = _manifest_destination(destination)
                if isinstance(
                    spec, (ParquetOutput, CsvOutput, IpcOutput, NdjsonOutput)
                ):
                    _make_parent(destination)
                    file_outputs.append(
                        (node_id, destination, built.frames[node_id], spec)
                    )
                else:
                    direct.append((node_id, built.frames[node_id], spec, destination))
            else:
                manifest[node_id] = "output"
                direct.append((node_id, built.frames[node_id], spec, "output"))
        except Exception as error:
            destination = manifest.get(node_id, "output")
            safe_error = redact_exception(error, _output_secrets(spec))
            raise QueryExecutionError.output_failed(
                node_id, str(destination), safe_error
            ) from safe_error
    if file_outputs:
        try:
            pl.collect_all(
                [
                    _lazy_sink(frame, spec, destination)
                    for _, destination, frame, spec in file_outputs
                ]
            )
        except Exception as error:
            secrets = (
                secret
                for _, _, _, spec in file_outputs
                for secret in _output_secrets(spec)
            )
            safe_error = redact_exception(error, secrets)
            raise QueryExecutionError.grouped_outputs_failed(
                [
                    (node_id, str(destination))
                    for node_id, destination, _, _ in file_outputs
                ],
                safe_error,
            ) from safe_error
        if events is not None:
            for node_id, destination, _, _ in file_outputs:
                events.emit("output_written", node_id=node_id, path=destination)
    for node_id, direct_frame, spec, destination in direct:
        try:
            if isinstance(spec, (DeltaOutput, DeltaUpsertOutput)):
                _make_parent(destination)
                _write_delta(direct_frame, spec, destination)
            elif isinstance(spec, IcebergOutput):
                _write_iceberg(direct_frame, spec)
            else:
                raise TypeError(f"{type(spec).__name__} is not a supported output")
            if events is not None:
                events.emit(
                    "output_written",
                    node_id=node_id,
                    path=manifest[node_id],
                )
        except Exception as error:
            safe_error = redact_exception(error, _output_secrets(spec))
            raise QueryExecutionError.output_failed(
                node_id, str(manifest[node_id]), safe_error
            ) from safe_error
    return MappingProxyType(manifest)
