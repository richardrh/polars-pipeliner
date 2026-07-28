"""Complete-graph lazy plan construction and mart materialization."""

from __future__ import annotations

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
from .graph import ModelGraph
from .model import MartModel, SourceModel
from .output import (
    CsvOutput,
    DeltaOutput,
    IcebergOutput,
    IpcOutput,
    NdjsonOutput,
    OutputSpec,
    ParquetOutput,
)


@dataclass(frozen=True)
class BuildResult:
    """Validated lazy plans for the complete discovered graph."""

    frames: Mapping[str, pl.LazyFrame]


def build_models(graph: ModelGraph) -> BuildResult:
    """Call every source/transform once and validate every resulting schema."""
    frames: dict[str, pl.LazyFrame] = {}
    for node_id in graph.resolve():
        node = graph.nodes[node_id]
        try:
            model = node.model()
            if isinstance(model, SourceModel):
                frame = model.source()
            else:
                transform = cast(Any, model).transform
                frame = transform(
                    **{
                        name: frames[binding.node_id]
                        for name, binding in model.inputs.items()
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
    return BuildResult(MappingProxyType(frames))


def _destination(spec: OutputSpec, root: Path) -> str | Path:
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


def _make_parent(destination: str | Path) -> None:
    if isinstance(destination, Path):
        destination.parent.mkdir(parents=True, exist_ok=True)


def _lazy_sink(
    frame: pl.LazyFrame, spec: OutputSpec, destination: str | Path
) -> pl.LazyFrame:
    _make_parent(destination)
    if isinstance(spec, ParquetOutput):
        result = frame.sink_parquet(
            destination, compression=spec.compression, lazy=True
        )
    elif isinstance(spec, CsvOutput):
        result = frame.sink_csv(
            destination,
            separator=spec.separator,
            include_header=spec.include_header,
            lazy=True,
        )
    elif isinstance(spec, IpcOutput):
        result = frame.sink_ipc(destination, compression=spec.compression, lazy=True)
    elif isinstance(spec, NdjsonOutput):
        result = frame.sink_ndjson(destination, lazy=True)
    else:
        raise TypeError(f"{type(spec).__name__} is not a composable file output")
    if not isinstance(result, pl.LazyFrame):
        raise RuntimeError("installed Polars does not support lazy file sink plans")
    return result


def run_models(graph: ModelGraph, root: Path) -> Mapping[str, str | Path]:
    """Validate all plans before writing every mart, then return its manifest."""
    built = build_models(graph)
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
            destination = _destination(spec, root)
            manifest[node_id] = _manifest_destination(destination)
            if isinstance(spec, (ParquetOutput, CsvOutput, IpcOutput, NdjsonOutput)):
                _make_parent(destination)
                file_outputs.append((node_id, destination, built.frames[node_id], spec))
            else:
                direct.append((node_id, built.frames[node_id], spec, destination))
        except Exception as error:
            destination = manifest.get(node_id, "output")
            raise QueryExecutionError.output_failed(
                node_id, str(destination), error
            ) from redact_exception(error)
    if file_outputs:
        try:
            pl.collect_all(
                [
                    _lazy_sink(frame, spec, destination)
                    for _, destination, frame, spec in file_outputs
                ]
            )
        except Exception as error:
            raise QueryExecutionError.grouped_outputs_failed(
                [
                    (node_id, str(destination))
                    for node_id, destination, _, _ in file_outputs
                ],
                error,
            ) from redact_exception(error)
    for node_id, direct_frame, spec, destination in direct:
        try:
            _make_parent(destination)
            if isinstance(spec, DeltaOutput):
                direct_frame.sink_delta(
                    destination, mode=spec.mode
                )  # direct, non-composable API
            elif isinstance(spec, IcebergOutput):
                direct_frame.sink_iceberg(
                    destination, mode=spec.mode
                )  # direct, non-composable API
        except Exception as error:
            raise QueryExecutionError.output_failed(
                node_id, str(manifest[node_id]), error
            ) from redact_exception(error)
    return MappingProxyType(manifest)
