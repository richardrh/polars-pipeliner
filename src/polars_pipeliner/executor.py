"""Lazy plan construction and physical target collection."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

import polars as pl

from .errors import (
    ModelValidationError,
    QueryBuildError,
    QueryExecutionError,
    QueryValidationError,
    redact_exception,
)
from .graph import ModelGraph, normalise_targets
from .sources import QuerySource, Source, source_location

LOGGER = logging.getLogger("polars_pipeliner.executor")


@dataclass(frozen=True)
class BuildResult:
    """Lazy frames built for a selection, including its dependency closure."""

    targets: Mapping[str, pl.LazyFrame]
    frames: Mapping[str, pl.LazyFrame]


def build_models(
    graph: ModelGraph,
    targets: Sequence[str],
) -> BuildResult:
    """Build selected LazyFrame plans and validate their schemas in DAG order."""
    requested = normalise_targets(targets)
    order = graph.resolve(requested)
    frames: dict[str, pl.LazyFrame] = {}
    source_frames: dict[tuple[str, str], pl.LazyFrame] = {}
    source_contracts: dict[tuple[str, str], Source] = {}
    for node_id in order:
        node = graph.nodes[node_id]
        arguments: dict[str, pl.LazyFrame] = {}
        dependencies = tuple(graph.dependencies[node_id])
        for name, source in node.model.metadata.inputs.items():
            if isinstance(source, QuerySource):
                arguments[name] = frames[source.node_id]
            else:
                arguments[name] = resolve_source(
                    source, source_frames, source_contracts
                )
        LOGGER.info("Building LazyFrame plan for model %s", node_id)
        LOGGER.debug(
            "Model %s path=%s dependencies=%s", node_id, node.path, dependencies
        )
        started = time.perf_counter()
        try:
            frame = node.model.transform(**arguments)
            if not isinstance(frame, pl.LazyFrame):
                raise ModelValidationError.non_lazy_frame(node.model, frame)
            node.model.validate(frame)
        except Exception as error:
            elapsed = (time.perf_counter() - started) * 1000
            LOGGER.error("Failed building model %s after %.1f ms", node_id, elapsed)
            raise QueryBuildError.model_failed(node_id, node.path, error) from error
        elapsed = (time.perf_counter() - started) * 1000
        LOGGER.info("Built LazyFrame plan for model %s in %.1f ms", node_id, elapsed)
        frames[node_id] = frame
    return BuildResult(
        targets=MappingProxyType({node_id: frames[node_id] for node_id in requested}),
        frames=MappingProxyType(frames),
    )


def run_models(
    graph: ModelGraph,
    targets: Sequence[str],
) -> Mapping[str, pl.DataFrame]:
    """Build target plans and physically collect only requested target frames."""
    requested = normalise_targets(targets)
    built = build_models(graph, requested)
    target_label = ", ".join(requested)
    LOGGER.info("Collecting target LazyFrames: %s", target_label)
    started = time.perf_counter()
    try:
        collected = pl.collect_all(list(built.targets.values()))
    except Exception as error:
        elapsed = (time.perf_counter() - started) * 1000
        LOGGER.error(
            "Failed collecting target(s) %s after %.1f ms", target_label, elapsed
        )
        redacted_error = redact_exception(error)
        execution_error = QueryExecutionError.collection_failed(requested, error)
        raise execution_error from redacted_error
    elapsed = (time.perf_counter() - started) * 1000
    LOGGER.info("Collected target LazyFrames: %s in %.1f ms", target_label, elapsed)
    return MappingProxyType(dict(zip(built.targets, collected, strict=True)))


def resolve_source(
    source: Source,
    frames: dict[tuple[str, str], pl.LazyFrame],
    contracts: dict[tuple[str, str], Source],
) -> pl.LazyFrame:
    """Construct and schema-validate each physical source exactly once per run."""
    identity = source.identity
    existing = contracts.get(identity)
    if existing is not None:
        if existing != source:
            raise QueryValidationError.source_contract_conflict(
                source_location(identity)
            )
        return frames[identity]
    try:
        frame = source.scan()
        actual = frame.collect_schema()
    except Exception as error:
        raise QueryBuildError.source_failed(
            source_location(identity), error
        ) from redact_exception(error)
    if actual != source.schema:
        raise QueryBuildError.source_failed(
            source_location(identity),
            ModelValidationError.schema_mismatch(type(source), source.schema, actual),
        )
    contracts[identity] = source
    frames[identity] = frame
    return frame
