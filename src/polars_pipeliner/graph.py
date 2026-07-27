"""Immutable model DAG validation and deterministic target resolution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from types import MappingProxyType

from .discovery import ModelNode
from .errors import QueryValidationError
from .sources import QuerySource


@dataclass(frozen=True)
class ModelGraph:
    """A fully validated immutable graph of discovered model nodes."""

    nodes: Mapping[str, ModelNode]
    dependencies: Mapping[str, tuple[str, ...]]

    @classmethod
    def create(cls, nodes: Mapping[str, ModelNode]) -> ModelGraph:
        """Validate missing dependencies and cycles for all discovered models."""
        stable_nodes = MappingProxyType(dict(nodes))
        dependencies = MappingProxyType(
            {
                node_id: tuple(
                    source.node_id
                    for source in node.model.metadata.inputs.values()
                    if isinstance(source, QuerySource)
                )
                for node_id, node in stable_nodes.items()
            }
        )
        validate_missing_dependencies(stable_nodes, dependencies)
        topological_order(dependencies)
        validate_query_source_schemas(stable_nodes)
        return cls(nodes=stable_nodes, dependencies=dependencies)

    @property
    def node_ids(self) -> tuple[str, ...]:
        """All node IDs in stable discovered/path order."""
        return tuple(self.nodes)

    def resolve(self, targets: Sequence[str]) -> tuple[str, ...]:
        """Select a target closure manually, then graphlib-order its induced DAG."""
        requested = normalise_targets(targets)
        unknown = [target for target in requested if target not in self.nodes]
        if unknown:
            raise QueryValidationError.unknown_targets(unknown)
        selected: set[str] = set()
        pending = list(requested)
        while pending:
            node_id = pending.pop()
            if node_id not in selected:
                selected.add(node_id)
                pending.extend(self.dependencies[node_id])
        induced = {
            node_id: tuple(
                dependency for dependency in dependencies if dependency in selected
            )
            for node_id, dependencies in self.dependencies.items()
            if node_id in selected
        }
        return topological_order(induced)


def validate_missing_dependencies(
    nodes: Mapping[str, ModelNode], dependencies: Mapping[str, tuple[str, ...]]
) -> None:
    """Reject unresolved references before graphlib treats them as implicit nodes."""
    for node_id, node in nodes.items():
        missing = sorted(set(dependencies[node_id]).difference(nodes))
        if missing:
            raise QueryValidationError.missing_dependencies(node_id, node.path, missing)


def validate_query_source_schemas(nodes: Mapping[str, ModelNode]) -> None:
    """Ensure every graph edge agrees with its producer's declared output schema."""
    for node_id, node in nodes.items():
        for argument, source in node.model.metadata.inputs.items():
            if isinstance(source, QuerySource):
                producer = nodes[source.node_id]
                actual = producer.model.metadata.output_schema
                if source.schema != actual:
                    raise QueryValidationError.query_source_schema_mismatch(
                        node_id,
                        node.path,
                        argument,
                        source.node_id,
                        source.schema,
                        actual,
                    )


def topological_order(graph: Mapping[str, Sequence[str]]) -> tuple[str, ...]:
    """Return graphlib's deterministic insertion-order topological order."""
    try:
        return tuple(TopologicalSorter(graph).static_order())
    except CycleError as error:
        cycle = error.args[1] if len(error.args) > 1 else None
        if isinstance(cycle, list) and all(
            isinstance(node_id, str) for node_id in cycle
        ):
            raise QueryValidationError.cycle(cycle) from error
        raise QueryValidationError.cycle() from error


def normalise_targets(targets: Sequence[str]) -> tuple[str, ...]:
    """Validate explicit target IDs while preserving caller order."""
    if isinstance(targets, str):
        raise QueryValidationError.targets_must_be_sequence()
    normalised = tuple(targets)
    if not normalised:
        raise QueryValidationError.empty_targets()
    if any(not isinstance(target, str) or not target for target in normalised):
        raise QueryValidationError.invalid_targets()
    if len(set(normalised)) != len(normalised):
        raise QueryValidationError.duplicate_targets()
    return normalised
