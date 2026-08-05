"""Immutable complete model DAG validation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from types import MappingProxyType

from .discovery import ModelNode
from .errors import QueryValidationError
from .model import SourceModel, _declared_inputs


@dataclass(frozen=True)
class ModelGraph:
    nodes: Mapping[str, ModelNode]
    dependencies: Mapping[str, tuple[str, ...]]

    @classmethod
    def create(cls, nodes: Mapping[str, ModelNode]) -> ModelGraph:
        stable_nodes = MappingProxyType(dict(nodes))
        dependencies: dict[str, tuple[str, ...]] = {}
        for node_id, node in stable_nodes.items():
            if issubclass(node.model, SourceModel):
                dependencies[node_id] = ()
                continue
            inputs = _declared_inputs(node.model)
            dependencies[node_id] = tuple(
                binding.node_id for binding in inputs.values()
            )
            missing = sorted(set(dependencies[node_id]).difference(stable_nodes))
            if missing:
                raise QueryValidationError.missing_dependencies(
                    node_id, node.path, missing
                )
            for name, binding in inputs.items():
                producer = stable_nodes[binding.node_id].model
                if binding.schema != producer.output_schema:
                    raise QueryValidationError.input_schema_mismatch(
                        node_id,
                        node.path,
                        name,
                        binding.node_id,
                        binding.schema,
                        producer.output_schema,
                    )
        graph = cls(stable_nodes, MappingProxyType(dependencies))
        graph.resolve()
        return graph

    @property
    def node_ids(self) -> tuple[str, ...]:
        return tuple(self.nodes)

    def resolve(self) -> tuple[str, ...]:
        try:
            return tuple(TopologicalSorter(self.dependencies).static_order())
        except CycleError as error:
            cycle = error.args[1] if len(error.args) > 1 else None
            raise QueryValidationError.cycle(
                cycle if isinstance(cycle, Sequence) else None
            ) from error
