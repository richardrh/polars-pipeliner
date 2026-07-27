"""Recursive model-file discovery, importing, and contract validation."""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

import polars as pl

from .errors import DiscoveryError, QueryValidationError
from .model import PolarsModel
from .sources import QueryMetadata, QuerySource, Source


@dataclass(frozen=True)
class ModelNode:
    """A validated model and its stable path-derived node identity."""

    node_id: str
    path: Path
    model: type[PolarsModel[...]]


def discover_models(query_root: str | Path) -> Mapping[str, ModelNode]:
    """Discover public model files in deterministic relative-path order."""
    root = Path(query_root).resolve()
    if not root.is_dir():
        raise DiscoveryError.invalid_root(root)

    nodes: dict[str, ModelNode] = {}
    for path in sorted(root.rglob("*.py")):
        relative = path.relative_to(root)
        if path.name == "__init__.py" or any(
            component.startswith("_") for component in relative.parts
        ):
            continue
        node_id = ".".join(relative.with_suffix("").parts)
        if node_id in nodes:
            raise QueryValidationError.duplicate_node_id(node_id, path)
        model = load_model(path, root, node_id)
        validate_model(model, path, node_id)
        nodes[node_id] = ModelNode(node_id=node_id, path=path, model=model)
    return nodes


def load_model(path: Path, root: Path, node_id: str) -> type[PolarsModel[...]]:
    """Import one model module and return its sole local concrete model class."""
    module_name = (
        "_polars_pipeliner_model_"
        + hashlib.sha256(f"{root}:{node_id}".encode()).hexdigest()
    )
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    if module_spec is None or module_spec.loader is None:
        raise DiscoveryError.import_spec(path)
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    try:
        module_spec.loader.exec_module(module)
    except Exception as error:
        sys.modules.pop(module_name, None)
        raise DiscoveryError.import_failed(node_id, path, error) from error
    return local_concrete_model(module, path, node_id)


def local_concrete_model(
    module: ModuleType, path: Path, node_id: str
) -> type[PolarsModel[...]]:
    """Find exactly one non-abstract PolarsModel declared by this module."""
    models = [
        candidate
        for candidate in module.__dict__.values()
        if isinstance(candidate, type)
        and candidate.__module__ == module.__name__
        and issubclass(candidate, PolarsModel)
        and candidate is not PolarsModel
        and not inspect.isabstract(candidate)
    ]
    if len(models) != 1:
        raise DiscoveryError.model_count(node_id, path, len(models))
    return models[0]


def validate_model(model: type[PolarsModel[...]], path: Path, node_id: str) -> None:
    """Validate static model metadata and its uncalled transform signature."""
    metadata = model.__dict__.get("metadata")
    if not isinstance(metadata, QueryMetadata):
        raise DiscoveryError.missing_definition(
            node_id, path, "QueryMetadata as metadata"
        )
    validate_metadata(metadata, path, node_id)
    for method_name in ("transform",):
        if method_name not in model.__dict__:
            raise DiscoveryError.missing_definition(node_id, path, method_name)
    signature = inspect.signature(model.transform)
    expected = set(metadata.inputs)
    actual: set[str] = set()
    for parameter in signature.parameters.values():
        if parameter.kind not in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        ):
            raise DiscoveryError.invalid_parameter_kind(node_id, path, "transform")
        actual.add(parameter.name)
    if actual != expected:
        raise DiscoveryError.binding_signature_mismatch(
            node_id, path, "transform", actual, expected
        )


def validate_metadata(metadata: QueryMetadata, path: Path, node_id: str) -> None:
    """Validate named source contracts without executing a model."""
    if not isinstance(metadata.output_schema, pl.Schema):
        raise DiscoveryError.invalid_binding(node_id, path, "metadata", "output schema")
    for argument, source in metadata.inputs.items():
        if not isinstance(argument, str) or not argument:
            raise DiscoveryError.invalid_binding(node_id, path, "input", "argument")
        if not isinstance(source, (Source, QuerySource)):
            raise DiscoveryError.invalid_source(node_id, path)
        if not isinstance(source.schema, pl.Schema):
            raise DiscoveryError.invalid_binding(node_id, path, "input", "schema")
        if isinstance(source, QuerySource) and (
            not isinstance(source.node_id, str) or not source.node_id
        ):
            raise DiscoveryError.invalid_binding(
                node_id, path, "query source", "node ID"
            )
