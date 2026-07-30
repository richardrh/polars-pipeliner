"""Scoped model discovery and static contract validation."""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from .errors import DiscoveryError
from .model import Input, MartModel, Model, SourceModel, _declared_inputs
from .output import OutputSpec

MODEL_DIRECTORIES = ("sources", "staging", "intermediate", "marts")


@dataclass(frozen=True)
class ModelNode:
    node_id: str
    path: Path
    model: type[SourceModel | Model]


def discover_models(project_root: str | Path) -> Mapping[str, ModelNode]:
    """Import exactly one local model class from every scoped model file."""
    root = Path(project_root).resolve()
    if not root.is_dir():
        raise DiscoveryError.invalid_root(root)
    nodes: dict[str, ModelNode] = {}
    paths = sorted(
        path
        for directory in MODEL_DIRECTORIES
        if (root / directory).is_dir()
        for path in (root / directory).rglob("*.py")
        if path.name != "__init__.py"
        and not any(
            part.startswith("_") or part == "__pycache__"
            for part in path.relative_to(root).parts
        )
    )
    for path in paths:
        node_id = ".".join(path.relative_to(root).with_suffix("").parts)
        model_type = load_model(path, root, node_id)
        validate_model(model_type, path, node_id)
        nodes[node_id] = ModelNode(node_id, path, model_type)
    return nodes


def load_model(path: Path, root: Path, node_id: str) -> type[SourceModel | Model]:
    module_name = (
        "_polars_pipeliner_model_"
        + hashlib.sha256(f"{root}:{node_id}".encode()).hexdigest()
    )
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise DiscoveryError.import_spec(path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as error:
        sys.modules.pop(module_name, None)
        raise DiscoveryError.import_failed(node_id, path, error) from error
    models = [
        item
        for item in module.__dict__.values()
        if isinstance(item, type)
        and item.__module__ == module.__name__
        and issubclass(item, (SourceModel, Model))
        and item not in (SourceModel, Model, MartModel)
    ]
    if len(models) != 1:
        raise DiscoveryError.model_count(node_id, path, len(models))
    return models[0]


def validate_model(model: type[SourceModel | Model], path: Path, node_id: str) -> None:
    tier = node_id.split(".", maxsplit=1)[0]
    if not isinstance(model.__dict__.get("output_schema"), pl.Schema):
        raise DiscoveryError.missing_definition(
            node_id, path, "output_schema: pl.Schema"
        )
    if isinstance(model.__dict__.get("inputs"), Mapping):
        raise DiscoveryError.legacy_inputs_mapping(node_id, path)
    if tier == "sources":
        if not issubclass(model, SourceModel):
            raise DiscoveryError.invalid_placement(
                node_id, path, "SourceModel", "sources"
            )
        if _declared_inputs(model):
            raise DiscoveryError.source_inputs(node_id, path)
        _validate_method(model, path, node_id, "source", set())
        return
    if tier == "marts":
        if not issubclass(model, MartModel):
            raise DiscoveryError.invalid_placement(node_id, path, "MartModel", "marts")
        if not isinstance(model.__dict__.get("output"), OutputSpec):
            raise DiscoveryError.missing_definition(node_id, path, "output declaration")
    elif issubclass(model, (SourceModel, MartModel)):
        raise DiscoveryError.invalid_placement(
            node_id, path, model.__name__, "staging or intermediate"
        )
    inputs = _validate_inputs(model, path, node_id)
    _validate_method(model, path, node_id, "transform", set(inputs))


def _validate_inputs(
    model: type[Model], path: Path, node_id: str
) -> Mapping[str, Input]:
    inputs = _declared_inputs(model)
    for binding in inputs.values():
        if not binding.node_id or not isinstance(binding.schema, pl.Schema):
            raise DiscoveryError.invalid_binding(
                node_id, path, "input", "node ID or schema"
            )
    return inputs


def _validate_method(
    model: type[SourceModel | Model],
    path: Path,
    node_id: str,
    method: str,
    expected: set[str],
) -> None:
    descriptor = model.__dict__.get(method)
    if descriptor is None:
        raise DiscoveryError.missing_definition(node_id, path, method)
    if isinstance(descriptor, (classmethod, staticmethod)):
        raise DiscoveryError.invalid_method(node_id, path, method)
    parameters = list(inspect.signature(descriptor).parameters.values())
    if not parameters or parameters[0].name != "self":
        raise DiscoveryError.invalid_method(node_id, path, method)
    named = parameters[1:]
    if any(
        parameter.kind not in (parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY)
        for parameter in named
    ):
        raise DiscoveryError.invalid_parameter_kind(node_id, path, method)
    actual = {parameter.name for parameter in named}
    if actual != expected:
        raise DiscoveryError.binding_signature_mismatch(
            node_id, path, method, actual, expected
        )
