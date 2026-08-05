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

from .errors import DiscoveryError, QueryError, QueryValidationError
from .model import MartModel, Model, SourceModel, _declared_inputs
from .output import DeltaUpsertOutput, OutputSpec

MODEL_DIRECTORIES = ("sources", "staging", "intermediate", "marts")


@dataclass(frozen=True)
class ModelNode:
    node_id: str
    path: Path
    model: type[SourceModel | Model]


def _discover_models(root: Path) -> dict[str, ModelNode]:
    """Import exactly one local model class from every scoped model file."""
    if not root.is_dir():
        raise DiscoveryError.invalid_root(root)
    nodes: dict[str, ModelNode] = {}
    paths = sorted(
        (
            path
            for directory in MODEL_DIRECTORIES
            if (root / directory).is_dir()
            for path in (root / directory).rglob("*.py")
            if not any(part.startswith("_") for part in path.relative_to(root).parts)
        ),
        key=str,
    )
    for path in paths:
        node_id = ".".join(path.relative_to(root).with_suffix("").parts)
        try:
            if node_id in nodes:
                raise QueryValidationError.duplicate_node_id(
                    node_id, nodes[node_id].path, path
                )
            model_type = _load_model_class(path, root, node_id)
        except QueryError as error:
            error.context.update(
                {"node_id": node_id, "path": path, "models_found": len(paths)}
            )
            raise
        nodes[node_id] = ModelNode(node_id, path, model_type)
    return nodes


def _load_model_class(
    path: Path, root: Path, node_id: str
) -> type[SourceModel | Model]:
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
    try:
        models = {
            item
            for item in module.__dict__.values()
            if isinstance(item, type)
            and item.__module__ == module.__name__
            and issubclass(item, (SourceModel, Model))
            and item not in (SourceModel, Model, MartModel)
            and not inspect.isabstract(item)
        }
        if len(models) != 1:
            raise DiscoveryError.model_count(node_id, path, len(models))
        model = next(iter(models))
        _validate_model_class(model, path, node_id)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return model


def _validate_model_class(
    model: type[SourceModel | Model], path: Path, node_id: str
) -> None:
    tier = node_id.split(".", maxsplit=1)[0]
    if not isinstance(model.__dict__.get("output_schema"), pl.Schema):
        raise DiscoveryError.missing_definition(
            node_id, path, "output_schema: pl.Schema"
        )
    if isinstance(model.__dict__.get("inputs"), Mapping):
        raise DiscoveryError.unsupported_inputs_mapping(node_id, path)
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
        output = model.__dict__.get("output")
        if not isinstance(output, OutputSpec):
            raise DiscoveryError.missing_definition(node_id, path, "output declaration")
        if isinstance(output, DeltaUpsertOutput):
            missing_keys = [
                key for key in output.keys if key not in model.output_schema
            ]
            if missing_keys:
                raise DiscoveryError.invalid_output(
                    node_id,
                    path,
                    f"Delta upsert key(s) missing from output_schema: {missing_keys!r}",
                )
    elif issubclass(model, (SourceModel, MartModel)):
        raise DiscoveryError.invalid_placement(
            node_id, path, model.__name__, "staging or intermediate"
        )
    inputs = _declared_inputs(model)
    for binding in inputs.values():
        if (
            not isinstance(binding.node_id, str)
            or not binding.node_id
            or not isinstance(binding.schema, pl.Schema)
        ):
            raise DiscoveryError.invalid_input_binding(
                node_id, path, "input", "node ID or schema"
            )
    _validate_method(model, path, node_id, "transform", set(inputs))


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
    if not inspect.isfunction(descriptor):
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
