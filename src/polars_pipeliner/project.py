"""Public project facade for complete-graph execution."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import polars as pl

from .config import ProjectConfig, load_config
from .discovery import ModelNode, _discover_models
from .errors import ConfigError
from .events import RunEvents, failure_fields
from .executor import BuildResult, build_models, run_models
from .graph import ModelGraph


class Project:
    def __init__(
        self,
        root: Path,
        graph: ModelGraph,
        config: ProjectConfig,
        events: RunEvents | None = None,
    ) -> None:
        self._root = root
        self._graph = graph
        self.config = config
        self._events = events

    @property
    def node_ids(self) -> tuple[str, ...]:
        return self._graph.node_ids

    def resolve(self) -> tuple[str, ...]:
        return self._graph.resolve()

    def build(self) -> BuildResult:
        return build_models(self._graph)

    def run(self) -> Mapping[str, str | Path]:
        events = self._events or RunEvents.for_project(
            self._root, self.config.run_log_dir, self.config.log_level
        )
        events.emit("run_started", root=self._root)
        try:
            manifest = run_models(self._graph, self._root, events)
        except Exception as error:
            events.emit("run_failed", level="ERROR", **failure_fields(error))
            raise
        events.emit("run_succeeded", manifest=manifest)
        return manifest

    def validate(self) -> Mapping[str, pl.Schema]:
        """Build and validate schemas without preparing or writing mart outputs."""
        events = self._events or RunEvents.for_project(
            self._root, self.config.run_log_dir, self.config.log_level
        )
        events.emit("validation_started", root=self._root)
        validated_nodes: set[str] = set()
        try:
            built = build_models(
                self._graph,
                events,
                include_event_schema=True,
                validated_nodes=validated_nodes,
            )
        except Exception as error:
            fields = failure_fields(error)
            events.emit(
                "validation_failed",
                level="ERROR",
                summary=validation_summary(
                    len(self._graph.nodes), validated_nodes, fields
                ),
                **fields,
            )
            raise
        events.emit(
            "validation_succeeded",
            schemas=built.schemas,
            summary=validation_summary(len(self._graph.nodes), validated_nodes),
        )
        return built.schemas


def discover(
    query_root: str | Path,
    *,
    config: ProjectConfig | None = None,
    config_path: str | Path | None = None,
    _events: RunEvents | None = None,
    _started_event: str = "run_started",
    _failure_event: str = "run_failed",
    _validation_summary: bool = False,
) -> Project:
    if config is not None and config_path is not None:
        raise ConfigError.conflicting_sources()
    root = Path(query_root).resolve()
    events = _events
    resolved_config: ProjectConfig | None = None
    nodes: Mapping[str, ModelNode] | None = None
    try:
        resolved_config = config if config is not None else load_config(config_path)
        if events is not None:
            events.set_log_level(resolved_config.log_level)
        nodes = _discover_models(root)
        return Project(root, ModelGraph.create(nodes), resolved_config, events)
    except Exception as error:
        if events is None and resolved_config is not None:
            events = RunEvents.for_project(
                root, resolved_config.run_log_dir, resolved_config.log_level
            )
        if events is not None:
            events.emit(_started_event, root=root)
            fields = failure_fields(error)
            models_found = fields.pop("models_found", len(nodes or {}))
            assert isinstance(models_found, int)
            if _validation_summary:
                events.emit(
                    _failure_event,
                    level="ERROR",
                    summary=validation_summary(models_found, set(), fields),
                    **fields,
                )
            else:
                events.emit(_failure_event, level="ERROR", **fields)
        raise


def validation_summary(
    models_found: int,
    validated_nodes: set[str],
    failure: Mapping[str, object] | None = None,
) -> dict[str, int | list[str]]:
    """Return a fail-fast validation summary without inspecting event output."""
    node_id = failure.get("node_id") if failure is not None else None
    failed_models = [node_id] if isinstance(node_id, str) else []
    return {
        "models_found": models_found,
        "models_verified": len(validated_nodes),
        "models_failed": len(failed_models),
        "failed_models": failed_models,
    }
