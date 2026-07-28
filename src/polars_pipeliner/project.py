"""Public project facade for complete-graph execution."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from .config import ProjectConfig, configure_logging, load_config
from .discovery import discover_models
from .errors import ConfigError
from .executor import BuildResult, build_models, run_models
from .graph import ModelGraph


class Project:
    def __init__(self, root: Path, graph: ModelGraph, config: ProjectConfig) -> None:
        self._root = root
        self._graph = graph
        self.config = config
        self.graph = graph.dependencies

    @property
    def node_ids(self) -> tuple[str, ...]:
        return self._graph.node_ids

    def resolve(self) -> tuple[str, ...]:
        return self._graph.resolve()

    def build(self) -> BuildResult:
        return build_models(self._graph)

    def run(self) -> Mapping[str, str | Path]:
        return run_models(self._graph, self._root)


def discover(
    query_root: str | Path,
    *,
    config: ProjectConfig | None = None,
    config_path: str | Path | None = None,
) -> Project:
    if config is not None and config_path is not None:
        raise ConfigError.conflicting_sources()
    root = Path(query_root).resolve()
    resolved_config = config if config is not None else load_config(config_path)
    configure_logging(resolved_config)
    return Project(root, ModelGraph.create(discover_models(root)), resolved_config)
