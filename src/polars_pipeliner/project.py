"""Public project facade for model discovery, planning, and execution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl

from .config import ProjectConfig, configure_logging, load_config
from .discovery import discover_models
from .errors import ConfigError
from .executor import BuildResult, build_models, run_models
from .graph import ModelGraph


class Project:
    """A discovered, fully validated model project."""

    def __init__(self, graph: ModelGraph, config: ProjectConfig) -> None:
        self._graph = graph
        self.config = config
        self.graph = graph.dependencies

    @property
    def node_ids(self) -> tuple[str, ...]:
        """All discovered node IDs in deterministic path order."""
        return self._graph.node_ids

    def resolve(self, targets: Sequence[str]) -> tuple[str, ...]:
        """Return the selected targets' dependency closure in build order."""
        return self._graph.resolve(targets)

    def build(
        self,
        targets: Sequence[str],
    ) -> BuildResult:
        """Construct selected LazyFrame plans without physical execution."""
        return build_models(self._graph, targets)

    def run(
        self,
        targets: Sequence[str],
    ) -> Mapping[str, pl.DataFrame]:
        """Construct and physically collect selected targets."""
        return run_models(self._graph, targets)


def discover(
    query_root: str | Path,
    *,
    config: ProjectConfig | None = None,
    config_path: str | Path | None = None,
) -> Project:
    """Discover a project with either supplied config or an explicit TOML path."""
    if config is not None and config_path is not None:
        raise ConfigError.conflicting_sources()
    resolved_config = config if config is not None else load_config(config_path)
    configure_logging(resolved_config)
    return Project(ModelGraph.create(discover_models(query_root)), resolved_config)
