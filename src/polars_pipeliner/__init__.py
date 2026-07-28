"""A small dependency-aware builder for Polars LazyFrame query files."""

from .config import ProjectConfig, load_config
from .errors import (
    ConfigError,
    DiscoveryError,
    ModelValidationError,
    QueryBuildError,
    QueryError,
    QueryExecutionError,
    QueryValidationError,
)
from .model import Input, MartModel, Model, SourceModel
from .output import Output
from .project import Project, discover

__all__ = [
    "ConfigError",
    "DiscoveryError",
    "ModelValidationError",
    "Input",
    "MartModel",
    "Model",
    "Output",
    "Project",
    "ProjectConfig",
    "QueryBuildError",
    "QueryError",
    "QueryExecutionError",
    "QueryValidationError",
    "SourceModel",
    "discover",
    "load_config",
]
