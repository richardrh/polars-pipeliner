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
from .executor import BuildResult
from .model import PolarsModel
from .project import Project, discover
from .sources import (
    CsvSource,
    ParquetSource,
    QueryMetadata,
    QuerySource,
    Source,
)

__all__ = [
    "BuildResult",
    "ConfigError",
    "DiscoveryError",
    "ModelValidationError",
    "PolarsModel",
    "CsvSource",
    "ParquetSource",
    "Project",
    "ProjectConfig",
    "QueryMetadata",
    "QuerySource",
    "QueryBuildError",
    "QueryError",
    "QueryExecutionError",
    "QueryValidationError",
    "Source",
    "discover",
    "load_config",
]
