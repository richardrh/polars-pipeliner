"""The Polars model lifecycle and declarative input metadata."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ParamSpec, final

import polars as pl

from .errors import ModelValidationError
from .sources import QueryMetadata

P = ParamSpec("P")


class PolarsModel[**P](ABC):
    """Base class for query-file models with schema, validation, and transformation."""

    metadata: QueryMetadata

    @classmethod
    @final
    def validate(cls, frame: pl.LazyFrame) -> None:
        """Resolve and compare a model plan's output schema."""
        try:
            actual = frame.collect_schema()
        except Exception as error:
            raise ModelValidationError.schema_resolution_failed(cls, error) from error
        expected = cls.metadata.output_schema
        if actual != expected:
            raise ModelValidationError.schema_mismatch(cls, expected, actual)

    @classmethod
    @abstractmethod
    def transform(cls, *args: P.args, **kwargs: P.kwargs) -> pl.LazyFrame:
        """Create this model's lazy plan from its declared inputs."""
