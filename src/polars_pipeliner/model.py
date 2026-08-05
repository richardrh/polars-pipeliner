"""Public model contracts for Polars pipeline projects."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar

import polars as pl

from .output import OutputSpec


@dataclass(frozen=True, slots=True)
class Input:
    """An immutable binding from a transform argument to an upstream node."""

    node_id: str
    schema: pl.Schema = field(kw_only=True)


class SourceModel:
    """A model that constructs a source LazyFrame."""

    output_schema: ClassVar[pl.Schema]

    def source(self) -> pl.LazyFrame:
        """Construct this source's canonical lazy plan."""
        raise NotImplementedError


class Model:
    """A model that transforms named upstream LazyFrames."""

    output_schema: ClassVar[pl.Schema]


class MartModel(Model):
    """A materialized model with an executor-owned output declaration."""

    output: ClassVar[OutputSpec]


def _declared_inputs(model: type[SourceModel | Model]) -> Mapping[str, Input]:
    """Return Input bindings declared directly on a concrete model class."""
    return {
        name: binding
        for name, binding in model.__dict__.items()
        if isinstance(binding, Input)
    }
