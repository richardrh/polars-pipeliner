"""Public model contracts for Polars pipeline projects."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

import polars as pl

from .output import OutputSpec


@dataclass(frozen=True, init=False)
class Input:
    """An immutable binding from a transform argument to an upstream node."""

    node_id: str
    schema: pl.Schema

    def __init__(self, node_id: str, *, schema: pl.Schema) -> None:
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "schema", schema)


class SourceModel:
    """A model that constructs a source LazyFrame."""

    output_schema: ClassVar[pl.Schema]

    def source(self) -> pl.LazyFrame:
        """Construct this source's canonical lazy plan."""
        raise NotImplementedError


class Model:
    """A model that transforms named upstream LazyFrames."""

    inputs: ClassVar[dict[str, Input]]
    output_schema: ClassVar[pl.Schema]


class MartModel(Model):
    """A materialized model with an executor-owned output declaration."""

    output: ClassVar[OutputSpec]
