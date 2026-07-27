"""Typed, declarative inputs for Polars query models."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, runtime_checkable
from urllib.parse import urlsplit, urlunsplit

import polars as pl


def _freeze_inputs(inputs: Mapping[str, SourceInput]) -> Mapping[str, SourceInput]:
    return MappingProxyType(dict(inputs))


def _source_path(path: str | Path) -> str:
    """Return a canonical local path while preserving URI source identities."""
    source_path = str(path)
    if urlsplit(source_path).scheme:
        return source_path
    return str(Path(source_path).expanduser().resolve())


def _preserve_csv_header(names: list[str]) -> list[str]:
    """Keep CSV field names available for subsequent schema validation."""
    return names


def source_location(identity: tuple[str, str]) -> str:
    """Return a source location suitable for error messages."""
    source_path = identity[1]
    parsed = urlsplit(source_path)
    if not parsed.scheme:
        return source_path
    return urlunsplit(
        (parsed.scheme, parsed.netloc.rsplit("@", maxsplit=1)[-1], parsed.path, "", "")
    )


@runtime_checkable
class Source(Protocol):
    """A physical input that can construct a lazy frame for a declared schema."""

    @property
    def schema(self) -> pl.Schema:
        """The schema expected by the consuming query."""

    @property
    def identity(self) -> tuple[str, str]:
        """A stable identity independent of the declared schema."""

    def scan(self) -> pl.LazyFrame:
        """Construct this source's lazy Polars scan."""


@dataclass(frozen=True, kw_only=True)
class CsvSource:
    """A lazily scanned CSV path or URI with an explicit expected schema."""

    path: str | Path
    schema: pl.Schema
    has_header: bool = True
    separator: str = ","

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _source_path(self.path))

    @property
    def identity(self) -> tuple[str, str]:
        return ("csv", _source_path(self.path))

    def scan(self) -> pl.LazyFrame:
        return pl.scan_csv(
            _source_path(self.path),
            schema_overrides=self.schema,
            has_header=self.has_header,
            separator=self.separator,
            with_column_names=(_preserve_csv_header if self.has_header else None),
            new_columns=None if self.has_header else list(self.schema),
        )


@dataclass(frozen=True, kw_only=True)
class ParquetSource:
    """A lazily scanned Parquet path or URI with an explicit expected schema."""

    path: str | Path
    schema: pl.Schema

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _source_path(self.path))

    @property
    def identity(self) -> tuple[str, str]:
        return ("parquet", _source_path(self.path))

    def scan(self) -> pl.LazyFrame:
        return pl.scan_parquet(_source_path(self.path))


@dataclass(frozen=True, kw_only=True)
class QuerySource:
    """A declared input produced by another discovered query node."""

    node_id: str
    schema: pl.Schema


type SourceInput = Source | QuerySource


@dataclass(frozen=True, kw_only=True)
class QueryMetadata:
    """Immutable input and output contracts for one query model."""

    inputs: Mapping[str, SourceInput]
    output_schema: pl.Schema

    def __post_init__(self) -> None:
        object.__setattr__(self, "inputs", _freeze_inputs(self.inputs))
