"""Immutable output declarations owned by the pipeline executor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

type Destination = str | Path


@dataclass(frozen=True, kw_only=True)
class OutputSpec:
    """Base class for a mart output destination."""

    destination: Destination


@dataclass(frozen=True, kw_only=True)
class ParquetOutput(OutputSpec):
    compression: Literal["snappy", "gzip", "lzo", "brotli", "lz4", "zstd"] = "zstd"


@dataclass(frozen=True, kw_only=True)
class CsvOutput(OutputSpec):
    separator: str = ","
    include_header: bool = True


@dataclass(frozen=True, kw_only=True)
class IpcOutput(OutputSpec):
    compression: Literal["uncompressed", "lz4", "zstd"] = "uncompressed"


@dataclass(frozen=True, kw_only=True)
class NdjsonOutput(OutputSpec):
    pass


@dataclass(frozen=True, kw_only=True)
class DeltaOutput(OutputSpec):
    mode: Literal["error", "append", "overwrite", "ignore"] = "error"


@dataclass(frozen=True, kw_only=True)
class IcebergOutput(OutputSpec):
    mode: Literal["append", "overwrite"] = "append"


class Output:
    """Factories for executor-owned mart outputs."""

    @staticmethod
    def parquet(
        destination: Destination,
        *,
        compression: Literal["snappy", "gzip", "lzo", "brotli", "lz4", "zstd"] = "zstd",
    ) -> ParquetOutput:
        return ParquetOutput(destination=destination, compression=compression)

    @staticmethod
    def csv(
        destination: Destination, *, separator: str = ",", include_header: bool = True
    ) -> CsvOutput:
        return CsvOutput(
            destination=destination, separator=separator, include_header=include_header
        )

    @staticmethod
    def ipc(
        destination: Destination,
        *,
        compression: Literal["uncompressed", "lz4", "zstd"] = "uncompressed",
    ) -> IpcOutput:
        return IpcOutput(destination=destination, compression=compression)

    @staticmethod
    def ndjson(destination: Destination) -> NdjsonOutput:
        return NdjsonOutput(destination=destination)

    @staticmethod
    def delta(
        destination: Destination,
        *,
        mode: Literal["error", "append", "overwrite", "ignore"] = "error",
    ) -> DeltaOutput:
        return DeltaOutput(destination=destination, mode=mode)

    @staticmethod
    def iceberg(
        destination: Destination,
        *,
        mode: Literal["append", "overwrite"] = "append",
    ) -> IcebergOutput:
        return IcebergOutput(destination=destination, mode=mode)
