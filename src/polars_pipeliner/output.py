"""Immutable output declarations owned by the pipeline executor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

type Destination = str | Path
type _ParquetCompression = Literal["snappy", "gzip", "lzo", "brotli", "lz4", "zstd"]
type _IpcCompression = Literal["uncompressed", "lz4", "zstd"]
type _DeltaMode = Literal["error", "append", "overwrite", "ignore"]
type _IcebergMode = Literal["append", "overwrite"]


@dataclass(frozen=True, kw_only=True, slots=True)
class OutputSpec:
    """Base class for a mart output destination."""

    destination: Destination


@dataclass(frozen=True, kw_only=True, slots=True)
class ParquetOutput(OutputSpec):
    compression: _ParquetCompression = "zstd"


@dataclass(frozen=True, kw_only=True, slots=True)
class CsvOutput(OutputSpec):
    separator: str = ","
    include_header: bool = True


@dataclass(frozen=True, kw_only=True, slots=True)
class IpcOutput(OutputSpec):
    compression: _IpcCompression = "uncompressed"


@dataclass(frozen=True, kw_only=True, slots=True)
class NdjsonOutput(OutputSpec):
    pass


@dataclass(frozen=True, kw_only=True, slots=True)
class DeltaOutput(OutputSpec):
    mode: _DeltaMode = "error"


@dataclass(frozen=True, kw_only=True, slots=True)
class IcebergOutput(OutputSpec):
    mode: _IcebergMode = "append"


class Output:
    """Factories for executor-owned mart outputs."""

    @staticmethod
    def parquet(
        destination: Destination,
        *,
        compression: _ParquetCompression = "zstd",
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
        compression: _IpcCompression = "uncompressed",
    ) -> IpcOutput:
        return IpcOutput(destination=destination, compression=compression)

    @staticmethod
    def ndjson(destination: Destination) -> NdjsonOutput:
        return NdjsonOutput(destination=destination)

    @staticmethod
    def delta(
        destination: Destination,
        *,
        mode: _DeltaMode = "error",
    ) -> DeltaOutput:
        return DeltaOutput(destination=destination, mode=mode)

    @staticmethod
    def iceberg(
        destination: Destination,
        *,
        mode: _IcebergMode = "append",
    ) -> IcebergOutput:
        return IcebergOutput(destination=destination, mode=mode)
