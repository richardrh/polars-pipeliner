"""Immutable output declarations owned by the pipeline executor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Literal

type Destination = str | Path
type StorageOptions = Mapping[str, str]
type _ParquetCompression = Literal["snappy", "gzip", "lzo", "brotli", "lz4", "zstd"]
type _IpcCompression = Literal["uncompressed", "lz4", "zstd"]
type _DeltaMode = Literal["error", "append", "overwrite", "ignore"]
type _IcebergMode = Literal["append", "overwrite"]


@dataclass(frozen=True, kw_only=True, slots=True)
class OutputSpec:
    """Base class for an executor-owned mart output."""


@dataclass(frozen=True, kw_only=True, slots=True)
class StorageOutput(OutputSpec):
    """An output that can forward storage backend options."""

    storage_options: StorageOptions | None = None

    def __post_init__(self) -> None:
        if self.storage_options is not None:
            object.__setattr__(
                self,
                "storage_options",
                MappingProxyType(dict(self.storage_options)),
            )


@dataclass(frozen=True, kw_only=True, slots=True)
class DestinationOutput(StorageOutput):
    """An output addressed by a local path or storage URI."""

    destination: Destination


@dataclass(frozen=True, kw_only=True, slots=True)
class ParquetOutput(DestinationOutput):
    compression: _ParquetCompression = "zstd"


@dataclass(frozen=True, kw_only=True, slots=True)
class CsvOutput(DestinationOutput):
    separator: str = ","
    include_header: bool = True


@dataclass(frozen=True, kw_only=True, slots=True)
class IpcOutput(DestinationOutput):
    compression: _IpcCompression = "uncompressed"


@dataclass(frozen=True, kw_only=True, slots=True)
class NdjsonOutput(DestinationOutput):
    pass


@dataclass(frozen=True, kw_only=True, slots=True)
class DeltaOutput(DestinationOutput):
    mode: _DeltaMode = "error"


@dataclass(frozen=True, kw_only=True, slots=True)
class DeltaUpsertOutput(DestinationOutput):
    keys: tuple[str, ...]

    def __post_init__(self) -> None:
        DestinationOutput.__post_init__(self)
        if (
            not self.keys
            or any(not isinstance(key, str) or not key for key in self.keys)
            or len(set(self.keys)) != len(self.keys)
        ):
            raise ValueError("Delta upsert keys must be unique non-empty strings")


@dataclass(frozen=True, kw_only=True, slots=True)
class IcebergOutput(OutputSpec):
    table: str
    catalog: str = "default"
    mode: _IcebergMode = "append"

    def __post_init__(self) -> None:
        if not self.table:
            raise ValueError("Iceberg table must be a non-empty identifier")
        if not self.catalog:
            raise ValueError("Iceberg catalog must be a non-empty name")


class Output:
    """Factories for executor-owned mart outputs."""

    @staticmethod
    def parquet(
        destination: Destination,
        *,
        compression: _ParquetCompression = "zstd",
        storage_options: StorageOptions | None = None,
    ) -> ParquetOutput:
        return ParquetOutput(
            destination=destination,
            compression=compression,
            storage_options=storage_options,
        )

    @staticmethod
    def csv(
        destination: Destination,
        *,
        separator: str = ",",
        include_header: bool = True,
        storage_options: StorageOptions | None = None,
    ) -> CsvOutput:
        return CsvOutput(
            destination=destination,
            separator=separator,
            include_header=include_header,
            storage_options=storage_options,
        )

    @staticmethod
    def ipc(
        destination: Destination,
        *,
        compression: _IpcCompression = "uncompressed",
        storage_options: StorageOptions | None = None,
    ) -> IpcOutput:
        return IpcOutput(
            destination=destination,
            compression=compression,
            storage_options=storage_options,
        )

    @staticmethod
    def ndjson(
        destination: Destination,
        *,
        storage_options: StorageOptions | None = None,
    ) -> NdjsonOutput:
        return NdjsonOutput(
            destination=destination,
            storage_options=storage_options,
        )

    @staticmethod
    def delta(
        destination: Destination,
        *,
        mode: _DeltaMode = "error",
        storage_options: StorageOptions | None = None,
    ) -> DeltaOutput:
        return DeltaOutput(
            destination=destination,
            mode=mode,
            storage_options=storage_options,
        )

    @staticmethod
    def delta_upsert(
        destination: Destination,
        *,
        keys: tuple[str, ...],
        storage_options: StorageOptions | None = None,
    ) -> DeltaUpsertOutput:
        return DeltaUpsertOutput(
            destination=destination,
            keys=keys,
            storage_options=storage_options,
        )

    @staticmethod
    def iceberg(
        table: str,
        *,
        catalog: str = "default",
        mode: _IcebergMode = "append",
    ) -> IcebergOutput:
        return IcebergOutput(
            table=table,
            catalog=catalog,
            mode=mode,
        )
