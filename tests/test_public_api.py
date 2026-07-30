from __future__ import annotations

from dataclasses import FrozenInstanceError
from inspect import Parameter, signature
from typing import Any, cast

import polars as pl
import pytest

from polars_pipeliner import Input, MartModel, Model, Output, SourceModel


def test_input_is_immutable_and_has_the_public_signature() -> None:
    schema = pl.Schema({"id": pl.Int64})
    binding = Input("sources.orders", schema=schema)

    assert binding.node_id == "sources.orders"
    assert binding.schema == schema
    assert tuple(signature(Input).parameters) == ("node_id", "schema")
    assert signature(Input).parameters["schema"].kind is Parameter.KEYWORD_ONLY
    with pytest.raises(FrozenInstanceError):
        cast(Any, binding).node_id = "other"


def test_output_factories_return_immutable_typed_specs() -> None:
    specs = (
        Output.parquet("a.parquet"),
        Output.csv("a.csv"),
        Output.ipc("a.ipc"),
        Output.ndjson("a.ndjson"),
        Output.delta("table"),
        Output.iceberg("table"),
    )

    assert [type(spec).__name__ for spec in specs] == [
        "ParquetOutput",
        "CsvOutput",
        "IpcOutput",
        "NdjsonOutput",
        "DeltaOutput",
        "IcebergOutput",
    ]
    with pytest.raises(FrozenInstanceError):
        cast(Any, specs[0]).destination = "other"


def test_public_model_classes_use_ordinary_instance_methods() -> None:
    class Orders(SourceModel):
        output_schema = pl.Schema({"id": pl.Int64})

        def source(self) -> pl.LazyFrame:
            return pl.LazyFrame({"id": [1]})

    class CleanOrders(Model):
        orders = Input("sources.orders", schema=Orders.output_schema)
        output_schema = Orders.output_schema

        def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
            return orders

    class OrdersMart(MartModel):
        orders = Input("sources.orders", schema=Orders.output_schema)
        output_schema = CleanOrders.output_schema
        output = Output.parquet("target/orders.parquet")

        def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
            return orders

    assert isinstance(Orders().source(), pl.LazyFrame)
    assert isinstance(CleanOrders().transform(Orders().source()), pl.LazyFrame)
    assert isinstance(OrdersMart().output, type(Output.parquet("x")))
