from pathlib import Path

import polars as pl

from polars_pipeliner import CsvSource, PolarsModel, QueryMetadata

RAW_ORDERS = pl.Schema(
    {
        "order_id": pl.Int64,
        "country": pl.String,
        "amount": pl.Float64,
    }
)


class Model(PolarsModel):
    metadata = QueryMetadata(
        inputs={
            "orders": CsvSource(
                path=Path(__file__).parents[2] / "orders.csv", schema=RAW_ORDERS
            )
        },
        output_schema=RAW_ORDERS,
    )

    @classmethod
    def transform(cls, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
