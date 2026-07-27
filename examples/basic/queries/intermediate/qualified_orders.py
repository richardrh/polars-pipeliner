import polars as pl

from polars_pipeliner import PolarsModel, QueryMetadata, QuerySource

RAW_ORDERS = pl.Schema(
    {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
)
QUALIFIED_ORDERS = pl.Schema({"country": pl.String, "amount": pl.Float64})


class Model(PolarsModel):
    metadata = QueryMetadata(
        inputs={"orders": QuerySource(node_id="staging.orders", schema=RAW_ORDERS)},
        output_schema=QUALIFIED_ORDERS,
    )

    @classmethod
    def transform(cls, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("amount") >= 5.0).select("country", "amount")
