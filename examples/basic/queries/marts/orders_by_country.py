import polars as pl

from polars_pipeliner import PolarsModel, QueryMetadata, QuerySource

QUALIFIED_ORDERS = pl.Schema({"country": pl.String, "amount": pl.Float64})
ORDERS_BY_COUNTRY = pl.Schema({"country": pl.String, "total_amount": pl.Float64})


class Model(PolarsModel):
    metadata = QueryMetadata(
        inputs={
            "orders": QuerySource(
                node_id="intermediate.qualified_orders", schema=QUALIFIED_ORDERS
            )
        },
        output_schema=ORDERS_BY_COUNTRY,
    )

    @classmethod
    def transform(cls, orders: pl.LazyFrame) -> pl.LazyFrame:
        return (
            orders.group_by("country")
            .agg(pl.col("amount").sum().alias("total_amount"))
            .sort("country")
        )
