import polars as pl

from polars_pipeliner import Input, Model


class Orders(Model):
    orders = Input(
        "sources.orders",
        schema=pl.Schema(
            {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
        ),
    )
    output_schema = pl.Schema(
        {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
    )

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("amount") > 0)
