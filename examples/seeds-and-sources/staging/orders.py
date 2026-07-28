import polars as pl

from polars_pipeliner import Input, Model

ORDERS = pl.Schema({"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64})


class Orders(Model):
    inputs = {"orders": Input("sources.orders", schema=ORDERS)}
    output_schema = ORDERS

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("amount") > 0)
