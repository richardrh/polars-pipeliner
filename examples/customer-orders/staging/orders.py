import polars as pl

from polars_pipeliner import Input, Model

ORDERS = pl.Schema(
    {
        "order_id": pl.Int64,
        "customer_id": pl.Int64,
        "product_id": pl.Int64,
        "order_date": pl.Date,
        "quantity": pl.Int64,
    }
)


class Orders(Model):
    inputs = {"orders": Input("sources.orders", schema=ORDERS)}
    output_schema = ORDERS

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("quantity") > 0).select(
            "order_id", "customer_id", "product_id", "order_date", "quantity"
        )
