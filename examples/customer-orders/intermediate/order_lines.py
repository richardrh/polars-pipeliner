import polars as pl

from polars_pipeliner import Input, Model


class OrderLines(Model):
    orders = Input(
        "staging.orders",
        schema=pl.Schema(
            {
                "order_id": pl.Int64,
                "customer_id": pl.Int64,
                "product_id": pl.Int64,
                "order_date": pl.Date,
                "quantity": pl.Int64,
            }
        ),
    )
    products = Input(
        "staging.products",
        schema=pl.Schema(
            {
                "product_id": pl.Int64,
                "product_name": pl.String,
                "category": pl.String,
                "unit_price": pl.Float64,
            }
        ),
    )
    output_schema = pl.Schema(
        {
            "order_id": pl.Int64,
            "customer_id": pl.Int64,
            "product_id": pl.Int64,
            "order_date": pl.Date,
            "quantity": pl.Int64,
            "product_name": pl.String,
            "category": pl.String,
            "unit_price": pl.Float64,
            "line_total": pl.Float64,
        }
    )

    def transform(self, orders: pl.LazyFrame, products: pl.LazyFrame) -> pl.LazyFrame:
        return orders.join(products, on="product_id", how="inner").select(
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "product_name",
            "category",
            "unit_price",
            (pl.col("quantity") * pl.col("unit_price")).alias("line_total"),
        )
