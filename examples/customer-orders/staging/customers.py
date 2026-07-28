import polars as pl

from polars_pipeliner import Input, Model

CUSTOMERS = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
    }
)


class Customers(Model):
    inputs = {"customers": Input("sources.customers", schema=CUSTOMERS)}
    output_schema = CUSTOMERS

    def transform(self, customers: pl.LazyFrame) -> pl.LazyFrame:
        return customers.select(
            "customer_id",
            pl.col("customer_name").str.strip_chars().alias("customer_name"),
            pl.col("segment").str.strip_chars().alias("segment"),
        )
