import polars as pl

from polars_pipeliner import Input, MartModel, Output

CUSTOMERS = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
    }
)
ORDER_LINES = pl.Schema(
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
CUSTOMER_ORDERS = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
        "order_count": pl.UInt32,
        "units_ordered": pl.Int64,
        "total_spend": pl.Float64,
    }
)


class CustomerOrders(MartModel):
    inputs = {
        "customers": Input("staging.customers", schema=CUSTOMERS),
        "order_lines": Input("intermediate.order_lines", schema=ORDER_LINES),
    }
    output_schema = CUSTOMER_ORDERS
    output = Output.parquet("target/customer_orders.parquet")

    def transform(
        self, customers: pl.LazyFrame, order_lines: pl.LazyFrame
    ) -> pl.LazyFrame:
        customer_totals = order_lines.group_by("customer_id").agg(
            pl.len().alias("order_count"),
            pl.col("quantity").sum().alias("units_ordered"),
            pl.col("line_total").sum().alias("total_spend"),
        )
        return (
            customers.join(customer_totals, on="customer_id", how="left")
            .with_columns(
                pl.col("order_count").fill_null(0),
                pl.col("units_ordered").fill_null(0),
                pl.col("total_spend").fill_null(0.0),
            )
            .select(
                "customer_id",
                "customer_name",
                "segment",
                "order_count",
                "units_ordered",
                "total_spend",
            )
            .sort("customer_id")
        )
