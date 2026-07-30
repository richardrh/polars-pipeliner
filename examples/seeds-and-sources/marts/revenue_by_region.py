import polars as pl

from polars_pipeliner import Input, MartModel, Output


class RevenueByRegion(MartModel):
    orders = Input(
        "intermediate.enriched_orders",
        schema=pl.Schema(
            {
                "order_id": pl.Int64,
                "country": pl.String,
                "amount": pl.Float64,
                "region": pl.String,
            }
        ),
    )
    output_schema = pl.Schema({"region": pl.String, "revenue": pl.Float64})
    output = Output.parquet("target/revenue_by_region.parquet")

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return (
            orders.group_by("region")
            .agg(pl.col("amount").sum().alias("revenue"))
            .sort("region")
        )
