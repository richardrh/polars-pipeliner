import polars as pl

from polars_pipeliner import Input, MartModel, Output

ENRICHED_ORDERS = pl.Schema(
    {
        "order_id": pl.Int64,
        "country": pl.String,
        "amount": pl.Float64,
        "region": pl.String,
    }
)
REVENUE_BY_REGION = pl.Schema({"region": pl.String, "revenue": pl.Float64})


class RevenueByRegion(MartModel):
    inputs = {"orders": Input("intermediate.enriched_orders", schema=ENRICHED_ORDERS)}
    output_schema = REVENUE_BY_REGION
    output = Output.parquet("target/revenue_by_region.parquet")

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return (
            orders.group_by("region")
            .agg(pl.col("amount").sum().alias("revenue"))
            .sort("region")
        )
