import polars as pl

from polars_pipeliner import Input, Model

COUNTRIES = pl.Schema({"country": pl.String, "region": pl.String})
ORDERS = pl.Schema({"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64})
ENRICHED_ORDERS = pl.Schema(
    {
        "order_id": pl.Int64,
        "country": pl.String,
        "amount": pl.Float64,
        "region": pl.String,
    }
)


class EnrichedOrders(Model):
    inputs = {
        "countries": Input("sources.countries", schema=COUNTRIES),
        "orders": Input("staging.orders", schema=ORDERS),
    }
    output_schema = ENRICHED_ORDERS

    def transform(self, countries: pl.LazyFrame, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.join(countries, on="country", how="inner")
