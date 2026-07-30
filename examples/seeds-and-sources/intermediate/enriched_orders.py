import polars as pl

from polars_pipeliner import Input, Model


class EnrichedOrders(Model):
    countries = Input(
        "sources.countries",
        schema=pl.Schema({"country": pl.String, "region": pl.String}),
    )
    orders = Input(
        "staging.orders",
        schema=pl.Schema(
            {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
        ),
    )
    output_schema = pl.Schema(
        {
            "order_id": pl.Int64,
            "country": pl.String,
            "amount": pl.Float64,
            "region": pl.String,
        }
    )

    def transform(self, countries: pl.LazyFrame, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.join(countries, on="country", how="inner")
