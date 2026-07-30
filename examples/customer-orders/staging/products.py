import polars as pl

from polars_pipeliner import Input, Model


class Products(Model):
    products = Input(
        "sources.products",
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
            "product_id": pl.Int64,
            "product_name": pl.String,
            "category": pl.String,
            "unit_price": pl.Float64,
        }
    )

    def transform(self, products: pl.LazyFrame) -> pl.LazyFrame:
        return products.filter(pl.col("unit_price") > 0).select(
            "product_id",
            pl.col("product_name").str.strip_chars().alias("product_name"),
            pl.col("category").str.strip_chars().alias("category"),
            "unit_price",
        )
