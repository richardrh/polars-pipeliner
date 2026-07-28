from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel

PRODUCTS = pl.Schema(
    {
        "product_id": pl.Int64,
        "product_name": pl.String,
        "category": pl.String,
        "unit_price": pl.Float64,
    }
)


class Products(SourceModel):
    output_schema = PRODUCTS

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds" / "products.csv",
            schema_overrides=PRODUCTS,
        )
