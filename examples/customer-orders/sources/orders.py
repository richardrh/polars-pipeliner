from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel


class Orders(SourceModel):
    output_schema = pl.Schema(
        {
            "order_id": pl.Int64,
            "customer_id": pl.Int64,
            "product_id": pl.Int64,
            "order_date": pl.Date,
            "quantity": pl.Int64,
        }
    )

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds" / "orders.csv",
            schema_overrides=pl.Schema(
                {
                    "order_id": pl.Int64,
                    "customer_id": pl.Int64,
                    "product_id": pl.Int64,
                    "order_date": pl.Date,
                    "quantity": pl.Int64,
                }
            ),
        )
