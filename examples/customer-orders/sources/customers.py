from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel


class Customers(SourceModel):
    output_schema = pl.Schema(
        {
            "customer_id": pl.Int64,
            "customer_name": pl.String,
            "segment": pl.String,
        }
    )

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds" / "customers.csv",
            schema_overrides=pl.Schema(
                {
                    "customer_id": pl.Int64,
                    "customer_name": pl.String,
                    "segment": pl.String,
                }
            ),
        )
