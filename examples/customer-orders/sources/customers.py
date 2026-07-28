from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel

CUSTOMERS = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
    }
)


class Customers(SourceModel):
    output_schema = CUSTOMERS

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds" / "customers.csv",
            schema_overrides=CUSTOMERS,
        )
