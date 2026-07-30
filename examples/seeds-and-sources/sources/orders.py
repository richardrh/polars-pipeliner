from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel


class Orders(SourceModel):
    output_schema = pl.Schema(
        {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
    )

    def source(self) -> pl.LazyFrame:
        return pl.scan_parquet(Path(__file__).parents[1] / "data" / "orders.parquet")
