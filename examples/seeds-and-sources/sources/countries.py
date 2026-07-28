from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel

COUNTRIES = pl.Schema({"country": pl.String, "region": pl.String})


class Countries(SourceModel):
    output_schema = COUNTRIES

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds" / "countries.csv",
            schema_overrides=COUNTRIES,
        )
