# polars-pipeliner

## 1. What is polars-pipeliner?

`polars-pipeliner` automatically runs polars in a dbt-inspired data pipeline.
It leverages Polars lazy query functionality to run the entire pipeline without
running queries one by one.

This is suitable for small to medium workloads and larger than memory datasets
and agentic workflows.

It contains validation logic at each step to check the schema of the input, and
the output dataframes.

It defines an interface for polars queries that are automatically discovered
and dispatched.

Polars-build-tool is used at Variance Zero with coding agents.

## 2. What problem does it solve?

Dbt is heavyweight, sql is not type checked. Polars fills that void but needs
an orchestrator.

`polars-pipeliner` supports any Polars data source that returns a `LazyFrame`.

- ordering dependent queries and tracking implicit dependencies;
- avoiding duplicate scans of the same physical source;
- finding schema mismatches only after an expensive collection; and
- collecting unrelated work when only selected outputs are needed.

## 3. Code example showing how it works

Create this small project:

```text
my-project/
├── data/
│   └── orders.csv
├── queries/
│   ├── staging/
│   │   └── orders.py
│   └── marts/
│       └── orders_by_country.py
└── run.py
```

`data/orders.csv`:

```csv
order_id,country,amount
1,US,10.5
2,GB,7.25
3,US,4.5
```

`queries/staging/orders.py` declares a physical source. `Path(__file__)` keeps
the data path independent of the current working directory.

```python
from pathlib import Path

import polars as pl

from polars_pipeliner import CsvSource, PolarsModel, QueryMetadata

RAW_ORDERS = pl.Schema(
    {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
)


class Model(PolarsModel):
    metadata = QueryMetadata(
        inputs={
            "orders": CsvSource(
                path=Path(__file__).parents[2] / "data" / "orders.csv",
                schema=RAW_ORDERS,
            )
        },
        output_schema=RAW_ORDERS,
    )

    @classmethod
    def transform(cls, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
```

`queries/marts/orders_by_country.py` depends on `staging.orders`. A
`QuerySource` schema must exactly equal the producer's declared output schema.

```python
import polars as pl

from polars_pipeliner import PolarsModel, QueryMetadata, QuerySource

RAW_ORDERS = pl.Schema(
    {"order_id": pl.Int64, "country": pl.String, "amount": pl.Float64}
)
ORDERS_BY_COUNTRY = pl.Schema({"country": pl.String, "total_amount": pl.Float64})


class Model(PolarsModel):
    metadata = QueryMetadata(
        inputs={"orders": QuerySource(node_id="staging.orders", schema=RAW_ORDERS)},
        output_schema=ORDERS_BY_COUNTRY,
    )

    @classmethod
    def transform(cls, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.group_by("country").agg(
            pl.col("amount").sum().alias("total_amount")
        )
```

## 4. How to install/run it

Requirements: Python >=3.13 and Polars >=1.20.0.

Once the package is published on PyPI, add it to your uv project:

```bash
uv add polars-pipeliner
```

For contributor or pre-release development from a source checkout:

```bash
uv pip install -e .
```

Run the bundled example from this repository:

```bash
uv run python examples/basic/run.py
```

See the runnable project at [`examples/basic`](examples/basic). Its output
shows the resolved order, a built intermediate lazy frame, and the collected
target `DataFrame`.

### Notes

`CsvSource` and `ParquetSource` are built-in convenience classes; they accept
local paths and Polars-supported cloud URIs. Other Polars sources that produce a
`LazyFrame` can participate through a custom implementation of the public
`Source` protocol. Keep cloud credentials out of metadata and configure them
through Polars. `pl.read_database*` executes immediately and loads its result
into an in-memory `DataFrame`; calling `.lazy()` afterward defers only later
transformations, not the database read. For large database sources, filter and
project in SQL or stage them to a lazily scannable format such as Parquet, Delta,
or Iceberg.
Maintainers can run `uv lock`, `uv sync --all-groups`, `uv run pytest -q`,
`uv run mypy`, `uv run ty check`, and Ruff checks before building with
`uv build --no-sources`.
