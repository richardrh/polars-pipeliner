# Setup and run

## Install

Use Python 3.13 or later. Add the package to a uv project:

```bash
uv add polars-pipeliner
```

For this source checkout, install the project and its development tools:

```bash
uv sync --all-groups
```

## Place files

Keep source data and query files in your project. A minimal layout is:

```text
my-project/
├── data/
│   └── orders.csv
└── queries/
    └── staging/
        └── orders.py
```

## Minimal working example

Create `queries/staging/orders.py`:

```python
from pathlib import Path

import polars as pl

from polars_pipeliner import CsvSource, PolarsModel, QueryMetadata

ORDERS = pl.Schema({"order_id": pl.Int64, "amount": pl.Float64})


class Model(PolarsModel):
    metadata = QueryMetadata(
        inputs={
            "orders": CsvSource(
                path=Path(__file__).parents[2] / "data" / "orders.csv",
                schema=ORDERS,
            )
        },
        output_schema=ORDERS,
    )

    @classmethod
    def transform(cls, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
```

Run a target from Python:

```bash
uv run python -c 'from polars_pipeliner import discover; print(discover("queries").run(["staging.orders"]))'
```

Run the repository example exactly as shipped:

```bash
uv run python examples/basic/run.py
```

Build this documentation site locally:

```bash
uv run zensical build --strict
```
