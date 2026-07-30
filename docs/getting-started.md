# Getting started

The canonical `examples/customer-orders` project reads customers, products, and
orders; joins them into order lines; and writes one customer-order mart.

Install the distribution from a project that uses Python 3.13+:

```bash
uv add polars-pipeliner
```

From this repository, run the example exactly as shipped:

```bash
uv run python examples/customer-orders/run.py
```

The runner discovers the project root and runs all marts without targets:

```python
from pathlib import Path

from polars_pipeliner import discover

root = Path(__file__).parent
manifest = discover(root).run()
print(manifest["marts.customer_orders"])
```

To resolve and validate every model schema without writing the mart, call
`discover(root).validate()` or run:

```bash
uv run polars-pipeliner validate examples/customer-orders --config examples/customer-orders/polars-pipeliner.toml
```

The source in `sources/customers.py` declares the schema it supplies and returns
a lazy scan:

```python
from pathlib import Path

import polars as pl

from polars_pipeliner import SourceModel


CUSTOMERS_SCHEMA = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
    }
)


class Customers(SourceModel):
    output_schema = CUSTOMERS_SCHEMA

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds" / "customers.csv",
            schema_overrides=CUSTOMERS_SCHEMA,
        )
```

The mart in `marts/customer_orders.py` declares its two upstream edges and its
output. Its normal `transform()` method receives the matching named plans:

```python
import polars as pl

from polars_pipeliner import Input, MartModel, Output


CUSTOMERS_SCHEMA = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
    }
)
ORDER_LINES_SCHEMA = pl.Schema(
    {
        "order_id": pl.Int64,
        "customer_id": pl.Int64,
        "product_id": pl.Int64,
        "order_date": pl.Date,
        "quantity": pl.Int64,
        "product_name": pl.String,
        "category": pl.String,
        "unit_price": pl.Float64,
        "line_total": pl.Float64,
    }
)
CUSTOMER_ORDERS_SCHEMA = pl.Schema(
    {
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
        "order_count": pl.UInt32,
        "units_ordered": pl.Int64,
        "total_spend": pl.Float64,
    }
)


class CustomerOrders(MartModel):
    customers = Input("staging.customers", schema=CUSTOMERS_SCHEMA)
    order_lines = Input("intermediate.order_lines", schema=ORDER_LINES_SCHEMA)
    output_schema = CUSTOMER_ORDERS_SCHEMA
    output = Output.parquet("target/customer_orders.parquet")

    def transform(
        self, customers: pl.LazyFrame, order_lines: pl.LazyFrame
    ) -> pl.LazyFrame:
        customer_totals = order_lines.group_by("customer_id").agg(
            pl.len().alias("order_count"),
            pl.col("quantity").sum().alias("units_ordered"),
            pl.col("line_total").sum().alias("total_spend"),
        )
        return (
            customers.join(customer_totals, on="customer_id", how="left")
            .with_columns(
                pl.col("order_count").fill_null(0),
                pl.col("units_ordered").fill_null(0),
                pl.col("total_spend").fill_null(0.0),
            )
            .select(
                "customer_id",
                "customer_name",
                "segment",
                "order_count",
                "units_ordered",
                "total_spend",
            )
            .sort("customer_id")
        )
```

See [Model types](concepts/models.md) for the placement and contract rules.
