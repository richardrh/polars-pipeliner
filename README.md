# polars-build-tool

## What is it?

`polars-build-tool` discovers and runs dependency-ordered Polars `LazyFrame`
models. It validates model schemas before writing any mart output.

## What problem does it solve?

Polars provides fast, typed transformations; this package supplies a small,
file-based project layout for connecting sources, transformations, and durable
mart outputs without query wrappers or decorators.

## Example

```text
project/
├── seeds/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
├── sources/
│   ├── customers.py
│   ├── products.py
│   └── orders.py
├── staging/
│   ├── customers.py
│   ├── products.py
│   └── orders.py
├── intermediate/order_lines.py
├── marts/customer_orders.py
└── run.py
```

`sources/customers.py` contains an ordinary instance method with an explicit
source contract:

```python
from pathlib import Path
import polars as pl
from polars_pipeliner import SourceModel

CUSTOMERS = pl.Schema(
    {"customer_id": pl.Int64, "customer_name": pl.String, "segment": pl.String}
)


class Customers(SourceModel):
    output_schema = CUSTOMERS

    def source(self) -> pl.LazyFrame:
        return pl.scan_csv(
            Path(__file__).parents[1] / "seeds/customers.csv",
            schema_overrides=CUSTOMERS,
        )
```

Staged orders and products join into typed order lines with `Input` bindings:

```python
from polars_pipeliner import Input, Model


class OrderLines(Model):
    inputs = {
        "orders": Input("staging.orders", schema=ORDERS),
        "products": Input("staging.products", schema=PRODUCTS),
    }
    output_schema = ORDER_LINES

    def transform(self, orders: pl.LazyFrame, products: pl.LazyFrame) -> pl.LazyFrame:
        return orders.join(products, on="product_id").with_columns(
            (pl.col("quantity") * pl.col("unit_price")).alias("line_total")
        )
```

The customer mart left-joins those totals so customers with no orders remain in
the result, with zero-filled aggregates. It declares an executor-owned output:

```python
from polars_pipeliner import Input, MartModel, Output


class CustomerOrders(MartModel):
    inputs = {
        "customers": Input("staging.customers", schema=CUSTOMERS),
        "order_lines": Input("intermediate.order_lines", schema=ORDER_LINES),
    }
    output_schema = CUSTOMER_ORDERS
    output = Output.parquet("target/customer_orders.parquet")

    def transform(
        self, customers: pl.LazyFrame, order_lines: pl.LazyFrame
    ) -> pl.LazyFrame:
        customer_totals = order_lines.group_by("customer_id").agg(
            pl.len().alias("order_count"),
            pl.col("quantity").sum().alias("units_ordered"),
            pl.col("line_total").sum().alias("total_spend"),
        )
        return customers.join(customer_totals, on="customer_id", how="left").with_columns(
            pl.col("order_count").fill_null(0),
            pl.col("units_ordered").fill_null(0),
            pl.col("total_spend").fill_null(0.0),
        )
```

## Install and run

Requires Python 3.13+ and Polars 1.38.1+. Install with `uv add polars-build-tool`,
then run every mart:

```python
from polars_pipeliner import discover

manifest = discover("project").run()
print(manifest)
```

Run the bundled examples with `uv run python examples/customer-orders/run.py` or
`uv run python examples/seeds-and-sources/run.py`.
