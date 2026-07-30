# polars-pipeliner

polars-pipeliner discovers a scoped set of native Polars `LazyFrame` models,
validates their declared contracts, and materializes every mart in the graph.
Its distribution and configuration name is `polars-pipeliner`; Python imports
and logs use `polars_pipeliner`.

```python
import polars as pl

from polars_pipeliner import Input, MartModel, Output


ORDER_LINES_SCHEMA = pl.Schema({"customer_id": pl.Int64, "line_total": pl.Float64})
CUSTOMER_ORDERS_SCHEMA = pl.Schema({"customer_id": pl.Int64, "total_spend": pl.Float64})


class CustomerOrders(MartModel):
    order_lines = Input(
        "intermediate.order_lines",
        schema=ORDER_LINES_SCHEMA,
    )
    output_schema = CUSTOMER_ORDERS_SCHEMA
    output = Output.parquet("target/customer_orders.parquet")

    def transform(self, order_lines: pl.LazyFrame) -> pl.LazyFrame:
        return order_lines.group_by("customer_id").agg(
            pl.col("line_total").sum().alias("total_spend")
        )
```

Start with [Getting started](getting-started.md), then learn the model
[concepts](concepts/models.md) or consult the [reference](reference/api.md).
