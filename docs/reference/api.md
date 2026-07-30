# Public API

Import the public API from `polars_pipeliner`:

```python
from polars_pipeliner import (
    Input,
    MartModel,
    Model,
    Output,
    Project,
    ProjectConfig,
    SourceModel,
    discover,
    load_config,
)
```

## Model contracts

### Source model

`SourceModel` belongs in `sources/`, declares `output_schema: pl.Schema`, has
no `Input` attributes, and implements `source(self) -> pl.LazyFrame`.

```python
import polars as pl

from polars_pipeliner import SourceModel


ORDERS_SCHEMA = pl.Schema({"order_id": pl.Int64, "customer_id": pl.Int64})


class Orders(SourceModel):
    output_schema = ORDERS_SCHEMA

    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({"order_id": [], "customer_id": []}, schema=ORDERS_SCHEMA)
```

### Transform model

`Model` belongs in `staging/` or `intermediate/`, declares
`output_schema: pl.Schema`, and uses a direct `Input`-valued class attribute
for each upstream frame. Each attribute name must exactly match a normal
`transform(self, ...named LazyFrames...) -> pl.LazyFrame` parameter. For
example, `orders = Input("sources.orders", schema=ORDERS_SCHEMA)` declares the
`orders` argument. Transform models may declare zero, one, or multiple inputs.

```python
import polars as pl

from polars_pipeliner import Input, Model


ORDERS_SCHEMA = pl.Schema({"order_id": pl.Int64, "customer_id": pl.Int64})
CUSTOMERS_SCHEMA = pl.Schema({"customer_id": pl.Int64, "name": pl.String})
ENRICHED_ORDERS_SCHEMA = pl.Schema(
    {"order_id": pl.Int64, "customer_id": pl.Int64, "name": pl.String}
)


class EnrichedOrders(Model):
    orders = Input("sources.orders", schema=ORDERS_SCHEMA)
    customers = Input("staging.customers", schema=CUSTOMERS_SCHEMA)
    output_schema = ENRICHED_ORDERS_SCHEMA

    def transform(
        self, orders: pl.LazyFrame, customers: pl.LazyFrame
    ) -> pl.LazyFrame:
        return orders.join(customers, on="customer_id")
```

### Mart model

`MartModel` belongs in `marts/`, follows the transform model contract, and also
declares `output: OutputSpec`. Use `Output.parquet`,
`Output.csv`, `Output.ipc`, `Output.ndjson`, `Output.delta`, or
`Output.iceberg` to create it. Parquet compression accepts `snappy`, `gzip`,
`lzo`, `brotli`, `lz4`, or `zstd`; IPC compression accepts `uncompressed`,
`lz4`, or `zstd`; Delta modes are `error`, `append`, `overwrite`, or `ignore`;
and Iceberg modes are `append` or `overwrite`.

```python
import polars as pl

from polars_pipeliner import Input, MartModel, Output


REVENUE_SCHEMA = pl.Schema({"region": pl.String, "revenue": pl.Float64})


class RevenueByRegion(MartModel):
    revenue = Input("intermediate.revenue", schema=REVENUE_SCHEMA)
    output_schema = REVENUE_SCHEMA
    output = Output.parquet("target/revenue_by_region.parquet")

    def transform(self, revenue: pl.LazyFrame) -> pl.LazyFrame:
        return revenue
```

`Input(node_id, schema=...)` creates an immutable upstream binding. `schema` is
keyword-only. The old `inputs = {"orders": Input(...)}` mapping declaration is
not supported.

`discover(query_root, *, config=None, config_path=None)` returns a `Project`.
Passing both configuration arguments is an error. `Project.resolve()` returns
the topological node IDs, `Project.build()` returns validated lazy frames, and
`Project.validate()` returns an immutable node-to-resolved-schema mapping without
row collection or mart writes. It constructs model plans and calls
`collect_schema()`, which may read file metadata or make network requests; it
cannot prove row values, uniqueness, ranges, null constraints, or business
correctness. `Project.run()` takes no arguments and returns the materialized mart
manifest.

Validation JSONL final events include a fail-fast summary with
`models_found`, `models_verified`, `models_failed`, and `failed_models`; counts
cover only schemas verified before the first failure.

`ProjectConfig(log_level="WARNING")` and `load_config(path=None)` provide the
configuration API. The public package also exports `ConfigError`, `DiscoveryError`,
`ModelValidationError`, `QueryBuildError`, `QueryError`, `QueryExecutionError`,
and `QueryValidationError`.
