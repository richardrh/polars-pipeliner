# Polars Pipeliner

Build dependency-ordered Polars pipelines from ordinary Python files.

## What it does

Polars Pipeliner:

- discovers model files automatically;
- connects models through explicit `Input` declarations;
- validates input and output schemas;
- builds Polars `LazyFrame` query plans in dependency order;
- writes declared mart outputs.

## Pipeline shape

Place each model in the folder that describes its role:

```text
sources/ → staging/ → intermediate/ → marts/
```

The file path becomes the node ID:

```text
sources/orders.py        → sources.orders
staging/clean_orders.py  → staging.clean_orders
marts/orders.py          → marts.orders
```

## Minimal model

```python
import polars as pl

from polars_pipeliner import SourceModel


ORDER_SCHEMA = pl.Schema({"order_id": pl.Int64})


class Orders(SourceModel):
    output_schema = ORDER_SCHEMA

    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({"order_id": [1, 2]}, schema=ORDER_SCHEMA)
```

Drop this file into `sources/orders.py`. Polars Pipeliner discovers it as
`sources.orders`.

## Best fit

Use Polars Pipeliner for:

- small and medium analytical pipelines;
- schema-first Polars transformations;
- self-contained generated query projects;
- workloads that benefit from lazy execution.

It is not a general-purpose scheduler or warehouse orchestrator.

## Start here

- [Install the package and create a pipeline](/getting-started)
- [Understand the model types](/concepts/models)
- [Use the Python API](/reference/api)
