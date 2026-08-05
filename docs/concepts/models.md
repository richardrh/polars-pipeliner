# Model types

Every discovered file defines one concrete model class.

## Placement

| Folder | Model class | Purpose | Writes output |
| --- | --- | --- | --- |
| `sources/` | `SourceModel` | Introduce data | No |
| `staging/` | `Model` | Clean or normalize source data | No |
| `intermediate/` | `Model` | Build reusable transformations | No |
| `marts/` | `MartModel` | Produce final datasets | Yes |

Only Python files below these four folders are discovered. Python files in
other folders do not become pipeline nodes.

## `SourceModel`

### Define

- `output_schema`;
- an ordinary `source()` instance method;
- no `Input` attributes.

### Place

Drop the file into `sources/`.

### Example

```python
import polars as pl

from polars_pipeliner import SourceModel


ORDER_SCHEMA = pl.Schema({"order_id": pl.Int64})


class Orders(SourceModel):
    output_schema = ORDER_SCHEMA

    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({"order_id": [1, 2]}, schema=ORDER_SCHEMA)
```

## `Model`

### Define

- one direct `Input` class attribute for each upstream frame;
- `output_schema`;
- an ordinary `transform()` instance method.

The `Input` attribute names must match the `transform()` parameter names.

### Place

Drop the file into:

- `staging/` for source cleanup or normalization;
- `intermediate/` for reusable downstream transformations.

### Example

```python
import polars as pl

from polars_pipeliner import Input, Model


ORDER_SCHEMA = pl.Schema({"order_id": pl.Int64})


class Orders(Model):
    orders = Input("sources.orders", schema=ORDER_SCHEMA)
    output_schema = ORDER_SCHEMA

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("order_id") > 0)
```

## `MartModel`

### Define

- the same inputs, schema, and method as `Model`;
- one `output` declaration.

### Place

Drop the file into `marts/`.

### Example

```python
import polars as pl

from polars_pipeliner import Input, MartModel, Output


ORDER_SCHEMA = pl.Schema({"order_id": pl.Int64})


class Orders(MartModel):
    orders = Input("intermediate.orders", schema=ORDER_SCHEMA)
    output_schema = ORDER_SCHEMA
    output = Output.parquet("target/orders.parquet")

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
```

Every discovered mart is written when `project.run()` executes.

## Dependency declarations

Declare dependencies directly on the class:

```python
orders = Input("sources.orders", schema=ORDER_SCHEMA)
```

This declaration means:

| Part | Meaning |
| --- | --- |
| `orders` | The `transform()` parameter name |
| `"sources.orders"` | The upstream node ID |
| `schema=ORDER_SCHEMA` | The expected upstream schema |

Mapping-style declarations are not supported:

```python
# Invalid
inputs = {"orders": Input("sources.orders", schema=ORDER_SCHEMA)}
```
