# Schemas and validation

Schemas define the contracts between models.

## Validation points

| Check | Compared values | When |
| --- | --- | --- |
| Source declaration | `source()` schema and `output_schema` | Build |
| Dependency edge | `Input.schema` and producer `output_schema` | Discovery |
| Transform output | Lazy plan schema and `output_schema` | Build or validation |

## Declare an output schema

Every model defines `output_schema`:

```python
import polars as pl

from polars_pipeliner import SourceModel


ORDER_SCHEMA = pl.Schema(
    {
        "order_id": pl.Int64,
        "amount": pl.Float64,
    }
)


class Orders(SourceModel):
    output_schema = ORDER_SCHEMA

    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {"order_id": [1], "amount": [10.0]},
            schema=ORDER_SCHEMA,
        )
```

The lazy frame returned by the model must have exactly these columns and data
types.

## Declare an input schema

Every consumer states the schema it expects from its producer:

```python
orders = Input("sources.orders", schema=ORDER_SCHEMA)
```

Discovery rejects the edge when:

- `sources.orders` does not exist;
- `ORDER_SCHEMA` differs from the producer's `output_schema`.

## Validate lazy plans

Use `project.validate()` to build every lazy plan and resolve its schema:

```python
schemas = project.validate()
orders_schema = schemas["staging.orders"]
```

Validation calls Polars `collect_schema()`. It does not collect result rows or
write mart outputs.

## What schema validation does not prove

A `pl.Schema` describes columns and data types. It does not prove:

- uniqueness;
- null policies;
- numeric or date ranges;
- referential integrity;
- business rules;
- CSV row contents.

Express row-level checks in Polars transformations or a separate data-quality
process.

## Reuse schema constants

Use descriptive module-level constants:

```python
import polars as pl

from polars_pipeliner import Input, Model


ORDER_SCHEMA = pl.Schema({"order_id": pl.Int64})


class Orders(Model):
    orders = Input("sources.orders", schema=ORDER_SCHEMA)
    output_schema = ORDER_SCHEMA

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
```

Each consumer still declares its own expected schema for every input.
