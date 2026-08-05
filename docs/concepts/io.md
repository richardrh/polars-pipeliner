# Sources and outputs

## Source contract

A `SourceModel.source()` method returns a genuine Polars `LazyFrame`.

### Valid lazy source

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
        return pl.scan_parquet("data/orders.parquet")
```

Common lazy operations include:

- `pl.scan_csv()`;
- `pl.scan_parquet()`;
- `pl.scan_ipc()`;
- other Polars operations that return `LazyFrame`.

The contract is the returned lazy frame and its declared `output_schema`, not a
specific storage format.

## Database reads

`pl.read_database()` and `pl.read_database_uri()` are eager operations.

```python
frame = pl.read_database(query="SELECT * FROM orders", connection=connection)
lazy_frame = frame.lazy()
```

In this example:

- the database read has already happened;
- the data is already in memory;
- `.lazy()` only defers later transformations.

For large database sources:

- filter and select columns in SQL;
- or stage the result in Parquet, Delta, or Iceberg.

## Mart outputs

Every `MartModel` declares one output.

| Factory | Default behavior |
| --- | --- |
| `Output.parquet()` | Parquet with Zstandard compression |
| `Output.csv()` | CSV with header |
| `Output.ipc()` | Uncompressed Arrow IPC |
| `Output.ndjson()` | Newline-delimited JSON |
| `Output.delta()` | Error if the destination exists |
| `Output.iceberg()` | Append |

### Valid output declaration

```python
import polars as pl

from polars_pipeliner import Input, MartModel, Output


ORDER_SCHEMA = pl.Schema({"order_id": pl.Int64})


class Orders(MartModel):
    orders = Input("intermediate.orders", schema=ORDER_SCHEMA)
    output_schema = ORDER_SCHEMA
    output = Output.parquet(
        "target/orders.parquet",
        compression="zstd",
    )

    def transform(self, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders
```

The model returns a lazy frame. The executor owns the write operation.

## Execution behavior

| Output family | Behavior |
| --- | --- |
| Parquet, CSV, IPC, NDJSON | Composable lazy sinks grouped by the executor |
| Delta, Iceberg | Direct Polars sinks after all plans pass schema validation |

## Destinations

Destinations may be:

- relative local paths, resolved from the project root;
- absolute paths;
- storage URIs supported by Polars.

Delta and Iceberg outputs accept paths or URIs. They do not use a separate
catalog or table-identifier API.
