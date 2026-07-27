# Conformity

## Model contract

Put one public Python query file under the query root. Its path becomes its node
ID: `queries/staging/orders.py` is `staging.orders`. Files or directories whose
names start with `_` are ignored.

Each query file must define exactly one concrete `PolarsModel` subclass. It must
define:

- `metadata = QueryMetadata(inputs=..., output_schema=...)`;
- `transform`, with parameters that exactly match the names in `inputs`; and
- a `transform` result that is a `polars.LazyFrame` matching `output_schema`.

Every input and output schema is a `polars.Schema`. A `QuerySource` must name an
existing node ID, and its schema must exactly match that producer's declared
output schema.

## Accepted source contracts

Use one of these values in `QueryMetadata.inputs`:

- `CsvSource(path=..., schema=...)` for a CSV path or URI;
- `ParquetSource(path=..., schema=...)` for a Parquet path or URI;
- `QuerySource(node_id=..., schema=...)` for another query's output; or
- a custom `Source` implementation with `schema`, `identity`, and `scan()`.

`scan()` must return a `polars.LazyFrame`. Give every physical source an explicit
schema. Do not put credentials in source metadata; configure cloud access through
Polars.
