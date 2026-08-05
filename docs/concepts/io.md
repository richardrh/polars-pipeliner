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

## Output support

Every `MartModel` declares one output.

| Output | Local | S3 | Append | Overwrite | Upsert |
| --- | ---: | ---: | ---: | ---: | ---: |
| Parquet | Yes | Yes | No | Yes | No |
| CSV | Yes | Yes | No | Yes | No |
| IPC | Yes | Yes | No | Yes | No |
| NDJSON | Yes | Yes | No | Yes | No |
| Delta | Yes | Yes | Yes | Yes | Yes, by key |
| Iceberg | Catalog-managed | Catalog-managed | Yes | Yes | No |

The executor validates every lazy plan before writing any output.

## Local files

Use a relative destination to write below the project root:

```python
class Orders(MartModel):
    output = Output.parquet(
        "target/orders.parquet",
        compression="zstd",
    )
```

Use an absolute `Path` when the destination is outside the project root.

## S3 and S3-compatible storage

Use an `s3://` URI instead of a local path:

```python
class Orders(MartModel):
    output = Output.parquet(
        "s3://analytics-bucket/marts/orders.parquet",
        compression="zstd",
    )
```

Polars uses its automatic AWS credential provider when `storage_options` is
omitted. Configure credentials through the normal AWS environment, profile,
workload identity, or instance-role chain.

For local development, export the standard AWS variables before running the
pipeline:

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-east-1"
```

If the credentials are stored in an AWS profile, select it instead:

```bash
export AWS_PROFILE="analytics"
```

In production, prefer the platform's workload identity or instance role. Do not
commit access keys to model files, project configuration, or documentation.

### Custom S3 endpoint

Use `storage_options` for a non-default endpoint such as MinIO:

```python
class Orders(MartModel):
    output = Output.parquet(
        "s3://analytics-bucket/marts/orders.parquet",
        storage_options={
            "endpoint_url": "https://objects.example.com",
        },
    )
```

Storage options are copied into an immutable mapping. They are never included
in run events. Prefer provider-chain credentials instead of placing secrets in
model files.

## Delta Lake

Install Delta support:

```bash
pip install "polars-pipeliner[delta]"
```

A Delta destination is a local path or table URI.

### Create a table

```python
output = Output.delta(
    "s3://analytics-bucket/tables/orders",
    mode="error",
)
```

`error` creates the table and fails if it already exists.

### Append rows

```python
output = Output.delta(
    "s3://analytics-bucket/tables/orders",
    mode="append",
)
```

The appended frame must be compatible with the existing Delta schema.

### Overwrite rows

```python
output = Output.delta(
    "s3://analytics-bucket/tables/orders",
    mode="overwrite",
)
```

Overwrite creates a new Delta version containing only the new frame.

### Upsert rows

```python
output = Output.delta_upsert(
    "s3://analytics-bucket/tables/orders",
    keys=("order_id",),
)
```

Upsert uses the declared keys to:

- update every column when a target row matches;
- insert every column when no target row matches.

For a composite key:

```python
output = Output.delta_upsert(
    "s3://analytics-bucket/tables/order_lines",
    keys=("order_id", "line_number"),
)
```

The target table must already exist. Every key must be unique in the declaration
and present in the mart's `output_schema`.

## Apache Iceberg

Install Iceberg support:

```bash
pip install "polars-pipeliner[iceberg]"
```

An Iceberg target is a catalog table identifier, not a filesystem destination.
The table must already exist.

### Configure a catalog

Configure PyIceberg outside the model. Example `.pyiceberg.yaml`:

```yaml
catalog:
  production:
    type: rest
    uri: https://iceberg.example.com
    warehouse: s3://analytics-bucket/warehouse
```

PyIceberg also supports `PYICEBERG_*` environment variables. Keep catalog and
object-store credentials outside the model file.

### Append rows

```python
class Orders(MartModel):
    output = Output.iceberg(
        "analytics.orders",
        catalog="production",
        mode="append",
    )
```

The executor loads the `production` catalog during `project.run()`, loads
`analytics.orders`, writes data files, and commits a new Iceberg snapshot.
Discovery, `build()`, and `validate()` do not contact the catalog.

### Overwrite rows

```python
class Orders(MartModel):
    output = Output.iceberg(
        "analytics.orders",
        catalog="production",
        mode="overwrite",
    )
```

Overwrite removes the current rows and commits the new frame as another
snapshot.

### S3-compatible Iceberg warehouse

For a custom endpoint, add Iceberg-style properties to the catalog
configuration:

```yaml
catalog:
  production:
    type: rest
    uri: https://iceberg.example.com
    warehouse: s3://analytics-bucket/warehouse
    s3.endpoint: https://objects.example.com
    s3.region: us-east-1
```

The catalog supplies these properties to both PyIceberg and the Polars writer.

### Current Iceberg limits

The underlying Polars Iceberg sink currently supports:

- append and overwrite;
- existing, unpartitioned tables;
- catalog-managed local or object-store warehouses.

It does not currently support:

- creating tables;
- row-level upserts;
- partitioned tables;
- tables with a configured sort order;
- custom Iceberg location providers.

## Manifest values

`project.run()` returns safe output identifiers:

```python
{
    "marts.parquet_orders": "s3://analytics-bucket/marts/orders.parquet",
    "marts.delta_orders": "s3://analytics-bucket/tables/orders",
    "marts.iceberg_orders": "production:analytics.orders",
}
```

URI credentials, query strings, fragments, and storage options are not included
in the manifest.
