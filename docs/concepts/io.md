# Sources and outputs

A `SourceModel.source()` may return any genuine Polars `LazyFrame`, including a
lazy scan created with `scan_csv`, `scan_parquet`, `scan_ipc`, or another lazy
Polars operation. The contract is the returned `LazyFrame` and its declared
`output_schema`, not a specific file format.

Be precise about database reads: Polars `read_database` and `read_database_uri`
are eager, in-memory reads. Calling `.lazy()` on their resulting `DataFrame`
creates a lazy plan over data already read into memory; it is not a database
scan. It can still return a `LazyFrame`, but has different execution and memory
properties from a genuine lazy source.

Every `MartModel` owns one output declaration. `Output.parquet`, `Output.csv`,
`Output.ipc`, and `Output.ndjson` create composable lazy sink plans and can be
grouped by the executor. `Output.delta` and `Output.iceberg` use direct,
non-composable Polars sink behavior after all model plans have passed schema
validation. Destinations may be relative local paths (resolved from the project
root), absolute paths, or URIs; Delta and Iceberg do not accept catalog/table
identifiers as a separate API.
