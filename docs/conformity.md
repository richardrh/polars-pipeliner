# Conformity

Each discovered model is instantiated once. Discovery does not call `source` or
`transform`. A source declares `output_schema: pl.Schema` and implements
`source(self) -> pl.LazyFrame`. A transform declares `inputs`, `output_schema`,
and a `transform(self, ...named LazyFrames...) -> pl.LazyFrame` method.

An input is `Input("upstream.node", schema=EXPECTED_SCHEMA)`. Its schema must
exactly equal the upstream model's declared output schema. The executor also
checks every returned LazyFrame schema with `collect_schema()`.

Every `MartModel` declares one `Output`: `Output.parquet`, `Output.csv`,
`Output.ipc`, `Output.ndjson`, `Output.delta`, or `Output.iceberg`. Parquet,
CSV, IPC, and NDJSON use composable lazy sink plans; Delta and Iceberg use their
current direct Polars sink behavior after all plans have been validated.
Delta and Iceberg destinations are local paths (resolved from the project root)
or URIs; catalog/table identifiers are not part of this API.
