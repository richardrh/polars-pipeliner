# DAG discovery and execution

`discover(root)` imports scoped model files, validates their placement and
method signatures, builds the dependency graph from `Input` declarations, and
rejects missing dependencies, incompatible declared schemas, and cycles.

Discovery does not instantiate models or invoke `source()` or `transform()`.
During a build, models are instantiated and their methods are invoked in
topological order exactly as required to construct each lazy plan. The executor
collects every plan's schema first, so the complete graph is built and validated
before it writes a mart.

`project.run()` has no target argument: it materializes every `MartModel` and
returns a mapping from mart node IDs to destinations. Compatible Parquet, CSV,
IPC, and NDJSON sink plans are grouped and collected together. Delta and
Iceberg use their direct Polars sink APIs after the build validation step.

`project.validate()` constructs every source and transform plan, resolves each
schema with `collect_schema()`, and returns the node-to-schema mapping without
collecting rows or preparing or writing mart sinks. It calls model methods to
construct plans; `collect_schema()` may read file metadata or make network
requests. It cannot prove row values, uniqueness, ranges, null constraints, or
business correctness, and does not validate CSV row contents.

Validation JSONL ends with a fail-fast `validation_succeeded` or
`validation_failed` summary containing `models_found`, `models_verified`,
`models_failed`, and `failed_models`. It reports only nodes verified before the
first failure; it does not label or aggregate blocked nodes.
