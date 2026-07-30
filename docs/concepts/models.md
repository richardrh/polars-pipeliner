# Model types

Only Python files recursively below `sources/`, `staging/`, `intermediate/`, and
`marts/` are auto-discovered. Each such file defines exactly one local model
class. Other Python files do not become models.

`SourceModel` belongs in `sources/`, declares `output_schema`, has no `Input`
attributes, and implements the ordinary instance method
`source(self) -> pl.LazyFrame`. `Model` belongs in `staging/` or
`intermediate/`, declares direct `Input`-valued class attributes and
`output_schema`, and implements `transform(self, **named_lazyframes) ->
pl.LazyFrame` as a normal instance method. `MartModel` belongs in `marts/`, has
the same transform contract, and additionally declares one `output`.

Use `Input(node_id, schema=...)` for each transform argument as a direct class
attribute. Its attribute name must equal the transform parameter name, and
`node_id` identifies the upstream model. These declarations, rather than
imports or method calls, define DAG edges. Every discovered `MartModel` is
materialized when `project.run()` runs.
