# Schemas and validation

Schemas serve four distinct checks:

1. **Source schema validation.** Every model declares `output_schema: pl.Schema`.
   A source's `source()` result must be a `LazyFrame` whose collected schema
   equals that declaration.
2. **Declared edge contracts.** Each direct `Input` class attribute, such as
   `orders = Input("upstream.node", schema=EXPECTED)`, must name an existing
   upstream node, and `EXPECTED` must exactly equal that upstream model's
   declared `output_schema`.
3. **Output-plan checks.** During the complete build, each source and transform
   returns a `LazyFrame`; `collect_schema()` checks the plan's actual output
   schema against the model's declaration before output writing starts.
4. **Runtime row and data constraints.** `pl.Schema` and `collect_schema()`
   describe columns and dtypes, not values such as uniqueness, nullability
   policies, ranges, or referential integrity. Express those row/data rules in
   your Polars transformations or data-quality process.

This separation lets the graph reject incompatible declared interfaces early
while still checking the schemas emitted by lazy Polars plans.

For readable, reusable contracts within a model module, define descriptive
module-level `pl.Schema` constants and reference them from `Input` and
`output_schema` declarations. Each consumer model should still declare its own
explicit expectation for every upstream input.
