# Setup and run

Use Python 3.13+ and `polars-build-tool` with Polars 1.38.1+.

```bash
uv add polars-build-tool
```

Only Python files recursively under `sources/`, `staging/`, `intermediate/`,
and `marts/` are models. Each must define exactly one local model class.
`SourceModel` belongs in `sources`; `Model` belongs in `staging` or
`intermediate`; and `MartModel` belongs in `marts`.

```python
from polars_pipeliner import discover

manifest = discover(".").run()
print(manifest)
```

`run()` validates every source and transform plan before it begins writing.
It materializes every mart and returns its destination manifest. Local relative
destinations are resolved from the project root.

The canonical [`customer-orders`](../examples/customer-orders) example contains
customers, products, and orders, then left-joins customer aggregates so a
customer with no orders has zero totals. Run it from the repository root with:

```bash
uv run python examples/customer-orders/run.py
```

Build docs locally with:

```bash
uv run zensical build --strict
```
