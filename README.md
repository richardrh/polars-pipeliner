# polars-pipeliner

## Designed for analytical LLM workflows

- Designed for use in agentic workflows where agents will write one or more
  queries.
- Non-Agent workflows still works well - provides the structure your analytics
  pipelines need to validate inputs, outputs and execute them in correct sequence.

## What is polars-pipeliner?

This is a tool to automatically discover and execute Polars queries using
dbt style folder structure to organize an analysis run.

Executes queries in the correct sequence and leverages Polars' lazy loading
and optimizations.

## What problem does it solve?

Dbt is too heavyweight, alternatives run on SQL only meaning they are not
type checked and schema checked.

Polars-pipeliner defines a contract that each Polars query must match.
Input and output data schemas are checked, the pipeline errors if they fail.

### Other features

- Validation of every transformation
- Structured logging
- Validate the execution plan up front
- Python and CLI APIs

## Documentation
https://richardrh.github.io/polars-pipeliner/

## Example

This mart consumes typed order lines, declares its result contract, and lets the
executor own the output destination:

```python
import polars as pl

from polars_pipeliner import Input, MartModel, Output


class CustomerOrders(MartModel):
    order_lines = Input(
        "intermediate.order_lines",
        schema=pl.Schema({"customer_id": pl.Int64, "line_total": pl.Float64}),
    )
    output_schema = pl.Schema({"customer_id": pl.Int64, "total_spend": pl.Float64})
    output = Output.parquet("target/customer_orders.parquet")

    def transform(self, order_lines: pl.LazyFrame) -> pl.LazyFrame:
        return order_lines.group_by("customer_id").agg(
            pl.col("line_total").sum().alias("total_spend")
        )
```

## Install and run

Requires Python 3.13+ and Polars 1.38.1+:

Using `uv`:

```bash
uv add polars-pipeliner
```

Using `pip`:

```bash
python -m pip install polars-pipeliner
```

Then run the bundled example:

```bash
uv run polars-pipeliner run examples/customer-orders --config examples/customer-orders/polars-pipeliner.toml
```

The CLI writes JSONL events to stdout and materializes
`examples/customer-orders/target/customer_orders.parquet`. The Python API keeps
the manifest return value:

```bash
uv run python examples/customer-orders/run.py
```

Validate declared and resolved schemas without writing a mart:

```bash
uv run polars-pipeliner validate examples/customer-orders --config examples/customer-orders/polars-pipeliner.toml
```

```python
schemas = discover("examples/customer-orders").validate()
```

Run the second bundled example with:

```bash
uv run python examples/seeds-and-sources/run.py
```

## Run logging

```toml
[polars-pipeliner]
LOG_LEVEL = "INFO"
RUN_LOG_DIR = "target/runs"
```

CLI runs emit JSONL to stdout; Python runs write JSONL files to `RUN_LOG_DIR`.

## Compared with Lea and dbt

|                                 | polars-pipeliner                                                   | Lea                                                                       | dbt Core                                                          |
| ------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| **LLM-friendly**                | Native LLM workflow tooling                                        | Simple file-based SQL; no LLM-specific interface                          | No dedicated LLM workflow                                         |
| **Logging**                     | JSONL for CLI commands and Python runs                             | Human-oriented Rich logs; no structured JSON event stream or run artifact | Text, debug, and JSON logs; run artifacts                         |
| **Schema validation**           | Exact input and output schema contracts at every node              | Tests and assertions; no explicit edge schema contracts                   | Optional YAML-declared output contracts; not input-edge contracts |
| **Transformations / execution** | Python Polars `LazyFrame` models                                   | SQL scripts in a data warehouse                                           | SQL models by a data warehouse adapter                            |
| **Best fit**                    | Schema-first native Polars pipelines, especially agentic workflows | Lightweight file-based SQL warehouse orchestration                        | Warehouse-centric SQL transformation                              |

Lea and dbt Core serve different workloads. Read the fuller, source-linked
[comparison](docs/comparison.md).

Read the full [documentation](docs/index.md).
