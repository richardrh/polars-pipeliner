# Compared with Lea and dbt

_Comparison reviewed July 2026._

| | polars-pipeliner | [Lea](https://github.com/carbonfact/lea) | [dbt Core](https://docs.getdbt.com/docs/introduction) |
| --- | --- | --- | --- |
| **LLM-friendly** | Native LLM workflow tooling | Simple file-based SQL; no LLM-specific interface | No dedicated LLM workflow |
| **Logging** | JSONL for CLI commands and Python runs | Detailed human-oriented Rich logs; no structured JSON event stream or run artifact | Text, debug, and JSON logs; run artifacts |
| **Schema validation** | Exact input and output schema contracts at every node | Tests and assertions; no explicit edge schema contracts | Optional YAML-declared output contracts on supported models; not input-edge contracts |
| **Transformations / execution** | Python Polars `LazyFrame` models | SQL scripts executed in a data warehouse | SQL models executed by a data warehouse adapter |
| **DAG dependencies** | Explicit named `Input` edges | Inferred from SQL table references | `ref()` and `source()` references |
| **Materialization / outputs** | Declared mart outputs, such as Parquet | Warehouse tables, with write-audit-publish | Configured warehouse relations (views, tables, incremental models) |
| **Best fit** | Schema-first native Polars pipelines, especially agentic workflows | Lightweight file-based SQL warehouse orchestration | Warehouse-centric SQL transformation with a broad adapter ecosystem |

Lea is a SQL orchestrator, not a Polars framework. These tools serve different
workloads: choose based on the data engine, deployment environment, contracts,
and workflow your project requires.
