# Compared with Lea and dbt

_Comparison reviewed July 2026._

The tools target different data engines and deployment models.

## Core model

| Capability | polars-pipeliner | [Lea](https://github.com/carbonfact/lea) | [dbt Core](https://docs.getdbt.com/docs/introduction) |
| --- | --- | --- | --- |
| Primary abstraction | Python Polars model | SQL file | SQL model |
| Execution engine | Polars `LazyFrame` | Data warehouse | Data warehouse adapter |
| Dependencies | Explicit named `Input` edges | Inferred from SQL table references | `ref()` and `source()` |
| Schema contracts | Exact input and output schemas | Tests and assertions | Optional output contracts on supported models |

## Operations

| Capability | polars-pipeliner | Lea | dbt Core |
| --- | --- | --- | --- |
| Logging | JSONL events and run artifacts | Human-oriented Rich logs | Text, debug, and JSON logs with artifacts |
| Outputs | Declared mart destinations | Warehouse tables | Configured warehouse relations |
| Materialization | Parquet, CSV, IPC, NDJSON, Delta, Iceberg | Write-audit-publish | Views, tables, incremental models |
| Workflow focus | Native Polars and generated pipelines | Lightweight SQL orchestration | Warehouse-centric SQL transformation |

## Choose Polars Pipeliner when

- transformations are written with Polars;
- exact schemas should be checked at dependency edges;
- outputs are files, tables, or storage URIs owned by each mart;
- a small file-based Python project is preferable.

## Choose Lea when

- transformations are SQL files;
- a lightweight warehouse orchestrator is sufficient;
- write-audit-publish is the desired materialization workflow.

## Choose dbt Core when

- the project is centered on warehouse SQL;
- a broad adapter and package ecosystem is required;
- warehouse relations are the primary materialization target.

Choose based on the data engine, deployment environment, contract requirements,
and operational workflow.
