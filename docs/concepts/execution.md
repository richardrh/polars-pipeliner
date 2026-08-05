# Discovery and execution

Polars Pipeliner separates file discovery, lazy-plan construction, schema
validation, and output writing.

## Lifecycle

| Step | Operation | Writes marts |
| --- | --- | --- |
| 1 | Discover model files | No |
| 2 | Validate dependencies | No |
| 3 | Build lazy plans | No |
| 4 | Resolve plan schemas | No |
| 5 | Materialize marts | Yes |

## 1. Discover model files

```python
from polars_pipeliner import discover


project = discover("my-pipeline")
```

Discovery:

- imports files below the four model folders;
- checks model placement;
- checks method signatures;
- reads direct `Input` declarations;
- rejects missing dependencies;
- rejects incompatible declared schemas;
- rejects dependency cycles.

Discovery does not instantiate models or call `source()` or `transform()`.

## 2. Resolve dependency order

```python
order = project.resolve()
```

Example:

```python
(
    "sources.orders",
    "staging.orders",
    "marts.orders",
)
```

The returned tuple is in dependency-safe, or topological, order.

## 3. Build lazy plans

```python
result = project.build()
```

Build:

- instantiates models;
- invokes model methods in dependency order;
- creates each `LazyFrame`;
- resolves each plan schema;
- returns immutable `frames` and `schemas` mappings.

Build does not write mart outputs.

## 4. Validate the project

```python
schemas = project.validate()
```

Validation:

- constructs every source and transform plan;
- calls Polars `collect_schema()`;
- returns an immutable node-to-schema mapping;
- does not collect rows;
- does not prepare or write mart sinks.

`collect_schema()` may read file metadata or contact remote storage. It cannot
prove row values or business constraints.

## 5. Run the project

```python
manifest = project.run()
```

Run:

- validates the complete graph before writing;
- materializes every `MartModel`;
- returns an immutable mart-to-destination mapping.

`project.run()` does not accept a target argument.

## Output execution

| Output | Execution |
| --- | --- |
| Parquet, CSV, IPC, NDJSON | Grouped lazy sink plans |
| Delta, Iceberg | Direct Polars sink APIs after validation |

## Validation summaries

Command-line validation emits JSON Lines (JSONL). The final event is either:

- `validation_succeeded`;
- `validation_failed`.

The summary contains:

| Field | Meaning |
| --- | --- |
| `models_found` | Scoped model files discovered |
| `models_verified` | Models verified before completion or failure |
| `models_failed` | Models that failed |
| `failed_models` | Failed node IDs |

Validation is fail-fast. Nodes blocked by the first failure are not reported as
failed.
