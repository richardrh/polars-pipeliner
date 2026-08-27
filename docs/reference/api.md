# Public API

Import public symbols from `polars_pipeliner`.

```python
from polars_pipeliner import (
    BuildResult,
    ConfigError,
    DiscoveryError,
    Input,
    MartModel,
    Model,
    ModelValidationError,
    Output,
    Project,
    ProjectConfig,
    QueryBuildError,
    QueryError,
    QueryExecutionError,
    QueryValidationError,
    SourceModel,
    discover,
    load_config,
)
```

## Quick reference

| Symbol | Purpose |
| --- | --- |
| `Input` | Declare an upstream dependency |
| `SourceModel` | Introduce a lazy data source |
| `Model` | Define a reusable transformation |
| `MartModel` | Define a materialized output |
| `Output` | Create a typed output specification |
| `BuildResult` | Hold built frames and resolved schemas |
| `Project` | Build, validate, and run a discovered project |
| `discover()` | Discover a project from a root directory |
| `ProjectConfig` | Configure logging |
| `load_config()` | Load TOML configuration |

## `Input`

```python
Input(node_id: str, *, schema: pl.Schema)
```

`Input` is an immutable upstream binding.

```python
orders = Input("sources.orders", schema=ORDER_SCHEMA)
```

| Argument | Meaning |
| --- | --- |
| `node_id` | Upstream model ID |
| `schema` | Exact schema expected from that model |

The class attribute name must match the corresponding `transform()` parameter.
Mapping-style `inputs = {...}` declarations are not supported.

## Model classes

| Class | Folder | Required method | Additional declarations |
| --- | --- | --- | --- |
| `SourceModel` | `sources/` | `source()` | `output_schema` |
| `Model` | `staging/`, `intermediate/` | `transform()` | Inputs and `output_schema` |
| `MartModel` | `marts/` | `transform()` | Inputs, `output_schema`, and `output` |

See [Model types](/concepts/models) for complete examples.

## `Output`

Use `Output` factory methods in a `MartModel`.

| Factory | Target | Options |
| --- | --- | --- |
| `Output.parquet()` | Path or storage URI | `compression`, `storage_options` |
| `Output.csv()` | Path or storage URI | `separator`, `include_header`, `storage_options` |
| `Output.ipc()` | Path or storage URI | `compression`, `storage_options` |
| `Output.ndjson()` | Path or storage URI | `storage_options` |
| `Output.delta()` | Delta path or URI | `mode`, `storage_options` |
| `Output.delta_upsert()` | Existing Delta path or URI | `keys`, `storage_options` |
| `Output.iceberg()` | Catalog table identifier | `catalog`, `mode` |

### File output

```python
output = Output.parquet(
    "s3://analytics-bucket/marts/orders.parquet",
    compression="zstd",
)
```

File output modes overwrite the destination.

### Delta output

```python
output = Output.delta(
    "s3://analytics-bucket/tables/orders",
    mode="append",
)
```

Valid modes are `error`, `append`, `overwrite`, and `ignore`.

Use `delta_upsert()` for update-all and insert-all semantics:

```python
output = Output.delta_upsert(
    "s3://analytics-bucket/tables/orders",
    keys=("order_id",),
)
```

### Iceberg output

```python
output = Output.iceberg(
    "analytics.orders",
    catalog="production",
    mode="append",
)
```

The first argument is a table identifier. It is not a path. The named
PyIceberg catalog is loaded only during `project.run()`.

### Storage options

`storage_options` accepts a mapping of backend-specific strings. The mapping is
copied and made immutable. Its values are redacted from execution failures and
are not included in events or manifests.

Output specifications are immutable. See
[Sources and outputs](/concepts/io) for installation, credentials, S3, Delta,
and Iceberg examples.

## `BuildResult`

`Project.build()` returns a `BuildResult`.

| Attribute | Type | Contents |
| --- | --- | --- |
| `frames` | `Mapping[str, pl.LazyFrame]` | Built lazy plans by node ID |
| `schemas` | `Mapping[str, pl.Schema]` | Resolved schemas by node ID |

Both mappings are immutable.

```python
result = project.build()

orders = result.frames["staging.orders"]
schema = result.schemas["staging.orders"]
```

## `Project`

### `node_ids`

```python
project.node_ids -> tuple[str, ...]
```
Returns discovered node IDs in stable discovery order.

### `resolve()`

```python
project.resolve() -> tuple[str, ...]
```

Returns node IDs in dependency-safe order.

### `build()`

```python
project.build() -> BuildResult
```

Builds every lazy plan and resolves every schema. It does not write marts.

### `validate()`

```python
project.validate() -> Mapping[str, pl.Schema]
```

Builds all plans and returns their resolved schemas without collecting rows or
writing marts.

### `run()`

```python
project.run() -> Mapping[str, str | Path]
```

Validates the graph, writes every mart, and returns an immutable manifest.

```python
manifest = project.run()
destination = manifest["marts.orders"]
```

Relative output paths become absolute manifest paths because the project root
is resolved during discovery.

`Project.graph` is not part of the public API. Use `node_ids` and `resolve()`.

## `discover()`

```python
discover(
    query_root,
    *,
    config=None,
    config_path=None,
) -> Project
```

### Rules

- `query_root` must be a directory.
- `config` and `config_path` cannot both be supplied.
- the project root is resolved to an absolute path;
- model declarations are checked before a `Project` is returned.

### Example

```python
project = discover(
    "my-pipeline",
    config_path="polars-pipeliner.toml",
)
```

## Configuration API

### `ProjectConfig`

```python
from pathlib import Path

from polars_pipeliner import ProjectConfig


config = ProjectConfig(
    log_level="WARNING",
    run_log_dir=Path("target/runs"),
)
```

### `load_config()`

```python
from polars_pipeliner import load_config


config = load_config("polars-pipeliner.toml")
```

Call `load_config()` without a path to return the default configuration.

## Errors

All package errors inherit from `QueryError`.

| Error | Category |
| --- | --- |
| `ConfigError` | Invalid configuration |
| `DiscoveryError` | Import or model declaration failure |
| `QueryValidationError` | Dependency or declared-schema failure |
| `QueryBuildError` | Lazy-plan construction failure |
| `ModelValidationError` | Resolved output-schema failure |
| `QueryExecutionError` | Mart output failure |

## Validation summary

The final command-line validation event contains:

| Field | Meaning |
| --- | --- |
| `models_found` | Scoped model files discovered |
| `models_verified` | Models verified before completion or failure |
| `models_failed` | Models that failed |
| `failed_models` | Failed node IDs |

Validation is fail-fast. Counts include only models verified before the first
failure.
