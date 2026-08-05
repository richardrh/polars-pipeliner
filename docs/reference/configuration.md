# Configuration

Configuration is optional.

## Defaults

Without a configuration file, `discover(root)` uses:

| Setting | Default |
| --- | --- |
| Log level | `INFO` |
| Run log directory | `target/runs` |

```python
from polars_pipeliner import discover


project = discover("my-pipeline")
```

## Configure in Python

```python
from pathlib import Path

from polars_pipeliner import ProjectConfig, discover


config = ProjectConfig(
    log_level="WARNING",
    run_log_dir=Path("target/runs"),
)

project = discover("my-pipeline", config=config)
```

## Configure with TOML

The filename is your choice. The table must be named `polars-pipeliner`.

```toml
[polars-pipeliner]
LOG_LEVEL = "INFO"
RUN_LOG_DIR = "target/runs"
```

Load the file through `discover()`:

```python
from polars_pipeliner import discover


project = discover(
    "my-pipeline",
    config_path="polars-pipeliner.toml",
)
```

Do not pass both `config` and `config_path`.

## Settings

| TOML setting | Required in a file | Default |
| --- | --- | --- |
| `LOG_LEVEL` | Yes | `INFO` without a file |
| `RUN_LOG_DIR` | No | `target/runs` |

Relative run log directories are resolved from the project root.

## Valid log levels

- `DEBUG`
- `INFO`
- `WARNING`
- `ERROR`
- `CRITICAL`

The default level is `INFO`.

| Event category | Controlled by `LOG_LEVEL` |
| --- | --- |
| Run and validation boundaries | No; always emitted |
| Per-node validation events | Yes |
| Output events | Yes |

## Invalid configuration

Configuration loading fails for:

- unknown settings;
- a missing `[polars-pipeliner]` table;
- a missing `LOG_LEVEL` in a selected TOML file;
- non-string settings;
- malformed TOML;
- an unreadable selected path;
- a selected path that does not exist.

## Event destinations

| Invocation | Destination |
| --- | --- |
| `project.validate()` | One JSONL run-log file |
| `project.run()` | One JSONL run-log file |
| Command-line validation | JSONL on standard output |

JSON Lines (JSONL) stores one JSON object per line.
