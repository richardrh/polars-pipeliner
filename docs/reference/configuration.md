# Configuration

Configuration is optional. `discover(root)` uses `ProjectConfig()` by default.
To load a file explicitly, pass `config_path`; do not pass both `config` and
`config_path`.

The configuration filename is your choice. Its required table name is
`polars-pipeliner`; `LOG_LEVEL` is required and `RUN_LOG_DIR` controls Python
run and validation JSONL files (default: `target/runs`, relative to the project
root):

```toml
[polars-pipeliner]
LOG_LEVEL = "INFO"
RUN_LOG_DIR = "target/runs"
```

Accepted values are `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`.
Unknown settings, a missing table or `LOG_LEVEL`, non-string settings, malformed
files, and unreadable or absent explicit paths are errors. `project.run()` and
`project.validate()` write one JSONL file per invocation; their CLI equivalents
emit JSONL to stdout instead. The default is `INFO`: run and validation boundary
events are always emitted, while per-node validation and output events respect
`LOG_LEVEL`.

```python
project = discover(".", config_path="polars-pipeliner.toml")
manifest = project.run()
```
