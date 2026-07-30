"""Typed project configuration."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from .errors import ConfigError

CONFIG_TABLE: Final = "polars-pipeliner"
DEFAULT_LOG_LEVEL: Final = "INFO"
DEFAULT_RUN_LOG_DIR: Final = Path("target/runs")
VALID_LOG_LEVELS: Final = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


@dataclass(frozen=True)
class ProjectConfig:
    """Runtime configuration for a discovered project."""

    log_level: str = DEFAULT_LOG_LEVEL
    run_log_dir: Path = DEFAULT_RUN_LOG_DIR

    def __post_init__(self) -> None:
        if self.log_level not in VALID_LOG_LEVELS:
            raise ConfigError.invalid_log_level(self.log_level, VALID_LOG_LEVELS)
        if not isinstance(self.run_log_dir, Path):
            raise ConfigError.invalid_run_log_dir(self.run_log_dir)


def load_config(path: str | Path | None = None) -> ProjectConfig:
    """Load an explicitly selected TOML config, or return the safe default."""
    if path is None:
        return ProjectConfig()

    config_path = Path(path)
    try:
        with config_path.open("rb") as config_file:
            document = tomllib.load(config_file)
    except FileNotFoundError as error:
        raise ConfigError.missing_file(config_path) from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError.malformed_file(config_path, error) from error
    except OSError as error:
        raise ConfigError.unreadable_file(config_path, error) from error

    table = document.get(CONFIG_TABLE)
    if not isinstance(table, dict):
        raise ConfigError.missing_table(config_path, CONFIG_TABLE)
    table = cast(dict[str, object], table)
    unknown = sorted(set(table).difference({"LOG_LEVEL", "RUN_LOG_DIR"}))
    if unknown:
        raise ConfigError.unknown_settings(config_path, unknown)
    if "LOG_LEVEL" not in table:
        raise ConfigError.missing_setting(config_path, CONFIG_TABLE, "LOG_LEVEL")
    log_level = table["LOG_LEVEL"]
    if not isinstance(log_level, str):
        raise ConfigError.non_string_setting(config_path, "LOG_LEVEL")
    configured_run_log_dir = table.get("RUN_LOG_DIR")
    if configured_run_log_dir is not None and not isinstance(
        configured_run_log_dir, str
    ):
        raise ConfigError.non_string_setting(config_path, "RUN_LOG_DIR")
    run_log_dir = Path(configured_run_log_dir or DEFAULT_RUN_LOG_DIR)
    try:
        return ProjectConfig(log_level=log_level, run_log_dir=run_log_dir)
    except ConfigError as error:
        raise ConfigError.invalid_setting(config_path, error) from error
