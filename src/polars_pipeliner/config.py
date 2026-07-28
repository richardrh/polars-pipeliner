"""Typed project configuration and package-owned logging setup."""

from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TextIO, cast

from .errors import ConfigError

CONFIG_TABLE: Final = "polars-build-tool"
DEFAULT_LOG_LEVEL: Final = "WARNING"
VALID_LOG_LEVELS: Final = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


@dataclass(frozen=True)
class ProjectConfig:
    """Runtime configuration for a discovered project."""

    log_level: str = DEFAULT_LOG_LEVEL

    def __post_init__(self) -> None:
        if self.log_level not in VALID_LOG_LEVELS:
            raise ConfigError.invalid_log_level(self.log_level, VALID_LOG_LEVELS)


class PackageLogHandler(logging.StreamHandler[TextIO]):
    """A marker handler owned exclusively by this package."""


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
    unknown = sorted(set(table).difference({"LOG_LEVEL"}))
    if unknown:
        raise ConfigError.unknown_settings(config_path, unknown)
    if "LOG_LEVEL" not in table:
        raise ConfigError.missing_setting(config_path, CONFIG_TABLE, "LOG_LEVEL")
    log_level = table["LOG_LEVEL"]
    if not isinstance(log_level, str):
        raise ConfigError.non_string_setting(config_path, "LOG_LEVEL")
    try:
        return ProjectConfig(log_level=log_level)
    except ConfigError as error:
        raise ConfigError.invalid_setting(config_path, error) from error


def configure_logging(config: ProjectConfig) -> logging.Logger:
    """Configure only the package logger, with exactly one owned stderr handler."""
    logger = logging.getLogger("polars_pipeliner")
    logger.setLevel(config.log_level)
    logger.propagate = False
    if not any(isinstance(handler, PackageLogHandler) for handler in logger.handlers):
        handler = PackageLogHandler()
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        logger.addHandler(handler)
    return logger
