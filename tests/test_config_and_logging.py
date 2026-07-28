from __future__ import annotations

from pathlib import Path

import pytest

from polars_pipeliner import (
    ConfigError,
    ProjectConfig,
    load_config,
)


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_config_accepts_standard_levels(tmp_path: Path, level: str) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(f'[polars-build-tool]\nLOG_LEVEL = "{level}"\n')

    assert load_config(config_path) == ProjectConfig(log_level=level)


def test_config_default_and_explicit_file_errors(tmp_path: Path) -> None:
    assert load_config() == ProjectConfig()
    with pytest.raises(ConfigError, match="does not exist"):
        load_config(tmp_path / "absent.toml")

    malformed = tmp_path / "malformed.toml"
    malformed.write_text("[polars-build-tool\n")
    with pytest.raises(ConfigError, match="Malformed"):
        load_config(malformed)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("LOG_LEVEL = 'INFO'", "must contain"),
        ("[polars-build-tool]", "missing"),
        ("[polars-build-tool]\nLOG_LEVEL = 'VERBOSE'", "Invalid LOG_LEVEL"),
        ("[polars-build-tool]\nLOG_LEVEL = 'INFO'\nOTHER = true", "unknown setting"),
    ],
)
def test_config_rejects_invalid_contracts(
    tmp_path: Path, content: str, message: str
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(content)

    with pytest.raises(ConfigError, match=message):
        load_config(config_path)
