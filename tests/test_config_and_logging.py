from __future__ import annotations

import logging
from pathlib import Path

import pytest

from polars_pipeliner import (
    ConfigError,
    ProjectConfig,
    QueryBuildError,
    QueryExecutionError,
    discover,
    load_config,
)
from polars_pipeliner.config import PackageLogHandler


def write_model(root: Path, name: str, source: str) -> None:
    root.joinpath(f"{name}.py").write_text(source)


def model_source(
    *,
    inputs: str = "{}",
    parameters: str = "",
    body: str = "return pl.LazyFrame({'value': [1]})",
    schema: str = "pl.Schema({'value': pl.Int64})",
) -> str:
    return f"""import polars as pl
from polars_pipeliner import PolarsModel, QueryMetadata, QuerySource
SCHEMA = {schema}
class Model(PolarsModel):
    metadata = QueryMetadata(inputs={inputs}, output_schema=SCHEMA)
    @classmethod
    def transform(cls, {parameters}) -> pl.LazyFrame:
        {body}
"""


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_config_accepts_standard_levels(tmp_path: Path, level: str) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(f'[polars-pipeliner]\nLOG_LEVEL = "{level}"\n')

    assert load_config(config_path) == ProjectConfig(log_level=level)


def test_config_default_and_explicit_file_errors(tmp_path: Path) -> None:
    assert load_config() == ProjectConfig()
    with pytest.raises(ConfigError, match="does not exist"):
        load_config(tmp_path / "absent.toml")

    malformed = tmp_path / "malformed.toml"
    malformed.write_text("[polars-pipeliner\n")
    with pytest.raises(ConfigError, match="Malformed"):
        load_config(malformed)


@pytest.mark.parametrize(
    ("content", "message"),
    [
        ("LOG_LEVEL = 'INFO'", "must contain"),
        ("[polars-pipeliner]", "missing"),
        ("[polars-pipeliner]\nLOG_LEVEL = 'VERBOSE'", "Invalid LOG_LEVEL"),
        ("[polars-pipeliner]\nLOG_LEVEL = 'INFO'\nOTHER = true", "unknown setting"),
    ],
)
def test_config_rejects_invalid_contracts(
    tmp_path: Path, content: str, message: str
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(content)

    with pytest.raises(ConfigError, match=message):
        load_config(config_path)


def test_configure_logging_does_not_mutate_root_logger(tmp_path: Path) -> None:
    root_logger = logging.getLogger()
    root_level = root_logger.level
    root_handlers = tuple(root_logger.handlers)
    write_model(tmp_path, "model", model_source())

    discover(tmp_path, config=ProjectConfig("INFO"))

    assert root_logger.level == root_level
    assert tuple(root_logger.handlers) == root_handlers


class RecordedMessages(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_build_and_collection_logs_are_ordered_safe_and_not_duplicated(
    tmp_path: Path,
) -> None:
    write_model(
        tmp_path,
        "source",
        model_source(),
    )
    write_model(
        tmp_path,
        "target",
        model_source(
            inputs="{'source': QuerySource(node_id='source', schema=SCHEMA)}",
            parameters="source",
            body="return source",
        ),
    )
    write_model(tmp_path, "disconnected", model_source())
    handler = RecordedMessages()
    executor_logger = logging.getLogger("polars_pipeliner.executor")
    executor_logger.addHandler(handler)
    try:
        project = discover(tmp_path, config=ProjectConfig("INFO"))
        discover(tmp_path, config=ProjectConfig("INFO"))
        package_logger = logging.getLogger("polars_pipeliner")
        assert (
            sum(
                isinstance(existing, PackageLogHandler)
                for existing in package_logger.handlers
            )
            == 1
        )

        project.run(["target"])
    finally:
        executor_logger.removeHandler(handler)

    assert len(handler.messages) == 6
    assert handler.messages[0] == "Building LazyFrame plan for model source"
    assert handler.messages[1].startswith("Built LazyFrame plan for model source in ")
    assert handler.messages[2] == "Building LazyFrame plan for model target"
    assert handler.messages[3].startswith("Built LazyFrame plan for model target in ")
    assert handler.messages[4] == "Collecting target LazyFrames: target"
    assert handler.messages[5].startswith("Collected target LazyFrames: target in ")
    assert all("disconnected" not in message for message in handler.messages)
    assert all("do-not-log-this" not in message for message in handler.messages)


def test_model_build_failure_logs_and_preserves_context(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "broken",
        model_source(schema="pl.Schema({'other': pl.Int64})"),
    )
    handler = RecordedMessages()
    executor_logger = logging.getLogger("polars_pipeliner.executor")
    executor_logger.addHandler(handler)
    try:
        project = discover(tmp_path, config=ProjectConfig("INFO"))
        with pytest.raises(
            QueryBuildError, match=r"broken.*broken.py.*schema mismatch"
        ):
            project.build(["broken"])
    finally:
        executor_logger.removeHandler(handler)

    assert handler.messages[0] == "Building LazyFrame plan for model broken"
    assert handler.messages[1].startswith("Failed building model broken after ")


def test_collection_failure_logs_target_context(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "broken_collection",
        model_source(
            body=(
                "return pl.LazyFrame({'value': [1]}).map_batches("
                "lambda frame: 1 / 0, schema=pl.Schema({'value': pl.Int64}))"
            )
        ),
    )
    handler = RecordedMessages()
    executor_logger = logging.getLogger("polars_pipeliner.executor")
    executor_logger.addHandler(handler)
    try:
        project = discover(tmp_path, config=ProjectConfig("INFO"))
        with pytest.raises(QueryExecutionError, match=r"target\(s\) broken_collection"):
            project.run(["broken_collection"])
    finally:
        executor_logger.removeHandler(handler)

    assert handler.messages[2] == "Collecting target LazyFrames: broken_collection"
    assert handler.messages[3].startswith(
        "Failed collecting target(s) broken_collection after "
    )
