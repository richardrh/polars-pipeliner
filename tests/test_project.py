from __future__ import annotations

from pathlib import Path

import pytest

from polars_pipeliner import DiscoveryError, QueryValidationError, discover


def write_model(root: Path, relative: str, source: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source)


SCHEMA = "pl.Schema({'value': pl.Int64})"


def source_model(body: str = "return pl.LazyFrame({'value': [1]})") -> str:
    return f"""import polars as pl
from polars_pipeliner import SourceModel
class Orders(SourceModel):
    output_schema = {SCHEMA}
    def source(self) -> pl.LazyFrame:
        {body}
"""


def transform_model(
    *,
    inputs: str = "{}",
    parameters: str = "",
    body: str = "return pl.LazyFrame({'value': [1]})",
    base: str = "Model",
) -> str:
    return f"""import polars as pl
from polars_pipeliner import Input, {base}
SCHEMA = {SCHEMA}
class Orders({base}):
    inputs = {inputs}
    output_schema = {SCHEMA}
    def transform(self, {parameters}) -> pl.LazyFrame:
        {body}
"""


def test_discovery_is_scoped_and_never_executes_root_scripts(tmp_path: Path) -> None:
    write_model(
        tmp_path, "run.py", "raise RuntimeError('must not import root scripts')"
    )
    write_model(
        tmp_path,
        "unrelated.py",
        "raise RuntimeError('must not import unrelated files')",
    )
    write_model(tmp_path, "sources/orders.py", source_model())
    write_model(tmp_path, "staging/_private.py", "raise RuntimeError('ignored')")
    write_model(
        tmp_path, "staging/__pycache__/cached.py", "raise RuntimeError('ignored')"
    )

    project = discover(tmp_path)

    assert project.node_ids == ("sources.orders",)


@pytest.mark.parametrize(
    "relative", ["sources/empty.py", "staging/empty.py", "marts/empty.py"]
)
def test_every_scoped_file_requires_one_local_model(
    tmp_path: Path, relative: str
) -> None:
    write_model(tmp_path, relative, "x = 1\n")

    with pytest.raises(
        DiscoveryError, match="exactly one local concrete model class; found 0"
    ):
        discover(tmp_path)


def test_multiple_local_models_and_imported_classes_are_handled(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "sources/orders.py",
        source_model() + source_model().replace("Orders", "Other"),
    )
    with pytest.raises(DiscoveryError, match="found 2"):
        discover(tmp_path)
    write_model(tmp_path, "sources/orders.py", source_model())
    write_model(tmp_path, "staging/copy.py", "from polars_pipeliner import Model\n")
    with pytest.raises(DiscoveryError, match="found 0"):
        discover(tmp_path)


@pytest.mark.parametrize(
    ("relative", "source", "message"),
    [
        ("sources/orders.py", transform_model(), "must be under sources"),
        ("staging/orders.py", source_model(), "must be under staging or intermediate"),
        ("marts/orders.py", transform_model(), "must be under marts"),
    ],
)
def test_model_placement_is_enforced(
    tmp_path: Path, relative: str, source: str, message: str
) -> None:
    write_model(tmp_path, relative, source)
    with pytest.raises(DiscoveryError, match=message):
        discover(tmp_path)


def test_inputs_signature_and_schema_edges_are_validated_without_execution(
    tmp_path: Path,
) -> None:
    write_model(
        tmp_path,
        "sources/orders.py",
        source_model("raise AssertionError('not called')"),
    )
    write_model(
        tmp_path,
        "staging/orders.py",
        transform_model(
            inputs="{'orders': Input('sources.orders', schema=pl.Schema({'other': pl.Int64}))}",
            parameters="orders",
            body="raise AssertionError('not called')",
        ),
    )
    with pytest.raises(QueryValidationError, match="expects schema"):
        discover(tmp_path)
    write_model(
        tmp_path,
        "staging/orders.py",
        transform_model(
            inputs="{'orders': Input('sources.orders', schema=SCHEMA)}",
            parameters="other",
            body="return other",
        ),
    )
    with pytest.raises(DiscoveryError, match="do not exactly match"):
        discover(tmp_path)


def test_missing_dependencies_and_cycles_are_rejected(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "staging/orders.py",
        transform_model(
            inputs="{'missing': Input('sources.missing', schema=SCHEMA)}",
            parameters="missing",
            body="return missing",
        ),
    )
    with pytest.raises(QueryValidationError, match="missing node"):
        discover(tmp_path)
    (tmp_path / "staging/orders.py").unlink()
    write_model(
        tmp_path,
        "staging/a.py",
        transform_model(
            inputs="{'b': Input('intermediate.b', schema=SCHEMA)}",
            parameters="b",
            body="return b",
        ),
    )
    write_model(
        tmp_path,
        "intermediate/b.py",
        transform_model(
            inputs="{'a': Input('staging.a', schema=SCHEMA)}",
            parameters="a",
            body="return a",
        ),
    )
    with pytest.raises(QueryValidationError, match="Cycle detected"):
        discover(tmp_path)


def test_non_mart_output_is_ignored(tmp_path: Path) -> None:
    write_model(tmp_path, "sources/orders.py", source_model())
    write_model(
        tmp_path,
        "staging/orders.py",
        transform_model(
            inputs="{'orders': Input('sources.orders', schema=SCHEMA)}",
            parameters="orders",
            body="return orders",
        )
        + "\nOrders.output = 'ignored'\n",
    )

    assert discover(tmp_path).node_ids == ("sources.orders", "staging.orders")


def test_mart_requires_a_valid_output_declaration(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "marts/orders.py",
        transform_model(base="MartModel").replace(
            "from polars_pipeliner import Input, MartModel",
            "from polars_pipeliner import Input, MartModel",
        ),
    )

    with pytest.raises(DiscoveryError, match="output declaration"):
        discover(tmp_path)
