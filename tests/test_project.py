from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest

from polars_pipeliner import (
    BuildResult,
    DiscoveryError,
    QueryValidationError,
    discover,
)


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
    input_declarations: str = "",
    parameters: str = "",
    body: str = "return pl.LazyFrame({'value': [1]})",
    base: str = "Model",
) -> str:
    return f"""import polars as pl
from polars_pipeliner import Input, {base}
SCHEMA = {SCHEMA}
class Orders({base}):
    {input_declarations}
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
            input_declarations="orders = Input('sources.orders', schema=pl.Schema({'other': pl.Int64}))",
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
            input_declarations="orders = Input('sources.orders', schema=SCHEMA)",
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
            input_declarations="missing = Input('sources.missing', schema=SCHEMA)",
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
            input_declarations="b = Input('intermediate.b', schema=SCHEMA)",
            parameters="b",
            body="return b",
        ),
    )
    write_model(
        tmp_path,
        "intermediate/b.py",
        transform_model(
            input_declarations="a = Input('staging.a', schema=SCHEMA)",
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
            input_declarations="orders = Input('sources.orders', schema=SCHEMA)",
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


def test_direct_input_attributes_allow_zero_one_and_multiple_inputs(
    tmp_path: Path,
) -> None:
    write_model(tmp_path, "sources/orders.py", source_model())
    write_model(
        tmp_path,
        "staging/empty.py",
        transform_model(body="return pl.LazyFrame({'value': [1]})"),
    )
    write_model(
        tmp_path,
        "staging/one.py",
        transform_model(
            input_declarations="orders = Input('sources.orders', schema=SCHEMA)",
            parameters="orders",
            body="return orders",
        ),
    )
    write_model(
        tmp_path,
        "intermediate/multiple.py",
        transform_model(
            input_declarations=(
                "orders = Input('sources.orders', schema=SCHEMA)\n"
                "    copy = Input('staging.one', schema=SCHEMA)"
            ),
            parameters="orders, copy",
            body="return orders",
        ),
    )

    assert discover(tmp_path).resolve() == (
        "sources.orders",
        "staging.empty",
        "staging.one",
        "intermediate.multiple",
    )


@pytest.mark.parametrize("relative", ["staging/orders.py", "sources/orders.py"])
def test_inputs_mapping_is_rejected_at_discovery(tmp_path: Path, relative: str) -> None:
    source = (
        source_model()
        .replace(
            "    def source",
            "    inputs = {'orders': Input('sources.orders', schema=pl.Schema({'value': pl.Int64}))}\n\n    def source",
        )
        .replace(
            "from polars_pipeliner import SourceModel",
            "from polars_pipeliner import Input, SourceModel",
        )
    )
    write_model(
        tmp_path,
        relative,
        source
        if relative.startswith("sources")
        else transform_model(
            input_declarations="inputs = {'orders': Input('sources.orders', schema=SCHEMA)}",
            parameters="orders",
            body="return orders",
        ),
    )

    with pytest.raises(DiscoveryError, match=r"inputs = \{\.\.\.\} is not supported"):
        discover(tmp_path)


def test_an_input_named_inputs_is_a_valid_direct_declaration(tmp_path: Path) -> None:
    write_model(tmp_path, "sources/orders.py", source_model())
    write_model(
        tmp_path,
        "staging/orders.py",
        transform_model(
            input_declarations="inputs = Input('sources.orders', schema=SCHEMA)",
            parameters="inputs",
            body="return inputs",
        ),
    )

    assert discover(tmp_path).node_ids == ("sources.orders", "staging.orders")


def test_sources_may_not_declare_inputs(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "sources/orders.py",
        source_model()
        .replace(
            "    def source",
            "    orders = Input('sources.orders', schema=pl.Schema({'value': pl.Int64}))\n\n    def source",
        )
        .replace(
            "from polars_pipeliner import SourceModel",
            "from polars_pipeliner import Input, SourceModel",
        ),
    )

    with pytest.raises(DiscoveryError, match="may not declare Input attributes"):
        discover(tmp_path)


def test_colliding_path_derived_node_ids_are_rejected(tmp_path: Path) -> None:
    first_path = tmp_path / "staging/orders.v1.py"
    duplicate_path = tmp_path / "staging/orders/v1.py"
    write_model(tmp_path, "staging/orders.v1.py", transform_model())
    write_model(
        tmp_path,
        "staging/orders/v1.py",
        'raise AssertionError("must not import duplicate")\n',
    )

    with pytest.raises(QueryValidationError) as caught:
        discover(tmp_path)

    assert str(caught.value) == (
        f"Duplicate node ID 'staging.orders.v1': {first_path} and {duplicate_path}"
    )


def test_abstract_helpers_and_aliases_do_not_inflate_model_count(
    tmp_path: Path,
) -> None:
    write_model(
        tmp_path,
        "staging/orders.py",
        f"""from abc import ABC, abstractmethod
import polars as pl
from polars_pipeliner import Model

class Helper(Model, ABC):
    @abstractmethod
    def transform(self) -> pl.LazyFrame:
        raise NotImplementedError

class Orders(Model):
    output_schema = {SCHEMA}
    def transform(self) -> pl.LazyFrame:
        return pl.LazyFrame({{"value": [1]}})

OrdersAlias = Orders
""",
    )

    assert discover(tmp_path).node_ids == ("staging.orders",)


def test_invalid_input_node_id_is_a_discovery_error(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "staging/orders.py",
        transform_model(
            input_declarations="orders = Input(123, schema=SCHEMA)",
            parameters="orders",
            body="return orders",
        ),
    )

    with pytest.raises(DiscoveryError, match="has an invalid input node ID or schema"):
        discover(tmp_path)


def test_non_function_model_method_is_a_discovery_error(tmp_path: Path) -> None:
    write_model(
        tmp_path,
        "sources/orders.py",
        source_model().replace(
            "    def source(self) -> pl.LazyFrame:\n"
            "        return pl.LazyFrame({'value': [1]})",
            "    source = 1",
        ),
    )

    with pytest.raises(
        DiscoveryError, match="must define ordinary instance method source"
    ):
        discover(tmp_path)


def test_failed_model_validation_does_not_register_module(tmp_path: Path) -> None:
    successful_root = tmp_path / "successful"
    failed_root = tmp_path / "failed"
    write_model(successful_root, "sources/orders.py", source_model())
    discover(successful_root)
    prefix = "_polars_pipeliner_model_"
    registered = {
        name: module for name, module in sys.modules.items() if name.startswith(prefix)
    }
    write_model(
        failed_root,
        "sources/orders.py",
        """import polars as pl
from polars_pipeliner import SourceModel

class Orders(SourceModel):
    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({"value": [1]})
""",
    )

    with pytest.raises(DiscoveryError, match="must define its own output_schema"):
        discover(failed_root)

    assert {
        name: module for name, module in sys.modules.items() if name.startswith(prefix)
    } == registered


def test_project_exposes_build_result_not_raw_graph(tmp_path: Path) -> None:
    write_model(tmp_path, "sources/orders.py", source_model())

    project = discover(tmp_path)
    result = project.build()

    assert isinstance(result, BuildResult)
    assert tuple(result.frames) == ("sources.orders",)
    assert tuple(result.schemas) == ("sources.orders",)
    with pytest.raises(TypeError):
        cast(Any, result.frames)["other"] = pl.LazyFrame()
    with pytest.raises(TypeError):
        cast(Any, result.schemas)["other"] = pl.Schema()
    assert not hasattr(project, "graph")
    assert project.node_ids == ("sources.orders",)
    assert project.resolve() == ("sources.orders",)
