from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from hypothesis import given
from hypothesis import strategies as st

from polars_pipeliner import discover


@given(st.integers(min_value=1, max_value=6))
def test_generated_model_chain_resolves_in_dependency_order(count: int) -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        root.joinpath("sources").mkdir()
        root.joinpath("sources/node_0.py").write_text("""import polars as pl
from polars_pipeliner import SourceModel
class Node(SourceModel):
    output_schema = pl.Schema({'value': pl.Int64})
    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({'value': [1]})
""")
        for index in range(1, count):
            root.joinpath("staging").mkdir(exist_ok=True)
            root.joinpath(f"staging/node_{index}.py").write_text(f"""import polars as pl
from polars_pipeliner import Input, Model
class Node(Model):
    inputs = {{'upstream': Input('{("sources.node_0" if index == 1 else f"staging.node_{index - 1}")}', schema=pl.Schema({{'value': pl.Int64}}))}}
    output_schema = pl.Schema({{'value': pl.Int64}})
    def transform(self, upstream: pl.LazyFrame) -> pl.LazyFrame:
        return upstream
""")
        project = discover(root)
    assert project.resolve() == ("sources.node_0",) + tuple(
        f"staging.node_{index}" for index in range(1, count)
    )
