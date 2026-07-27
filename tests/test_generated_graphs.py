from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from hypothesis import given
from hypothesis import strategies as st

from polars_pipeliner import discover


@given(st.integers(min_value=1, max_value=6))
def test_generated_query_source_chain_resolves_in_dependency_order(count: int) -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        for index in range(count):
            inputs = (
                "{}"
                if index == 0
                else (
                    "{'upstream': QuerySource("
                    f"node_id='node_{index - 1}', schema=SCHEMA)}}"
                )
            )
            parameters = "" if index == 0 else "upstream"
            body = (
                "return pl.LazyFrame({'value': [1]})"
                if index == 0
                else "return upstream"
            )
            root.joinpath(f"node_{index}.py").write_text(
                f"""import polars as pl
from polars_pipeliner import PolarsModel, QueryMetadata, QuerySource
SCHEMA = pl.Schema({{'value': pl.Int64}})
class Model(PolarsModel):
    metadata = QueryMetadata(inputs={inputs}, output_schema=SCHEMA)
    @classmethod
    def transform(cls, {parameters}) -> pl.LazyFrame:
        {body}
"""
            )
        project = discover(root)

    assert project.resolve([f"node_{count - 1}"]) == tuple(
        f"node_{index}" for index in range(count)
    )
