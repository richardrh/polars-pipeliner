# Getting started

## Install from PyPI

Install Polars Pipeliner from
[PyPI](https://pypi.org/project/polars-pipeliner/):

Using `uv`:

```bash
uv add polars-pipeliner
```

Using `pip`:

```bash
python -m pip install polars-pipeliner
```

Polars Pipeliner requires Python 3.13 or newer.

## Install optional table formats

Install Delta Lake support when a mart writes Delta tables:

```bash
uv add "polars-pipeliner[delta]"
# or: python -m pip install "polars-pipeliner[delta]"
```

Install Apache Iceberg support when a mart writes catalog-managed Iceberg
tables:

```bash
uv add "polars-pipeliner[iceberg]"
# or: python -m pip install "polars-pipeliner[iceberg]"
```

Install both extras:

```bash
uv add "polars-pipeliner[delta,iceberg]"
# or: python -m pip install "polars-pipeliner[delta,iceberg]"
```

File outputs, including Parquet on S3, do not require these extras.

## Create the model folders

Create a project directory and drop each model into the folder that describes
its role:

```text
my-pipeline/
├── sources/
│   └── values.py
├── staging/
│   └── positive_values.py
├── intermediate/
│   └── doubled_values.py
└── marts/
    └── values.py
```
The file path becomes the node ID. For example,
`staging/positive_values.py` becomes `staging.positive_values`.

Each file defines one concrete model class.

## `sources/`: load data

Define:

- an `output_schema`;
- a `source()` method that returns a `LazyFrame`;
- no upstream inputs.

Drop the file into `sources/`.

```python
# sources/values.py
import polars as pl

from polars_pipeliner import SourceModel


VALUE_SCHEMA = pl.Schema({"value": pl.Int64})


class Values(SourceModel):
    output_schema = VALUE_SCHEMA

    def source(self) -> pl.LazyFrame:
        return pl.LazyFrame({"value": [1, 2, 3]}, schema=VALUE_SCHEMA)
```

## `staging/`: clean source data

Define:

- each upstream dependency as an `Input`;
- an `output_schema`;
- a `transform()` method whose parameters match the `Input` attribute names.

Drop the file into `staging/`.

```python
# staging/positive_values.py
import polars as pl

from polars_pipeliner import Input, Model


VALUE_SCHEMA = pl.Schema({"value": pl.Int64})


class PositiveValues(Model):
    values = Input("sources.values", schema=VALUE_SCHEMA)
    output_schema = VALUE_SCHEMA

    def transform(self, values: pl.LazyFrame) -> pl.LazyFrame:
        return values.filter(pl.col("value") > 0)
```

## `intermediate/`: reuse transformations

Define:

- each upstream dependency as an `Input`;
- an `output_schema`;
- a `transform()` method;
- no output destination.

Drop the file into `intermediate/` when its result feeds another
transformation.

```python
# intermediate/doubled_values.py
import polars as pl

from polars_pipeliner import Input, Model


VALUE_SCHEMA = pl.Schema({"value": pl.Int64})


class DoubledValues(Model):
    values = Input("staging.positive_values", schema=VALUE_SCHEMA)
    output_schema = VALUE_SCHEMA

    def transform(self, values: pl.LazyFrame) -> pl.LazyFrame:
        return values.with_columns(pl.col("value") * 2)
```

Intermediate models do not write output files.

## `marts/`: write final outputs

Define:

- each upstream dependency as an `Input`;
- an `output_schema`;
- an `Output` specification;
- a `transform()` method.

Drop the file into `marts/`.

```python
# marts/values.py
import polars as pl

from polars_pipeliner import Input, MartModel, Output


VALUE_SCHEMA = pl.Schema({"value": pl.Int64})


class Values(MartModel):
    values = Input("intermediate.doubled_values", schema=VALUE_SCHEMA)
    output_schema = VALUE_SCHEMA
    output = Output.parquet("target/values.parquet")

    def transform(self, values: pl.LazyFrame) -> pl.LazyFrame:
        return values
```

The executor writes the returned lazy frame to the configured destination.

## Validate the project

Validate model declarations and schemas without writing the mart:

```bash
polars-pipeliner validate my-pipeline
```

## Run the project

```python
from polars_pipeliner import discover


project = discover("my-pipeline")

print(project.resolve())
# ('sources.values', 'staging.positive_values',
#  'intermediate.doubled_values', 'marts.values')

manifest = project.run()
print(manifest["marts.values"])
```

The mart is written to `my-pipeline/target/values.parquet`.

## Next steps

- See [Model types](/concepts/models) for placement and model contracts.
- See [Python API](/reference/api) for the complete public interface.
