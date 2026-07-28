from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from shutil import rmtree

import polars as pl
import pytest
from polars.testing import assert_frame_equal

PROJECT_ROOT = Path(__file__).parents[1]
CUSTOMER_ORDERS_EXAMPLE = PROJECT_ROOT / "examples" / "customer-orders"
SEEDS_AND_SOURCES_EXAMPLE = PROJECT_ROOT / "examples" / "seeds-and-sources"
CUSTOMER_ORDERS_TARGET = CUSTOMER_ORDERS_EXAMPLE / "target"

EXPECTED_CUSTOMER_ORDERS = pl.DataFrame(
    {
        "customer_id": [1, 2, 3, 4],
        "customer_name": [
            "Ada Lovelace",
            "Grace Hopper",
            "Alan Turing",
            "Katherine Johnson",
        ],
        "segment": ["Enterprise", "Small Business", "Enterprise", "Public Sector"],
        "order_count": [2, 2, 1, 0],
        "units_ordered": [3, 4, 2, 0],
        "total_spend": [130.0, 100.0, 500.0, 0.0],
    },
    schema={
        "customer_id": pl.Int64,
        "customer_name": pl.String,
        "segment": pl.String,
        "order_count": pl.UInt32,
        "units_ordered": pl.Int64,
        "total_spend": pl.Float64,
    },
)


@pytest.mark.parametrize(
    ("command", "cwd"),
    [
        ([sys.executable, "examples/customer-orders/run.py"], PROJECT_ROOT),
        ([sys.executable, "run.py"], CUSTOMER_ORDERS_EXAMPLE),
    ],
)
def test_customer_orders_example_runs_end_to_end(command: list[str], cwd: Path) -> None:
    assert not CUSTOMER_ORDERS_EXAMPLE.is_symlink()
    rmtree(CUSTOMER_ORDERS_TARGET, ignore_errors=True)
    try:
        subprocess.run(command, cwd=cwd, check=True, capture_output=True, text=True)
        actual = pl.read_parquet(CUSTOMER_ORDERS_TARGET / "customer_orders.parquet")
        assert actual.schema == EXPECTED_CUSTOMER_ORDERS.schema
        assert_frame_equal(actual, EXPECTED_CUSTOMER_ORDERS, check_row_order=True)
    finally:
        rmtree(CUSTOMER_ORDERS_TARGET, ignore_errors=True)


@pytest.mark.parametrize(
    ("command", "cwd"),
    [
        ([sys.executable, "examples/seeds-and-sources/run.py"], PROJECT_ROOT),
        ([sys.executable, "run.py"], SEEDS_AND_SOURCES_EXAMPLE),
    ],
)
def test_seeds_and_sources_example_runs_end_to_end(
    command: list[str], cwd: Path
) -> None:
    completed = subprocess.run(
        command,
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Manifest:" in completed.stdout
    assert "Revenue by region:" in completed.stdout
    assert "Europe" in completed.stdout
    assert "North America" in completed.stdout
