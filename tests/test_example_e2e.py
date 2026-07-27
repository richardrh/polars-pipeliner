from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parents[1]
EXAMPLE = PROJECT_ROOT / "examples" / "basic" / "run.py"


def test_basic_example_runs_end_to_end() -> None:
    completed = subprocess.run(
        [sys.executable, str(EXAMPLE)],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert (
        "Resolved order: staging.orders -> intermediate.qualified_orders -> "
        "marts.orders_by_country" in completed.stdout
    )
    assert "Qualified orders:" in completed.stdout
    assert "country" in completed.stdout
    assert "amount" in completed.stdout
    assert "order_id" not in completed.stdout
    assert "4.5" not in completed.stdout
    assert "Orders by country:" in completed.stdout
    assert "total_amount" in completed.stdout
    assert "10.5" in completed.stdout
    assert "7.25" in completed.stdout
    assert "Building LazyFrame plan for model staging.orders" in completed.stderr
    assert "Collecting target LazyFrames: marts.orders_by_country" in completed.stderr
