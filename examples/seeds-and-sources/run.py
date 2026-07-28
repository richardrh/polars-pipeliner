from pathlib import Path

import polars as pl

from polars_pipeliner import discover


def main() -> None:
    root = Path(__file__).parent
    project = discover(root, config_path=root / "polars-build-tool.toml")
    manifest = project.run()
    print("Manifest:", manifest)
    print("Revenue by region:")
    print(pl.read_parquet(manifest["marts.revenue_by_region"]))


if __name__ == "__main__":
    main()
