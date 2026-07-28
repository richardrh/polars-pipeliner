from pathlib import Path

import polars as pl

from polars_pipeliner import discover


def main() -> None:
    root = Path(__file__).parent
    manifest = discover(root).run()
    print("Manifest:", manifest)
    print("Customer orders:")
    print(pl.read_parquet(manifest["marts.customer_orders"]))


if __name__ == "__main__":
    main()
