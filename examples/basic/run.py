from pathlib import Path

from polars_pipeliner import discover


def main() -> None:
    root = Path(__file__).parent
    project = discover(root / "queries", config_path=root / "polars-pipeliner.toml")
    targets = ["marts.orders_by_country"]
    order = project.resolve(targets)
    print("Resolved order:", " -> ".join(order))

    built = project.build(targets)
    print("Qualified orders:")
    print(built.frames["intermediate.qualified_orders"].collect())

    results = project.run(targets)
    print("Orders by country:")
    print(results["marts.orders_by_country"])


if __name__ == "__main__":
    main()
