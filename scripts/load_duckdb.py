"""Muat data curated (hanya yang valid) ke DuckDB sebagai tabel raw_orders."""
import argparse

import duckdb

from common import DATA, ROOT


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--curated", default=str(DATA / "curated" / "orders_curated.parquet"))
    ap.add_argument("--db", default=str(ROOT / "warehouse" / "warehouse.duckdb"))
    args = ap.parse_args()

    path = args.curated.replace("\\", "/").replace("'", "''")
    con = duckdb.connect(args.db)
    con.execute(f"CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM read_parquet('{path}')")
    n = con.execute("SELECT count(*) FROM raw_orders").fetchone()[0]
    print(f"raw_orders dimuat: {n} baris -> {args.db}")
    print(con.execute("DESCRIBE raw_orders").df().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
