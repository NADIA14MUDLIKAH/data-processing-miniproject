"""Uji otomatis sederhana untuk membuktikan properti pipeline."""
import sys

import duckdb
import pandas as pd

from common import DATA, ROOT

DB = ROOT / "warehouse" / "warehouse.duckdb"


def main() -> int:
    con = duckdb.connect(str(DB), read_only=True)
    results = []

    def check(name, ok, detail=""):
        results.append(ok)
        print(f'[{"PASS" if ok else "FAIL"}] {name} {detail}')

    n_rows, n_unique = con.execute("SELECT count(*), count(DISTINCT order_id) FROM raw_orders").fetchone()
    check("order_id unik di raw_orders", n_rows == n_unique, f"(baris={n_rows}, unik={n_unique})")

    rej_path = DATA / "rejected" / "orders_rejected.parquet"
    if rej_path.exists():
        rej_ids = [i for i in pd.read_parquet(rej_path)["order_id"].dropna().tolist() if i]
        if rej_ids:
            leaked = con.execute("SELECT count(*) FROM raw_orders WHERE order_id IN (SELECT unnest(?))",
                                 [rej_ids]).fetchone()[0]
            check("record rejected tidak ada di warehouse", leaked == 0, f"(rejected={len(rej_ids)}, bocor={leaked})")

    bad_calc = con.execute("""
        SELECT count(*) FROM raw_orders
        WHERE abs(gross_amount - quantity * unit_price) > 0.005
           OR abs(net_amount - quantity * unit_price * (1 - discount_pct)) > 0.005
    """).fetchone()[0]
    check("gross_amount dan net_amount sesuai rumus", bad_calc == 0, f"(baris salah={bad_calc})")

    bad_status = con.execute(
        "SELECT count(*) FROM raw_orders WHERE status NOT IN ('paid','shipped','cancelled','refunded')"
    ).fetchone()[0]
    check("status hanya nilai yang diizinkan", bad_status == 0)

    bad_disc = con.execute("SELECT count(*) FROM raw_orders WHERE discount_pct < 0 OR discount_pct > 1").fetchone()[0]
    check("discount_pct dalam rentang 0..1", bad_disc == 0)

    con.close()
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
