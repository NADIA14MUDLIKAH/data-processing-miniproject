"""Jalankan queries/analytics.sql dan cetak hasil tiap query."""
import os
import re

import duckdb

from common import ROOT


def main():
    os.chdir(ROOT)  # supaya path relatif 'data/rejected/...' pada SQL terbaca
    sql = (ROOT / "queries" / "analytics.sql").read_text(encoding="utf-8")
    blocks = [b for b in re.split(r"(?m)^-- name: ", sql) if b.strip()]
    con = duckdb.connect(str(ROOT / "warehouse" / "warehouse.duckdb"), read_only=True)
    for b in blocks:
        title, _, query = b.partition("\n")
        print(f"\n=== {title.strip()}")
        print(con.execute(query).df().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
