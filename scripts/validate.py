"""Validasi raw Parquet terhadap data contract.

Mode:
  quarantine (default) : record valid dan rejected dipisah; proses lanjut.
  failfast             : bila ada satu saja pelanggaran, tidak ada output valid
                         ditulis dan proses keluar dengan kode 1.
"""
import argparse
import json
import sys
from datetime import timezone
from pathlib import Path

import pandas as pd

from common import DATA, append_jsonl, utc_now_iso
from contract import check_record

BUSINESS_COLS = [
    "order_id", "customer_id", "order_ts", "product_category", "quantity",
    "unit_price", "discount_pct", "payment_method", "status",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=str(DATA / "raw" / "orders.parquet"))
    ap.add_argument("--valid-out", default=str(DATA / "valid" / "orders_valid.parquet"))
    ap.add_argument("--rejected-out", default=str(DATA / "rejected" / "orders_rejected.parquet"))
    ap.add_argument("--mode", choices=["quarantine", "failfast"], default="quarantine")
    args = ap.parse_args()

    raw = pd.read_parquet(args.raw)
    valid_rows, rejected_rows = [], []

    for rec in raw.to_dict(orient="records"):
        payload = {k: rec.get(k) for k in BUSINESS_COLS}
        ok, order, errors = check_record(payload)
        if ok:
            row = order.model_dump()
            # Standarisasi: simpan sebagai timestamp naive dalam UTC.
            row["order_ts"] = row["order_ts"].astimezone(timezone.utc).replace(tzinfo=None)
            valid_rows.append(row)
        else:
            rejected_rows.append({
                "order_id": payload.get("order_id") or None,
                "error_fields": ",".join(sorted({e["field"] for e in errors})),
                "error_types": ",".join(e["type"] for e in errors),
                "error_messages": " | ".join(f'{e["field"]}: {e["message"]}' for e in errors),
                "payload": json.dumps(payload, ensure_ascii=False),
                "rejected_at": utc_now_iso(),
                "source": "batch",
            })

    summary = {
        "step": "validate", "mode": args.mode, "ts": utc_now_iso(),
        "records_read": len(raw), "records_valid": len(valid_rows),
        "records_rejected": len(rejected_rows),
    }
    print(json.dumps(summary, indent=2))
    append_jsonl(DATA / "logs" / "validate_runs.jsonl", summary)

    if rejected_rows:
        Path(args.rejected_out).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rejected_rows).to_parquet(args.rejected_out, index=False)
        print(f"rejected -> {args.rejected_out}")

    if args.mode == "failfast" and rejected_rows:
        print("FAIL-FAST: ada pelanggaran kontrak, tidak ada data valid yang ditulis.", file=sys.stderr)
        for r in rejected_rows:
            print(f'  {r["order_id"]}: {r["error_messages"]}', file=sys.stderr)
        return 1

    Path(args.valid_out).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(valid_rows).to_parquet(args.valid_out, index=False)
    print(f"valid    -> {args.valid_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
