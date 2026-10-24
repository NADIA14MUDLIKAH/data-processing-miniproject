
"""Transformasi order valid dengan checkpoint dan lookback."""

import json
from pathlib import Path

import pandas as pd


INPUT_PATH = Path("data/validated/valid_orders.json")
OUTPUT_DIR = Path("data/transformed")
OUTPUT_PATH = OUTPUT_DIR / "orders.parquet"
CHECKPOINT_PATH = OUTPUT_DIR / "watermark.json"

# Lookback membantu membaca ulang data dari satu hari sebelumnya.
LOOKBACK_DAYS = 1


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_json(INPUT_PATH)
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True)

    # Membaca watermark transformasi jika tersedia.
    watermark = None
    if CHECKPOINT_PATH.exists():
        checkpoint = json.loads(
            CHECKPOINT_PATH.read_text(encoding="utf-8")
        )
        watermark = checkpoint.get("watermark")

    # Filter berdasarkan lookback jika watermark sudah ada.
    if watermark:
        watermark_ts = pd.Timestamp(watermark)
        if watermark_ts.tzinfo is None:
            watermark_ts = watermark_ts.tz_localize("UTC")

        cutoff = watermark_ts - pd.Timedelta(days=LOOKBACK_DAYS)
        df = df[df["order_ts"] >= cutoff]

    # Menghitung nilai transaksi.
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])

    # Upsert: gabungkan data lama dan baru, lalu deduplikasi berdasarkan order_id.
    if OUTPUT_PATH.exists():
        old_df = pd.read_parquet(OUTPUT_PATH)
        df = pd.concat([old_df, df], ignore_index=True)
        df = df.drop_duplicates(subset="order_id", keep="last")

    df = df.sort_values("order_ts")
    df.to_parquet(OUTPUT_PATH, index=False)

    # Simpan watermark baru tanpa membiarkan checkpoint mundur.
    if not df.empty:
        new_watermark_ts = df["order_ts"].max()

        if watermark:
            old_watermark_ts = pd.Timestamp(watermark)
            if old_watermark_ts.tzinfo is None:
                old_watermark_ts = old_watermark_ts.tz_localize("UTC")

            new_watermark_ts = max(new_watermark_ts, old_watermark_ts)

        new_watermark = new_watermark_ts.isoformat()

        CHECKPOINT_PATH.write_text(
            json.dumps({"watermark": new_watermark}, indent=2),
            encoding="utf-8",
        )
    print(f"processed_records={len(df)}")
    print(f"output={OUTPUT_PATH}")
    print(f"checkpoint={CHECKPOINT_PATH}")
    print(f"watermark={df['order_ts'].max().isoformat() if not df.empty else 'none'}")
    print(f"unique_order_id={df['order_id'].nunique()}")
    print(f"duplicate_order_id={df['order_id'].duplicated().sum()}")


if __name__ == "__main__":
    main()
