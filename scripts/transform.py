"""Transformasi dengan Polars: gabung valid batch (+ stream), dedup, hitung metrik."""
import argparse
import json
from pathlib import Path

import polars as pl

from common import DATA, append_jsonl, utc_now_iso

COLS = ["order_id", "customer_id", "order_ts", "product_category", "quantity",
        "unit_price", "discount_pct", "payment_method", "status"]


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(COLS).with_columns(
        pl.col("order_id").cast(pl.Utf8),
        pl.col("customer_id").cast(pl.Utf8),
        pl.col("order_ts").cast(pl.Datetime("us")),
        pl.col("quantity").cast(pl.Int64),
        pl.col("unit_price").cast(pl.Float64),
        pl.col("discount_pct").cast(pl.Float64),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--valid", default=str(DATA / "valid" / "orders_valid.parquet"))
    ap.add_argument("--stream", default=str(DATA / "stream" / "valid_events.jsonl"))
    ap.add_argument("--include-stream", action="store_true")
    ap.add_argument("--out", default=str(DATA / "curated" / "orders_curated.parquet"))
    args = ap.parse_args()

    frames = [normalize(pl.read_parquet(args.valid)).with_columns(pl.lit("batch").alias("source"))]
    if args.include_stream and Path(args.stream).exists() and Path(args.stream).stat().st_size > 0:
        s = pl.read_ndjson(args.stream).with_columns(pl.col("order_ts").str.to_datetime())
        frames.append(normalize(s).with_columns(pl.lit("stream").alias("source")))

    combined = pl.concat(frames, how="vertical")
    deduped = combined.unique(subset="order_id", keep="first", maintain_order=True)

    curated = deduped.with_columns(
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    ).with_columns(
        (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
    ).sort("order_ts")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    curated.write_parquet(args.out)

    summary = {"step": "transform", "ts": utc_now_iso(), "rows_in": combined.height,
               "duplicates_removed": combined.height - deduped.height,
               "rows_out": curated.height, "out": args.out}
    print(json.dumps(summary, indent=2))
    print(curated.schema)
    append_jsonl(DATA / "logs" / "transform_runs.jsonl", summary)


if __name__ == "__main__":
    main()
