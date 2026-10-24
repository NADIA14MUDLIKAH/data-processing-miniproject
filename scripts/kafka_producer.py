
"""Mengirim order dari CSV ke Kafka topic orders."""

import argparse
import json
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--topic", default="orders")
    args = parser.parse_args()

    source_path = Path(args.source)
    df = pd.read_csv(source_path, dtype=str).fillna("")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False
        ).encode("utf-8"),
        acks="all",
        retries=3,
    )

    sent = 0

    try:
        for record in df.to_dict(orient="records"):
            order_id = record.get("order_id", "").strip()

            # Record tanpa order_id tidak bisa menggunakan key order_id.
            if not order_id:
                print("[SKIP] record tanpa order_id")
                continue

            future = producer.send(
                args.topic,
                key=order_id,
                value=record,
            )

            # Menunggu konfirmasi broker agar metadata aktual bisa dicatat.
            metadata = future.get(timeout=30)

            print(
                f"[PRODUCED] order_id={order_id} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}"
            )
            sent += 1

        producer.flush()
        print(f"[SUMMARY] sent={sent} source_records={len(df)}")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
