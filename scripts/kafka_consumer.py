
"""Membaca, memvalidasi, dan menyimpan order dari Kafka."""

import argparse
import json
from pathlib import Path
from typing import Literal
from datetime import datetime

from kafka import KafkaConsumer
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]


def load_records(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return []


def upsert_record(records, new_record):
    order_id = new_record.get("order_id", "")
    for index, record in enumerate(records):
        if record.get("order_id", "") == order_id and order_id:
            records[index] = new_record
            return
    records.append(new_record)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--group", default="orders-validation")
    parser.add_argument("--max-messages", type=int, default=36)
    args = parser.parse_args()

    output_dir = Path("data/kafka")
    output_dir.mkdir(parents=True, exist_ok=True)

    valid_path = output_dir / "valid_orders.json"
    rejected_path = output_dir / "rejected_orders.json"

    valid_records = load_records(valid_path)
    rejected_records = load_records(rejected_path)

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers="localhost:9092",
        group_id=args.group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    processed = 0

    try:
        for message in consumer:
            payload = message.value
            order_id = payload.get("order_id", "")

            print(
                f"[CONSUMED] order_id={order_id} "
                f"partition={message.partition} offset={message.offset}"
            )

            try:
                order = Order.model_validate(payload)
                clean_record = order.model_dump(mode="json")
                upsert_record(valid_records, clean_record)
                print(f"[VALID] order_id={order.order_id}")

            except ValidationError as exc:
                rejected_record = {
                    "order_id": order_id,
                    "original_payload": payload,
                    "errors": exc.errors(
                        include_url=False,
                        include_input=False,
                    ),
                    "partition": message.partition,
                    "offset": message.offset,
                }
                upsert_record(rejected_records, rejected_record)
                print(f"[REJECTED] order_id={order_id}")

            # Simpan hasil sebelum melakukan commit offset.
            valid_path.write_text(
                json.dumps(valid_records, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            rejected_path.write_text(
                json.dumps(rejected_records, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )

            consumer.commit()
            processed += 1

            if processed >= args.max_messages:
                break

        print(f"[SUMMARY] processed={processed}")
        print(f"[SUMMARY] valid={len(valid_records)}")
        print(f"[SUMMARY] rejected={len(rejected_records)}")
        print(f"[OUTPUT] valid={valid_path}")
        print(f"[OUTPUT] rejected={rejected_path}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
