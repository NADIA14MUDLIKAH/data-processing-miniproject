
"""Validasi data order menggunakan Pydantic."""

import json
from pathlib import Path
from datetime import datetime
from typing import Literal

import pandas as pd
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


def main():
    source_path = Path("data/orders.csv")
    output_dir = Path("data/validated")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(source_path, dtype=str).fillna("")

    valid_records = []
    rejected_records = []

    for record in df.to_dict(orient="records"):
        try:
            order = Order.model_validate(record)
            valid_records.append(
                order.model_dump(mode="json")
            )
        except ValidationError as exc:
            rejected_records.append({
                "order_id": record.get("order_id", ""),
                "original_payload": record,
                "errors": exc.errors(
                    include_url=False,
                    include_input=False,
                ),
            })

    valid_path = output_dir / "valid_orders.json"
    rejected_path = output_dir / "rejected_orders.json"

    valid_path.write_text(
        json.dumps(valid_records, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    rejected_path.write_text(
        json.dumps(rejected_records, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"total_records={len(df)}")
    print(f"valid_records={len(valid_records)}")
    print(f"rejected_records={len(rejected_records)}")
    print(f"valid_output={valid_path}")
    print(f"rejected_output={rejected_path}")

    if rejected_records:
        print("\nContoh record yang ditolak:")
        first = rejected_records[0]
        print(f"order_id={first['order_id']}")
        for error in first["errors"]:
            field = ".".join(str(x) for x in error["loc"])
            print(f"- field={field}: {error['msg']}")


if __name__ == "__main__":
    main()
