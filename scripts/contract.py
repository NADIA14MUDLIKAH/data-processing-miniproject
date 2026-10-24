"""Data contract pesanan (Pydantic v2).

Dipakai bersama oleh validator batch (validate.py) dan consumer Kafka (consumer.py)
supaya aturan di kedua jalur identik.
"""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]

    @field_validator("order_ts")
    @classmethod
    def must_have_timezone(cls, v: datetime) -> datetime:
        # Kontrak: waktu harus eksplisit (ada zona waktu) agar tidak ambigu.
        if v.tzinfo is None:
            raise ValueError("order_ts harus memuat zona waktu (contoh: ...Z)")
        return v


def check_record(payload: dict):
    """Validasi satu record.

    Mengembalikan (True, Order, None) bila lolos, atau
    (False, None, daftar_error) bila gagal. Setiap error berisi field, tipe, pesan.
    """
    try:
        return True, Order.model_validate(payload), None
    except ValidationError as exc:
        errors = [
            {
                "field": ".".join(str(p) for p in e["loc"]) or "(record)",
                "type": e["type"],
                "message": e["msg"],
            }
            for e in exc.errors()
        ]
        return False, None, errors
