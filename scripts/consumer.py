"""Consumer Kafka: validasi kontrak, tulis valid/rejected, sink idempotent.

Semantik: at-least-once. Offset di-commit MANUAL setelah penulisan ke sink.
Bila proses mati setelah menulis tetapi sebelum commit, pesan akan dibaca lagi;
duplikat bisnis dicegah oleh pengecekan order_id pada sink (idempotent sink).
"""
import argparse
import json
from datetime import timezone

from kafka import KafkaConsumer

from common import DATA, append_jsonl, utc_now_iso
from contract import check_record

STREAM = DATA / "stream"
VALID_FILE = STREAM / "valid_events.jsonl"
REJECTED_FILE = STREAM / "rejected_events.jsonl"
DUP_FILE = STREAM / "duplicate_events.jsonl"


def load_seen_ids():
    seen = set()
    if VALID_FILE.exists():
        for line in VALID_FILE.read_text(encoding="utf-8").splitlines():
            if line.strip():
                seen.add(json.loads(line)["order_id"])
    return seen


def load_rejected_offsets():
    """(partition, offset) pesan rejected yang sudah tercatat -> sink rejected juga idempotent."""
    seen = set()
    if REJECTED_FILE.exists():
        for line in REJECTED_FILE.read_text(encoding="utf-8").splitlines():
            if line.strip():
                r = json.loads(line)
                seen.add((r.get("_partition"), r.get("_offset")))
    return seen


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--group-id", default="orders-consumer-v1")
    ap.add_argument("--idle-timeout-ms", type=int, default=8000,
                    help="berhenti bila tidak ada pesan baru selama waktu ini")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        enable_auto_commit=False,        # commit manual setelah sink sukses
        auto_offset_reset="earliest",    # group baru mulai dari awal
        consumer_timeout_ms=args.idle_timeout_ms,
    )

    seen = load_seen_ids()
    seen_rejected = load_rejected_offsets()
    stats = {"consumed": 0, "valid_written": 0, "duplicates_skipped": 0,
             "rejected": 0, "rejected_replay_skipped": 0}
    print(f"group={args.group_id} topic={args.topic} id_sudah_ada={len(seen)}")

    for msg in consumer:
        stats["consumed"] += 1
        meta = {"_topic": msg.topic, "_partition": msg.partition, "_offset": msg.offset}

        if (msg.partition, msg.offset) in seen_rejected:  # pesan rejected yang sama terbaca ulang
            stats["rejected_replay_skipped"] += 1
            consumer.commit()
            continue

        try:
            payload = json.loads(msg.value.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("payload bukan objek JSON")
        except Exception as exc:  # JSON rusak
            append_jsonl(REJECTED_FILE, {
                "order_id": None, "error_types": "invalid_json", "error_messages": str(exc),
                "payload": msg.value.decode("utf-8", errors="replace"),
                "rejected_at": utc_now_iso(), "source": "stream", **meta})
            seen_rejected.add((msg.partition, msg.offset))
            stats["rejected"] += 1
            consumer.commit()
            continue

        ok, order, errors = check_record(payload)
        if not ok:
            append_jsonl(REJECTED_FILE, {
                "order_id": payload.get("order_id") or None,
                "error_fields": ",".join(sorted({e["field"] for e in errors})),
                "error_types": ",".join(e["type"] for e in errors),
                "error_messages": " | ".join(f'{e["field"]}: {e["message"]}' for e in errors),
                "payload": json.dumps(payload, ensure_ascii=False),
                "rejected_at": utc_now_iso(), "source": "stream", **meta})
            seen_rejected.add((msg.partition, msg.offset))
            stats["rejected"] += 1
        elif order.order_id in seen:
            append_jsonl(DUP_FILE, {"order_id": order.order_id, "seen_at": utc_now_iso(), **meta})
            stats["duplicates_skipped"] += 1
            print(f"  [duplikat dilewati] order_id={order.order_id} partition={msg.partition} offset={msg.offset}")
        else:
            row = order.model_dump()
            row["order_ts"] = row["order_ts"].astimezone(timezone.utc).replace(tzinfo=None).isoformat()
            append_jsonl(VALID_FILE, {**row, **meta})
            seen.add(order.order_id)
            stats["valid_written"] += 1

        consumer.commit()  # commit SETELAH sink selesai -> at-least-once

    consumer.close()
    stats.update({"step": "consumer", "group_id": args.group_id, "ts": utc_now_iso()})
    print(json.dumps(stats, indent=2))
    append_jsonl(DATA / "logs" / "consumer_runs.jsonl", stats)


if __name__ == "__main__":
    main()
