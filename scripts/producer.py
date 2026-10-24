"""Producer Kafka: membaca orders.csv dan menerbitkan event ke topic 'orders'."""
import argparse
import csv
import json

from kafka import KafkaProducer

from common import DATA


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(DATA / "orders.csv"))
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--limit", type=int, default=None, help="jumlah baris pertama yang dikirim")
    ap.add_argument("--duplicate-id", default=None,
                    help="order_id yang dikirim ulang di akhir (uji duplikasi)")
    ap.add_argument("--bad-json", action="store_true",
                    help="kirim satu pesan bukan-JSON (uji penanganan pesan rusak)")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",     # tunggu konfirmasi broker
        retries=5,      # retry otomatis pada error sementara (bisa menimbulkan duplikat)
        linger_ms=10,
    )

    with open(args.source, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if args.limit:
        rows = rows[: args.limit]

    to_send = list(rows)
    if args.duplicate_id:
        dup = [r for r in rows if r["order_id"] == args.duplicate_id]
        if not dup:
            raise SystemExit(f"order_id {args.duplicate_id} tidak ada pada data yang dikirim")
        to_send.append(dup[0])  # event yang sama dikirim kedua kali

    futures = []
    for r in to_send:
        key = r["order_id"].encode("utf-8")           # key = order_id -> event sama ke partition sama
        value = json.dumps(r).encode("utf-8")
        futures.append((r["order_id"], producer.send(args.topic, key=key, value=value)))

    if args.bad_json:
        futures.append(("(bad-json)", producer.send(args.topic, key=b"BAD", value=b"{ini bukan json")))

    producer.flush()
    for oid, fut in futures:
        md = fut.get(timeout=10)
        print(f"sent order_id={oid} topic={md.topic} partition={md.partition} offset={md.offset}")
    print(f"total terkirim = {len(futures)}")


if __name__ == "__main__":
    main()
