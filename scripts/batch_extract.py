"""Ekstraksi batch dari 'API' berpaginasi (CSV lokal) -> raw Parquet.

Fitur: pagination (page, page_size), retry + exponential backoff, simulasi
kegagalan sementara, watermark persisten (JSON), upsert berdasarkan order_id,
dan log metrik per proses (JSONL).

Catatan desain: raw layer menyimpan nilai ASLI sebagai string (termasuk nilai
cacat) supaya dapat diaudit. Validasi tipe dilakukan di tahap kontrak.
"""
import argparse
import json
import random
import time
from pathlib import Path

import pandas as pd

from common import DATA, append_jsonl, utc_now_iso


class TransientAPIError(Exception):
    """Error sementara (setara HTTP 429/5xx / timeout)."""


class CsvApiAdapter:
    """Meniru endpoint GET /orders?page=&page_size= dengan sumber CSV lokal."""

    def __init__(self, path, max_rows=None, fail_first_n=0):
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        self.df = df.iloc[:max_rows] if max_rows else df
        self.fail_left = fail_first_n  # jumlah panggilan awal yang sengaja gagal

    def get(self, page: int, page_size: int) -> dict:
        if self.fail_left > 0:
            self.fail_left -= 1
            raise TransientAPIError("simulasi: 503 Service Unavailable")
        start = page * page_size
        items = self.df.iloc[start:start + page_size].to_dict(orient="records")
        return {"page": page, "page_size": page_size, "total": len(self.df), "items": items}


class RestApiAdapter:
    """KERANGKA untuk API sungguhan (tidak dipakai pada demo lokal)."""

    def __init__(self, base_url, token=None, timeout=10):
        import requests
        self.requests, self.base_url, self.timeout = requests, base_url, timeout
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}

    def get(self, page: int, page_size: int) -> dict:
        try:
            r = self.requests.get(f"{self.base_url}/orders", headers=self.headers,
                                  params={"page": page, "page_size": page_size},
                                  timeout=self.timeout)
        except (self.requests.Timeout, self.requests.ConnectionError) as exc:
            raise TransientAPIError(str(exc)) from exc
        if r.status_code == 429 or r.status_code >= 500:
            wait = r.headers.get("Retry-After")
            if wait and wait.isdigit():
                time.sleep(int(wait))
            raise TransientAPIError(f"HTTP {r.status_code}")
        r.raise_for_status()  # 4xx lain = bukan error sementara, jangan di-retry
        return r.json()


def fetch_with_retry(adapter, page, page_size, retries, backoff_base):
    for attempt in range(retries + 1):
        try:
            return adapter.get(page, page_size)
        except TransientAPIError as exc:
            if attempt == retries:
                raise
            delay = backoff_base * (2 ** attempt) + random.uniform(0, backoff_base / 2)
            print(f"  [retry] page={page} attempt={attempt + 1} error='{exc}' tunggu={delay:.2f}s")
            time.sleep(delay)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(DATA / "orders.csv"))
    ap.add_argument("--out", default=str(DATA / "raw"))
    ap.add_argument("--checkpoint", default=str(DATA / "checkpoints" / "watermark.json"))
    ap.add_argument("--page-size", type=int, default=10)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--backoff-base", type=float, default=0.5)
    ap.add_argument("--simulate-failures", type=int, default=0,
                    help="jumlah panggilan awal yang sengaja gagal (uji retry)")
    ap.add_argument("--max-rows", type=int, default=None,
                    help="batasi isi sumber (simulasi data bertambah dari waktu ke waktu)")
    args = ap.parse_args()

    adapter = CsvApiAdapter(args.source, args.max_rows, args.simulate_failures)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    target = out / "orders.parquet"
    ckpt = Path(args.checkpoint)

    watermark = None
    if ckpt.exists():
        watermark = pd.Timestamp(json.loads(ckpt.read_text())["watermark"])
    print(f"watermark awal = {watermark}")

    # 1) Pagination sampai halaman kosong
    items, page = [], 0
    while True:
        resp = fetch_with_retry(adapter, page, args.page_size, args.retries, args.backoff_base)
        print(f"page={page} rows={len(resp['items'])}")
        if not resp["items"]:
            break
        items.extend(resp["items"])
        page += 1
    fetched = pd.DataFrame(items)
    read_n = len(fetched)

    # 2) Filter incremental. Timestamp tak terbaca tidak bisa dibandingkan,
    #    jadi tetap diteruskan ke tahap kontrak (dedup oleh order_id).
    ts = pd.to_datetime(fetched["order_ts"], errors="coerce", utc=True) if read_n else pd.Series(dtype="datetime64[ns, UTC]")
    if watermark is not None and read_n:
        keep = ts.isna() | (ts > watermark)
        candidates = fetched[keep.values].copy()
    else:
        candidates = fetched.copy()

    # 3) Upsert berdasarkan order_id (idempotent)
    existing = pd.read_parquet(target) if target.exists() else None
    if existing is not None:
        new_rows = candidates[~candidates["order_id"].isin(existing["order_id"])].copy()
    else:
        new_rows = candidates.drop_duplicates("order_id", keep="last").copy()
    duplicates = len(candidates) - len(new_rows)

    new_rows["_ingested_at"] = utc_now_iso()
    new_rows["_source_file"] = Path(args.source).name
    combined = pd.concat([existing, new_rows], ignore_index=True) if existing is not None else new_rows
    combined.to_parquet(target, index=False)

    # 4) Watermark baru disimpan SETELAH penulisan sukses
    all_ts = pd.to_datetime(combined["order_ts"], errors="coerce", utc=True)
    if all_ts.notna().any():
        ckpt.parent.mkdir(parents=True, exist_ok=True)
        ckpt.write_text(json.dumps({"watermark": all_ts.max().isoformat()}, indent=2))

    metrics = {
        "step": "batch_extract", "ts": utc_now_iso(), "pages": page,
        "records_read": read_n,
        "records_after_watermark": len(candidates),
        "records_duplicate_skipped": duplicates,
        "records_appended": len(new_rows),
        "total_in_raw": len(combined),
        "watermark_before": str(watermark),
        "watermark_after": str(all_ts.max()),
    }
    print(json.dumps(metrics, indent=2))
    append_jsonl(DATA / "logs" / "batch_runs.jsonl", metrics)


if __name__ == "__main__":
    main()
