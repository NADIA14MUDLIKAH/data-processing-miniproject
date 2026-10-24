# Mini Project Data Processing Workflow
## End-to-End Event-to-Warehouse Pipeline: Analisis Transaksi E-Commerce

Pipeline ini menggabungkan ingestion batch/API, streaming Kafka, data contract (Pydantic),
transformasi Polars, warehouse DuckDB, dan model analitik dbt. Seluruh data bersifat **sintetis**.
Dataset `data/orders.csv` dari paket pendukung **tidak diubah**; semua keluaran ditulis ke folder lain.

## Prasyarat
- Python 3.10 - 3.12 (disarankan; dbt-duckdb dan kafka-python belum tentu mendukung versi yang lebih baru)
- Docker Desktop / Docker Engine dengan Docker Compose v2 (hanya untuk bagian Kafka)
- Sekitar 1 GB ruang disk bila memakai benchmark 1.000.000 baris

## Struktur folder
```text
.
├── README.md
├── requirements.txt
├── docker-compose.yml          # Kafka single-node (dari paket pendukung)
├── data/
│   ├── orders.csv              # sumber (JANGAN diubah)
│   ├── raw/                    # Parquet mentah (semua nilai asli, string)
│   ├── valid/                  # record lolos kontrak
│   ├── rejected/               # record ditolak + alasan
│   ├── stream/                 # keluaran consumer Kafka (jsonl)
│   ├── curated/                # hasil transformasi Polars
│   ├── checkpoints/            # watermark.json
│   ├── logs/                   # metrik tiap langkah (jsonl)
│   └── bench/                  # data benchmark buatan (dibangkitkan skrip)
├── scripts/
│   ├── batch_extract.py        # ekstraksi: pagination, retry, watermark, Parquet
│   ├── contract.py             # model Pydantic (data contract)
│   ├── validate.py             # validasi batch: quarantine / failfast
│   ├── producer.py             # Kafka producer
│   ├── consumer.py             # Kafka consumer + sink idempotent
│   ├── transform.py            # transformasi Polars
│   ├── load_duckdb.py          # muat data valid ke DuckDB
│   ├── benchmark.py            # Pandas vs Polars + eksperimen skew
│   ├── run_queries.py          # jalankan queries/analytics.sql
│   └── verify_pipeline.py      # uji otomatis properti pipeline
├── queries/analytics.sql
├── results/                    # hasil benchmark
├── warehouse/warehouse.duckdb  # dibuat oleh load_duckdb.py
├── dbt_project/                # proyek dbt (staging, marts, tests)
└── Modul_Praktikum.docx        # modul / laporan praktikum
```

## Cara menjalankan (semua perintah dari folder proyek)

### 1. Lingkungan
```bash
python -m venv .venv
# Windows PowerShell:  .venv\Scripts\Activate.ps1
# macOS/Linux:         source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Batch ingestion (uji pagination, retry, watermark)
```bash
python scripts/batch_extract.py --max-rows 20 --simulate-failures 2 --backoff-base 0.2  # run 1
python scripts/batch_extract.py --max-rows 20                                           # run 2: appended = 0
python scripts/batch_extract.py                                                         # run 3: appended = 16
python scripts/batch_extract.py                                                         # run 4: appended = 0
```
Untuk mengulang dari nol: hapus `data/raw/orders.parquet` dan `data/checkpoints/watermark.json`.

### 3. Data contract
```bash
python scripts/validate.py                    # quarantine: 30 valid, 6 rejected
python scripts/validate.py --mode failfast    # keluar dengan kode 1 bila ada pelanggaran
```

### 4. Kafka streaming
```bash
docker compose up -d
docker compose ps
docker exec dpw-kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
python scripts/producer.py --duplicate-id O1001 --bad-json
python scripts/consumer.py                    # group orders-consumer-v1
python scripts/consumer.py --group-id orders-consumer-v2   # baca ulang: tidak ada duplikasi di sink
docker compose down
```
Jalankan producer sekali per topic baru. Bila diulang pada topic yang sama, pesan bertambah.

### 5. Transformasi dan DuckDB
```bash
python scripts/transform.py --include-stream   # tanpa --include-stream bila Kafka dilewati
python scripts/load_duckdb.py
python scripts/verify_pipeline.py
```

### 6. Benchmark dan skew
```bash
python scripts/benchmark.py --rows 1000000 --repeats 5
# bila RAM terbatas: python scripts/benchmark.py --rows 200000 --repeats 5
```
Hasil: `results/benchmark_results.csv`, `results/skew_results.csv`, `results/lazy_plan.txt`, `results/machine_info.json`.

### 7. dbt
```bash
cd dbt_project
dbt debug --profiles-dir .
dbt run   --profiles-dir .
dbt test  --profiles-dir .
dbt docs generate --profiles-dir .
dbt docs serve    --profiles-dir . --port 8080     # buka http://localhost:8080
cd ..
python scripts/run_queries.py
```

## Jaminan dan batas implementasi
- Consumer memakai commit offset manual setelah sink ditulis: semantiknya **at-least-once**.
  Duplikasi bisnis dicegah oleh sink idempotent berdasarkan `order_id`. Ini **bukan** exactly-once end-to-end.
- Watermark berbasis `order_ts` dapat melewatkan data terlambat dengan timestamp lama; mitigasi: lookback window.
- Adapter API adalah CSV lokal berpaginasi. `RestApiAdapter` pada `batch_extract.py` adalah kerangka (belum diuji pada API nyata).
- Benchmark dijalankan pada satu mesin dan data buatan; hasilnya tidak dapat digeneralisasi.
