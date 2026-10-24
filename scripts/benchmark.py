"""Benchmark Pandas vs Polars (eager & lazy) dan eksperimen data skew.

Dataset benchmark DIBANGKITKAN dari baris valid (orders_valid.parquet) ke
data/bench/. File sumber data/orders.csv tidak diubah.
Semua angka dicatat apa adanya ke results/*.csv; skrip tidak mengklaim OOM.
"""
import argparse
import json
import os
import platform
import statistics
import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

from common import DATA, ROOT

BENCH = DATA / "bench"
RESULTS = ROOT / "results"
TS_FMT = "%Y-%m-%d %H:%M:%S"


# ---------- pembangkit data ----------
def make_base(n, customers, seed=42):
    valid = pd.read_parquet(DATA / "valid" / "orders_valid.parquet")
    rng = np.random.default_rng(seed)
    base = valid.sample(n=n, replace=True, random_state=seed).reset_index(drop=True)
    base["order_id"] = [f"B{i:09d}" for i in range(n)]
    base["customer_id"] = [f"C{c:04d}" for c in rng.integers(1, customers + 1, size=n)]
    return base


def write_csv(df, path):
    out = df.copy()
    out["order_ts"] = pd.to_datetime(out["order_ts"]).dt.strftime(TS_FMT)
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False)


def make_skewed(base, share, dominant="C0001", seed=7):
    """Gandakan baris dengan customer_id dominan sampai porsinya ~ `share`."""
    extra_n = int(len(base) * share / (1 - share))
    extra = base.sample(n=extra_n, replace=True, random_state=seed).reset_index(drop=True)
    extra["customer_id"] = dominant
    extra["order_id"] = [f"S{i:09d}" for i in range(extra_n)]
    return pd.concat([base, extra], ignore_index=True)


# ---------- pekerjaan yang dibandingkan (identik) ----------
def job_pandas(path):
    df = pd.read_csv(path)
    df["order_ts"] = pd.to_datetime(df["order_ts"], format=TS_FMT)
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
    df["order_date"] = df["order_ts"].dt.date
    return df.groupby(["order_date", "product_category"])["net_amount"].sum()


def _polars_exprs():
    return [
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount_pct"))).alias("net_amount"),
        pl.col("order_ts").str.to_datetime(TS_FMT).dt.date().alias("order_date"),
    ]


def job_polars_eager(path):
    df = pl.read_csv(path).with_columns(_polars_exprs())
    return df.group_by(["order_date", "product_category"]).agg(pl.col("net_amount").sum())


def lazy_query(path):
    return (pl.scan_csv(path).with_columns(_polars_exprs())
            .group_by(["order_date", "product_category"]).agg(pl.col("net_amount").sum()))


def job_polars_lazy(path):
    return lazy_query(path).collect()


def timeit(fn, repeats, warmup=1):
    for _ in range(warmup):
        fn()
    t = []
    for _ in range(repeats):
        s = time.perf_counter()
        fn()
        t.append(time.perf_counter() - s)
    return {"median_s": statistics.median(t), "min_s": min(t), "max_s": max(t), "runs": repeats}


def machine_info():
    info = {"python": platform.python_version(), "platform": platform.platform(),
            "cpu_count": os.cpu_count(), "pandas": pd.__version__, "polars": pl.__version__}
    try:
        import psutil
        info["ram_gb"] = round(psutil.virtual_memory().total / 1024**3, 1)
    except ImportError:
        info["ram_gb"] = "tidak diukur (psutil tidak terpasang)"
    return info


# ---------- eksperimen skew ----------
def group_stats(path):
    counts = pl.read_csv(path, columns=["customer_id"]).group_by("customer_id").len()
    total = counts["len"].sum()
    return {"rows": int(total), "groups": counts.height,
            "max_group_rows": int(counts["len"].max()),
            "median_group_rows": float(counts["len"].median()),
            "top_group_share": round(counts["len"].max() / total, 4)}


def job_pandas_customer(path):
    df = pd.read_csv(path, usecols=["customer_id", "quantity", "unit_price", "discount_pct"])
    df["net_amount"] = df["quantity"] * df["unit_price"] * (1 - df["discount_pct"])
    return df.groupby("customer_id")["net_amount"].sum()


def job_polars_customer(path):
    return (pl.scan_csv(path).select(["customer_id", "quantity", "unit_price", "discount_pct"])
            .group_by("customer_id")
            .agg((pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount_pct"))).sum().alias("net_amount"))
            .collect())


def job_pandas_chunked(path, chunksize=200_000):
    """Agregasi parsial per chunk, lalu digabung (mitigasi memori)."""
    parts = []
    for chunk in pd.read_csv(path, usecols=["customer_id", "quantity", "unit_price", "discount_pct"], chunksize=chunksize):
        chunk["net_amount"] = chunk["quantity"] * chunk["unit_price"] * (1 - chunk["discount_pct"])
        parts.append(chunk.groupby("customer_id")["net_amount"].sum())
    return pd.concat(parts).groupby(level=0).sum()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--customers", type=int, default=1000)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--skew-share", type=float, default=0.5)
    args = ap.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)

    base = make_base(args.rows, args.customers)
    uniform_csv = BENCH / "orders_uniform.csv"
    write_csv(base, uniform_csv)
    skewed = make_skewed(base, args.skew_share)
    skewed_csv = BENCH / "orders_skewed.csv"
    write_csv(skewed, skewed_csv)
    size_mb = round(uniform_csv.stat().st_size / 1024**2, 1)
    info = machine_info()
    print(json.dumps(info, indent=2))
    print(f"dataset uniform: {args.rows} baris, {size_mb} MB")

    # --- Benchmark 1: Pandas vs Polars eager vs Polars lazy ---
    rows = []
    for name, fn in [("pandas", job_pandas), ("polars_eager", job_polars_eager), ("polars_lazy", job_polars_lazy)]:
        r = timeit(lambda f=fn: f(uniform_csv), args.repeats)
        rows.append({"engine": name, "rows": args.rows, "file_mb": size_mb, **r})
        print(f"{name:14s} median={r['median_s']:.3f}s min={r['min_s']:.3f}s max={r['max_s']:.3f}s")
    pd.DataFrame(rows).to_csv(RESULTS / "benchmark_results.csv", index=False)

    # Kebenaran hasil: ketiganya harus menghasilkan agregat yang sama
    a = job_pandas(uniform_csv)
    b = job_polars_lazy(uniform_csv).to_pandas().set_index(["order_date", "product_category"])["net_amount"]
    print("hasil pandas == polars:", bool(np.allclose(a.sort_index().values, b.sort_index().values)))

    # Rencana query lazy (bukti projection pushdown)
    plan = ["== NAIF ==", lazy_query(uniform_csv).explain(optimized=False),
            "== OPTIMIZED ==", lazy_query(uniform_csv).explain()]
    (RESULTS / "lazy_plan.txt").write_text("\n".join(plan), encoding="utf-8")

    # --- Benchmark 2: skew ---
    skew_rows = []
    for label, path in [("uniform", uniform_csv), ("skewed", skewed_csv)]:
        st = group_stats(path)
        for name, fn in [("pandas_groupby", job_pandas_customer), ("polars_lazy_groupby", job_polars_customer),
                         ("pandas_chunked_partial_agg", job_pandas_chunked)]:
            r = timeit(lambda f=fn, p=path: f(p), args.repeats)
            skew_rows.append({"dataset": label, "engine": name, **st, **r})
            print(f"[{label}] {name:28s} median={r['median_s']:.3f}s  top_share={st['top_group_share']}")
    pd.DataFrame(skew_rows).to_csv(RESULTS / "skew_results.csv", index=False)

    full = job_pandas_customer(skewed_csv).sort_index()
    part = job_pandas_chunked(skewed_csv).sort_index()
    print("agregasi chunked == agregasi penuh:", bool(np.allclose(full.values, part.values)))
    (RESULTS / "machine_info.json").write_text(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
