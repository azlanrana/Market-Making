#!/usr/bin/env python3
"""
Per-fill 1s adverse selection distribution (bimodal / toxic fraction).

Input: `<stem>_markout_1s.csv` from `backtest_s3_rebate_mm` + `BACKTEST_OUTPUT_CSV`.

Legacy `*_fills.csv` has no mids — re-run backtest once to emit `*_markout_1s.csv`.

Usage:
  python3 scripts/analyze_markout_1s.py path/to/jan2025_l2ethusd_markout_1s.csv
  python3 scripts/analyze_markout_1s.py path/to/markout.csv --fills path/to/fills.csv
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def _quantile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return float("nan")
    idx = q * (len(sorted_vals) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (idx - lo)


def analyze_stdlib(path: Path, fills_path: Path | None, toxic_override: float | None) -> int:
    with path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = list(r)

    if not rows:
        print("Empty CSV", file=sys.stderr)
        return 1

    cols = set(rows[0].keys())
    if "adverse_bps" not in cols or "side" not in cols:
        print(
            "This file is not *_markout_1s.csv (need adverse_bps, side). "
            "Re-run backtest with BACKTEST_OUTPUT_CSV to generate <stem>_markout_1s.csv.",
            file=sys.stderr,
        )
        print(f"Columns: {sorted(cols)}", file=sys.stderr)
        return 1

    thr = toxic_override
    if thr is None and "toxic_threshold_bps" in cols and rows[0].get("toxic_threshold_bps"):
        try:
            thr = float(rows[0]["toxic_threshold_bps"])
        except ValueError:
            thr = None
    if thr is None:
        thr = 0.2

    adverse = [float(row["adverse_bps"]) for row in rows]
    adverse.sort()
    n = len(adverse)
    mean = statistics.fmean(adverse)
    std = statistics.pstdev(adverse) if n > 1 else 0.0

    toxic = sum(1 for x in adverse if x > thr) / n
    good = sum(1 for x in adverse if x < -thr) / n
    neutral = 1.0 - toxic - good

    print(f"File: {path}  rows={n:,}  toxic_threshold_bps={thr}")
    print("\n--- adverse_bps (1s, + = bad for MM) ---")
    print(f"  mean={mean:.6f}  std={std:.6f}  min={adverse[0]:.6f}  max={adverse[-1]:.6f}")
    qs = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
    for q in qs:
        print(f"  p{int(q * 100):02d}={_quantile(adverse, q):.6f}")

    print(f"\nClassifier (|.| > {thr} bps):  good={good:.2%}  neutral={neutral:.2%}  toxic={toxic:.2%}")

    if "fill_reason" in cols:
        by_r: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            by_r[row.get("fill_reason") or ""].append(float(row["adverse_bps"]))
        print("\n--- By fill_reason (count / mean adverse_bps) ---")
        for k in sorted(by_r.keys(), key=lambda x: (-len(by_r[x]), x)):
            v = by_r[k]
            m = statistics.fmean(v)
            print(f"  {k or '(empty)':20}  n={len(v):7}  mean={m:+.6f}")

    by_s: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_s[row["side"]].append(float(row["adverse_bps"]))
    print("\n--- By side ---")
    for k, v in sorted(by_s.items()):
        print(f"  {k:6}  n={len(v):7}  mean={statistics.fmean(v):+.6f}")

    if fills_path and fills_path.is_file():
        with fills_path.open(newline="", encoding="utf-8") as f:
            fr = csv.DictReader(f)
            frows = {row["order_id"]: float(row["value_usd"]) for row in fr if row.get("order_id")}
        num = den = 0.0
        for row in rows:
            oid = row.get("order_id")
            if oid in frows:
                w = frows[oid]
                num += float(row["adverse_bps"]) * w
                den += w
        if den > 0:
            print(f"\nValue_usd-weighted mean adverse_bps: {num / den:.6f}")

    return 0


def analyze_pandas(path: Path, fills_path: Path | None, toxic_override: float | None) -> int:
    import pandas as pd

    df = pd.read_csv(path)
    need = {"adverse_bps", "side"}
    if not need.issubset(df.columns):
        print(f"Expected columns including {need}, got: {list(df.columns)}", file=sys.stderr)
        return 1

    thr = toxic_override
    if thr is None and "toxic_threshold_bps" in df.columns and pd.notna(df["toxic_threshold_bps"].iloc[0]):
        thr = float(df["toxic_threshold_bps"].iloc[0])
    if thr is None:
        thr = 0.2

    a = df["adverse_bps"].astype(float)
    print(f"File: {path}  rows={len(df):,}  toxic_threshold_bps={thr}")
    print("\n--- adverse_bps (1s, MM sign: + = bad) ---")
    print(a.describe(percentiles=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]))
    print("\nQuantiles:", a.quantile([0.1, 0.25, 0.5, 0.75, 0.9]).to_dict())

    toxic = (a > thr).mean()
    good = (a < -thr).mean()
    neutral = 1.0 - toxic - good
    print(f"\nClassifier (|.| > {thr} bps):  good={good:.2%}  neutral={neutral:.2%}  toxic={toxic:.2%}")

    if "fill_reason" in df.columns:
        print("\n--- Mean adverse_bps by fill_reason ---")
        print(df.groupby("fill_reason", dropna=False)["adverse_bps"].agg(["count", "mean", "std"]))

    print("\n--- Mean adverse_bps by side ---")
    print(df.groupby("side")["adverse_bps"].agg(["count", "mean", "std"]))

    if fills_path is not None and fills_path.is_file():
        fills = pd.read_csv(fills_path)
        if "order_id" not in fills.columns:
            print("Fills CSV missing order_id; skip merge", file=sys.stderr)
            return 0
        j = df.merge(fills[["order_id", "value_usd"]], on="order_id", how="left")
        w = j["value_usd"].fillna(0).astype(float)
        if w.sum() > 0:
            wa = (j["adverse_bps"].astype(float) * w).sum() / w.sum()
            print(f"\nValue_usd-weighted mean adverse_bps: {wa:.4f}")

    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Analyze 1s adverse selection from markout CSV")
    p.add_argument("markout_csv", type=Path)
    p.add_argument("--fills", type=Path, default=None)
    p.add_argument("--toxic-bps", type=float, default=None, dest="toxic_bps")
    p.add_argument(
        "--pandas",
        action="store_true",
        help="Use pandas if installed (richer describe())",
    )
    args = p.parse_args()

    if not args.markout_csv.is_file():
        print(f"Not found: {args.markout_csv}", file=sys.stderr)
        print(
            "Re-run: BACKTEST_OUTPUT_CSV=... cargo test -p mm-engine backtest_s3_rebate_mm ... "
            "→ writes <stem>_markout_1s.csv",
            file=sys.stderr,
        )
        return 1

    if args.pandas:
        try:
            return analyze_pandas(args.markout_csv, args.fills, args.toxic_bps)
        except ImportError:
            print("pandas not installed; using stdlib", file=sys.stderr)

    return analyze_stdlib(args.markout_csv, args.fills, args.toxic_bps)


if __name__ == "__main__":
    raise SystemExit(main())
