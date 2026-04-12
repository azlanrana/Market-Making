#!/usr/bin/env python3
"""
Merge monthly backtest equity CSVs (timestamp, portfolio_value) into one file for
scripts/visualize_backtest.py — without re-running the engine.

Each month is an independent simulation; this script *chains* portfolio_value so the
first point of month N+1 matches the last point of month N (synthetic continuous curve
for charts). Total P&L over the chain equals the sum of per-month (end−start) P&Ls.

Usage:
  cd mm && .venv/bin/python scripts/combine_monthly_backtests.py -o combined_2025_l2ethusd.csv \\
    jan2025_l2ethusd.csv feb2025_l2ethusd.csv mar2025_l2ethusd.csv

  .venv/bin/python scripts/combine_monthly_backtests.py -o out.csv --glob '*2025_l2ethusd.csv'

Or glob all month files (order is determined by data min timestamp, not filename):
  .venv/bin/python scripts/combine_monthly_backtests.py -o combined.csv --glob '*_l2ethusd.csv'

Dependencies: pip install pandas
"""

from __future__ import annotations

import argparse
import glob
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Error: pip install pandas", file=sys.stderr)
    sys.exit(1)


def load_equity(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = df.columns.str.strip()
    if "timestamp" not in df.columns or "portfolio_value" not in df.columns:
        raise ValueError(f"{path}: need columns timestamp, portfolio_value")
    df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp", "portfolio_value"])
    df["timestamp"] = df["timestamp"].astype("int64")
    df["portfolio_value"] = df["portfolio_value"].astype(float)
    return df.sort_values("timestamp").reset_index(drop=True)


def chain_segments(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate monthly equity series with portfolio_value continuous at boundaries."""
    if not dfs:
        raise ValueError("No input frames")
    # Chronological order by first timestamp in each segment
    ordered = sorted(dfs, key=lambda d: int(d["timestamp"].iloc[0]))
    parts: list[pd.DataFrame] = []
    running_end: float | None = None
    for df in ordered:
        if running_end is None:
            out = df.copy()
            running_end = float(out["portfolio_value"].iloc[-1])
        else:
            gap = running_end - float(df["portfolio_value"].iloc[0])
            out = df.copy()
            out["portfolio_value"] = out["portfolio_value"] + gap
            running_end = float(out["portfolio_value"].iloc[-1])
        parts.append(out)
    combined = pd.concat(parts, ignore_index=True)
    combined = combined.sort_values("timestamp").reset_index(drop=True)
    # Collapse duplicate timestamps (boundary edge case)
    combined = combined.groupby("timestamp", as_index=False).agg(
        {"portfolio_value": "last"}
    )
    combined = combined.sort_values("timestamp").reset_index(drop=True)
    return combined


def guess_asset_hint(stem: str) -> str:
    """Best-effort --asset for visualize_backtest.py from output filename stem."""
    s = stem.lower()
    if "l2btc" in s or "btcusd" in s:
        return "BTC_USD"
    if "l2eth" in s or "ethusd" in s:
        return "ETH_USD"
    return "YOUR_PAIR"


def merge_fills(equity_paths: list[Path], output_stem: Path) -> Path | None:
    """Concatenate <stem>_fills.csv next to each equity file; write next to output."""
    fills_frames: list[pd.DataFrame] = []
    for p in equity_paths:
        fills_path = p.parent / f"{p.stem}_fills{p.suffix}"
        if not fills_path.exists():
            continue
        df = pd.read_csv(fills_path)
        df.columns = df.columns.str.strip()
        fills_frames.append(df)
    if not fills_frames:
        return None
    all_fills = pd.concat(fills_frames, ignore_index=True)
    if "timestamp" in all_fills.columns:
        all_fills["timestamp"] = pd.to_numeric(all_fills["timestamp"], errors="coerce")
        all_fills = all_fills.dropna(subset=["timestamp"])
        all_fills = all_fills.sort_values("timestamp").reset_index(drop=True)
        # Dedupe identical rows at month boundaries (unlikely)
        all_fills = all_fills.drop_duplicates().reset_index(drop=True)
    out = output_stem.parent / f"{output_stem.stem}_fills{output_stem.suffix}"
    all_fills.to_csv(out, index=False)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Merge monthly backtest equity CSVs for visualize_backtest.py"
    )
    parser.add_argument(
        "csv_files",
        nargs="*",
        type=Path,
        help="Monthly portfolio CSV paths (timestamp, portfolio_value)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        metavar="PATH",
        help="Output portfolio CSV path (e.g. combined_2025_l2ethusd.csv)",
    )
    parser.add_argument(
        "--glob",
        action="append",
        dest="globs",
        metavar="PATTERN",
        help="Glob pattern(s); merged with positional files (quote the pattern)",
    )
    parser.add_argument(
        "--no-chain",
        action="store_true",
        help="Only concatenate and sort (no portfolio rebasing at month joins)",
    )
    parser.add_argument(
        "--no-fills",
        action="store_true",
        help="Do not merge companion *_fills.csv files",
    )
    args = parser.parse_args()

    paths: list[Path] = []
    for g in args.globs or []:
        paths.extend(Path(p) for p in sorted(glob.glob(g)))
    paths.extend(args.csv_files)

    # Dedupe while preserving order
    seen: set[str] = set()
    unique: list[Path] = []
    for p in paths:
        rp = str(p.resolve())
        if rp in seen:
            continue
        seen.add(rp)
        unique.append(p)

    if not unique:
        print("Error: no input CSVs (pass files or --glob)", file=sys.stderr)
        sys.exit(1)

    missing = [p for p in unique if not p.exists()]
    if missing:
        for p in missing:
            print(f"Error: not found: {p}", file=sys.stderr)
        sys.exit(1)

    segments = [load_equity(p) for p in unique]

    if args.no_chain:
        combined = pd.concat(segments, ignore_index=True)
        combined = combined.sort_values("timestamp").reset_index(drop=True)
        combined = combined.groupby("timestamp", as_index=False).agg(
            {"portfolio_value": "last"}
        )
    else:
        combined = chain_segments(segments)

    out_path = args.output
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    combined.to_csv(out_path, index=False)
    print(f"Wrote {len(combined)} rows → {out_path}")

    if not args.no_fills:
        fills_out = merge_fills(unique, out_path)
        if fills_out:
            print(f"Merged fills → {fills_out}")
        else:
            print("No *_fills.csv found next to inputs (skipped)")

    asset = guess_asset_hint(out_path.stem)
    print(
        "\nNext: python scripts/visualize_backtest.py "
        f"{out_path.name} --asset {asset} --output-dir {out_path.parent} --no-browser"
    )
    if asset == "YOUR_PAIR":
        print("  Pass the correct --asset for your pair.")


if __name__ == "__main__":
    main()
