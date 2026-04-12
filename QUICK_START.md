# Quick Start

## Prerequisites

- Rust: `rustc --version` / `cargo --version`
- AWS credentials configured (for S3 backtests)
- Python 3 + venv (for visualization)

## Project Structure

```bash
cd "/Users/azlan/Market making /mm"
```

## S3 Data

| Bucket      | Pair      | Path      |
|-------------|-----------|-----------|
| l2ethusd    | ETH_USD   | `2025/`   |
| l2btcusdt   | BTC_USDT  | `2025/`   |
| l2btcusd    | BTC_USD   | `2025/`   |

Structure: `s3://bucket/2025/MM/DD/cdc/PAIR/*.gz`

## Run Backtest (New Engine: RebateMM)

```bash
cd mm

MAX_FILES=2040 \
S3_BUCKET=l2ethusd \
S3_PREFIX=2025/ \
TRADING_PAIR=ETH_USD \
cargo test -p mm-engine backtest_s3_rebate_mm --release -- --ignored --nocapture
```

**~24h of data:** `MAX_FILES=2040`  
**~1 week:** `MAX_FILES=14280`  
**Specific month:** `S3_PREFIX=2025/01/`

## Export CSV for Visualization

Add `BACKTEST_OUTPUT_CSV` to write equity curve and fills data:

```bash
BACKTEST_OUTPUT_CSV=crates/engine/backtest_results.csv MAX_FILES=2040 \
S3_BUCKET=l2ethusd S3_PREFIX=2025/ TRADING_PAIR=ETH_USD \
cargo test -p mm-engine backtest_s3_rebate_mm --release -- --ignored --nocapture
```

Creates `backtest_results.csv` (equity snapshots) and `backtest_results_fills.csv` (all fills).

## Visualize Results

```bash
# One-time: create venv and install deps
python3 -m venv .venv
source .venv/bin/activate
pip install pandas plotly

# Generate charts
python scripts/visualize_backtest.py crates/engine/backtest_results.csv
```

Produces: `backtest_report.html` — one file with equity curve, drawdown, daily P&L, and backtest date range.

Use `--individual` to also save separate chart files.

## Verify Build

```bash
cargo check --workspace
cargo test -p mm-engine  # unit tests only, no S3
```
