#!/bin/bash
# Run Rebate-Alpha S3 backtest, export full logs to timestamped file, then generate regime aggregate.

set -e

cd "$(dirname "$0")/.."
SCRIPT_DIR="$(pwd)"

export S3_BUCKET="${S3_BUCKET:-ethusdt2025}"
export S3_PREFIX="${S3_PREFIX:-ETH_USDT/}"
export TRADING_PAIR="${TRADING_PAIR:-ETH_USDT}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export MAX_FILES="${MAX_FILES:-4000}"
export MAX_CONCURRENT_DOWNLOADS="${MAX_CONCURRENT_DOWNLOADS:-64}"

LOG="backtest_$(date +%Y%m%d_%H%M%S).log"
LOG_PATH="${SCRIPT_DIR}/${LOG}"

echo "=== Rebate-Alpha Backtest ==="
echo "Log: ${LOG_PATH}"
echo ""

# Run backtest: stdbuf for full log + live; fallback to plain tee if stdbuf missing
if command -v stdbuf >/dev/null 2>&1; then
  stdbuf -oL -eL cargo test --test backtest_s3_rebate_alpha --release -- --ignored --nocapture 2>&1 | tee "$LOG_PATH"
else
  cargo test --test backtest_s3_rebate_alpha --release -- --ignored --nocapture 2>&1 | tee "$LOG_PATH"
fi

echo ""
echo "=== Regime Aggregate (by day) ==="
"${SCRIPT_DIR}/scripts/aggregate_regime_by_day.sh" "$LOG_PATH"

echo ""
echo "=== L1 Fills Aggregate (by day) ==="
"${SCRIPT_DIR}/scripts/aggregate_fills_by_day.sh" "$LOG_PATH"

echo ""
echo "=== P&L by Entry Regime ==="
"${SCRIPT_DIR}/scripts/aggregate_pnl_by_regime.sh" "$LOG_PATH"

echo ""
echo "Done. Log: ${LOG_PATH}"
