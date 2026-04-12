#!/bin/bash
# Run Balanced MM S3 backtest on ETHUSDT or BTCUSDT.
#
# Usage:
#   ./scripts/run_balanced_mm_backtest.sh           # ETH_USDT (default)
#   TRADING_PAIR=BTC_USDT ./scripts/run_balanced_mm_backtest.sh
#
# Set S3_BUCKET, S3_PREFIX for your data. Example for ETH:
#   export S3_BUCKET=ethusdt2025
#   export S3_PREFIX=ETH_USDT/

set -e

cd "$(dirname "$0")/.."
SCRIPT_DIR="$(pwd)"

export S3_BUCKET="${S3_BUCKET:-ethusdt2025}"
export S3_PREFIX="${S3_PREFIX:-ETH_USDT/}"
export TRADING_PAIR="${TRADING_PAIR:-ETH_USDT}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export MAX_FILES="${MAX_FILES:-4000}"
export MAX_CONCURRENT_DOWNLOADS="${MAX_CONCURRENT_DOWNLOADS:-64}"

LOG="backtest_balanced_mm_$(date +%Y%m%d_%H%M%S).log"
LOG_PATH="${SCRIPT_DIR}/${LOG}"

echo "=== Balanced MM Backtest ==="
echo "Pair: ${TRADING_PAIR}"
echo "Log: ${LOG_PATH}"
echo ""

if command -v stdbuf >/dev/null 2>&1; then
  stdbuf -oL -eL cargo test --test backtest_s3 --release -- --ignored --nocapture 2>&1 | tee "$LOG_PATH"
else
  cargo test --test backtest_s3 --release -- --ignored --nocapture 2>&1 | tee "$LOG_PATH"
fi

echo ""
echo "Done. Log: ${LOG_PATH}"
