#!/bin/bash
# Run Rebate-Alpha S3 backtest with speed optimizations
# Requires: S3_BUCKET, S3_PREFIX, AWS_REGION
#
# Usage:
#   export S3_BUCKET=your-bucket
#   export S3_PREFIX=ETH_USDT/
#   export TRADING_PAIR=ETH_USDT   # optional, default BTC_USDT
#   ./scripts/run_rebate_alpha_backtest.sh
#
# Optional for faster runs:
#   export MAX_CONCURRENT_DOWNLOADS=64   # parallel S3 downloads (default 100)
#   export MAX_FILES=2000                # limit files (default 14280)

set -e

cd "$(dirname "$0")/.."

if [ -z "$S3_BUCKET" ]; then
    echo "Error: S3_BUCKET not set. Example: export S3_BUCKET=backtest-data"
    exit 1
fi

echo "Running Rebate-Alpha backtest (release build)..."
cargo test --test backtest_s3_rebate_alpha --release -- --ignored --nocapture
