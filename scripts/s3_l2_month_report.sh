#!/usr/bin/env bash
# Summarize L2 .gz inventory on S3 for one month prefix (total + per-day file counts).
# Usage: ./scripts/s3_l2_month_report.sh <bucket> <prefix>
# PREFIX must include year/month folders, trailing slash optional.
# Examples:
#   ./scripts/s3_l2_month_report.sh l2btcusd 2025/2025/01/
#   S3_REPORT_PAIR=ETH_USD ./scripts/s3_l2_month_report.sh l2ethusd 2025/2025/01/
set -euo pipefail
BUCKET="${1:?bucket}"
PREFIX="${2:?prefix e.g. 2025/2025/01/}"
[[ "$PREFIX" == */ ]] || PREFIX="${PREFIX}/"
PAIR="${S3_REPORT_PAIR:-BTC_USD}"

echo "=== s3://$BUCKET/$PREFIX (recursive --summarize) ==="
aws s3 ls "s3://$BUCKET/$PREFIX" --recursive --summarize | tail -3

echo ""
echo "=== Per-day object rows under s3://$BUCKET/${PREFIX}DD/cdc/$PAIR/ (list = 1 line per object) ==="
if command -v jot >/dev/null 2>&1; then DAYS=$(jot 31 1); else DAYS=$(seq 1 31); fi
for d in $DAYS; do
  dd=$(printf '%02d' "$d")
  c=$(aws s3 ls "s3://$BUCKET/${PREFIX}${dd}/cdc/${PAIR}/" 2>/dev/null | wc -l | tr -d ' ')
  echo "$dd $c"
done
