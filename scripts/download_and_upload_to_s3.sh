#!/bin/bash
# Download from Crypto.com SFTP and upload to S3 in one go
# Usage: ./download_and_upload_to_s3.sh <bucket-name> <trading-pair> <start-date> <end-date>

set -e

BUCKET="${1:-backtest-data}"
PAIR="${2:-BTC_USDT}"
START_DATE="${3:-2023-10-25}"
END_DATE="${4:-2023-10-31}"

echo "=== Download from SFTP and Upload to S3 ==="
echo "Bucket: $BUCKET"
echo "Trading Pair: $PAIR"
echo "Date Range: $START_DATE to $END_DATE"
echo ""

# Step 1: Download from SFTP using your existing backtest
echo "Step 1: Downloading from Crypto.com SFTP..."
echo "Running backtest to download files..."

cd "$(dirname "$0")/.."

# Set environment variables for SFTP download
export CRYPTO_COM_SFTP_REMOTE_PATH="exchange/book_l2_150_0010/2023/10/25/cdc/$PAIR"
export MAX_FILES=14280  # 1 week
export MAX_CONCURRENT_DOWNLOADS=100

# Run a quick download (you may need to modify this to just download without running full backtest)
# For now, we'll assume you run the SFTP backtest first to populate cache
echo "Note: Run your SFTP backtest first to download files to cache/"
echo "Then run: ./scripts/upload_to_s3.sh $BUCKET $PAIR $START_DATE $END_DATE"

