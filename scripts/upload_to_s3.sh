#!/bin/bash
# Upload Crypto.com orderbook data to S3
# Usage: ./upload_to_s3.sh <bucket-name> <trading-pair> [start-date] [end-date]

set -e

BUCKET="${1:-backtest-data}"
PAIR="${2:-BTC_USDT}"
START_DATE="${3:-2023-10-25}"
END_DATE="${4:-2023-10-31}"

echo "=== Uploading to S3 ==="
echo "Bucket: $BUCKET"
echo "Trading Pair: $PAIR"
echo "Date Range: $START_DATE to $END_DATE"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    echo "Install it with: brew install awscli"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo "Error: AWS credentials not configured"
    echo "Run: aws configure"
    exit 1
fi

# Create bucket if it doesn't exist
if ! aws s3 ls "s3://$BUCKET" &> /dev/null; then
    echo "Creating bucket: $BUCKET"
    aws s3 mb "s3://$BUCKET" --region us-east-1
fi

# Function to upload files for a specific date
upload_date() {
    local date=$1
    local year=$(echo $date | cut -d'-' -f1)
    local month=$(echo $date | cut -d'-' -f2)
    local day=$(echo $date | cut -d'-' -f3)
    
    local local_path="crates/backtest-engine/cache"
    local s3_prefix="$PAIR/$year/$month/$day/cdc/$PAIR/"
    
    echo "Uploading $date..."
    
    # Check if local files exist
    if [ ! -d "$local_path" ] || [ -z "$(ls -A $local_path/*.gz 2>/dev/null)" ]; then
        echo "  No local cache files found for $date"
        echo "  You may need to download from Crypto.com SFTP first"
        return
    fi
    
    # Upload all .gz files
    aws s3 sync "$local_path/" "s3://$BUCKET/$s3_prefix" \
        --exclude "*" \
        --include "*.gz" \
        --delete \
        --quiet
    
    local count=$(aws s3 ls "s3://$BUCKET/$s3_prefix" --recursive | wc -l)
    echo "  Uploaded $count files for $date"
}

# Convert dates to epoch for iteration
start_epoch=$(date -j -f "%Y-%m-%d" "$START_DATE" +%s 2>/dev/null || date -d "$START_DATE" +%s)
end_epoch=$(date -j -f "%Y-%m-%d" "$END_DATE" +%s 2>/dev/null || date -d "$END_DATE" +%s)

current_epoch=$start_epoch
total_days=0

# Upload each date
while [ $current_epoch -le $end_epoch ]; do
    current_date=$(date -j -f "%s" "$current_epoch" +"%Y-%m-%d" 2>/dev/null || date -d "@$current_epoch" +"%Y-%m-%d")
    upload_date "$current_date"
    
    # Move to next day
    current_epoch=$((current_epoch + 86400))
    total_days=$((total_days + 1))
done

echo ""
echo "=== Upload Complete ==="
echo "Uploaded $total_days days of data for $PAIR"
echo ""
echo "Verify upload:"
echo "  aws s3 ls s3://$BUCKET/$PAIR/ --recursive | head -20"
echo ""
echo "Count files:"
echo "  aws s3 ls s3://$BUCKET/$PAIR/ --recursive | wc -l"

