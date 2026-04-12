#!/bin/bash
# Check available dates on Crypto.com SFTP server
# Usage: ./check_available_dates.sh [trading_pair] [year]
#   trading_pair: BTC_USDT (default), ETH_USDT, etc.
#   year:         2025 (default), "all", or specific year e.g. 2024

set -e

PAIR="${1:-BTC_USDT}"
YEAR="${2:-2025}"
if [[ "$1" == "all" ]] || [[ "$1" == "2023" ]] || [[ "$1" == "2024" ]] || [[ "$1" == "2025" ]] || [[ "$1" == "2026" ]]; then
    YEAR="$1"
    PAIR="${2:-BTC_USDT}"
fi

SFTP_USER="${CRYPTO_COM_SFTP_USERNAME:-user080}"
# Key in mm/ - use ./ when running from mm; override via env if needed
KEY_PATH="${CRYPTO_COM_SFTP_KEY_PATH:-./user080 (1) 2}"

echo "=== Checking Available Dates for $PAIR ==="
echo "SFTP User: $SFTP_USER"
echo "Year: $YEAR"
echo ""

# Build CLI tool if not exists
if [ ! -f "./target/release/mm-cli" ]; then
    echo "Building CLI tool..."
    cargo build --release --bin mm-cli
fi

# Run check dates command
./target/release/mm-cli check-dates \
    --sftp-username "$SFTP_USER" \
    --sftp-key-path "$KEY_PATH" \
    --sftp-base-path "exchange/book_l2_150_0010" \
    --trading-pair "$PAIR" \
    --year "$YEAR"

echo ""
echo "=== Summary ==="
echo "Use this information to determine which dates to upload."
echo ""
echo "Example upload command:"
echo "./target/release/mm-cli upload \\"
echo "    --s3-bucket backtest-data \\"
echo "    --s3-prefix $PAIR/ \\"
echo "    --start-date YYYY-MM-DD \\"
echo "    --end-date YYYY-MM-DD"


