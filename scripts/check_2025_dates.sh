#!/bin/bash
# Check which days in 2025 have BTC_USDT data

set -e

KEY_FILE="user080 (1) 2"
SFTP_USER="user080"
PAIR="BTC_USDT"

echo "=== Checking 2025 BTC_USDT Data Availability ==="
echo ""

# Check each month
for month in {1..12}; do
    month_padded=$(printf "%02d" $month)
    
    echo "Checking 2025-$month_padded..."
    
    # List days in this month
    days=$(sftp -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        -b - user080@data.crypto.com <<EOF 2>/dev/null | grep -E "^[0-9]+" | sort -n
ls exchange/book_l2_150_0010/2025/$month_padded/
quit
EOF
)
    
    if [ -z "$days" ]; then
        echo "  No days found"
        continue
    fi
    
    # Check each day for BTC_USDT data
    days_with_data=()
    for day in $days; do
        day_padded=$(printf "%02d" $day)
        
        # Check if BTC_USDT directory exists and has files
        file_count=$(sftp -i "$KEY_FILE" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
            -b - user080@data.crypto.com <<EOF 2>/dev/null | grep -c "\.gz$" || echo "0"
ls exchange/book_l2_150_0010/2025/$month_padded/$day_padded/cdc/$PAIR/
quit
EOF
)
        
        if [ "$file_count" -gt 0 ]; then
            days_with_data+=("$day_padded")
        fi
    done
    
    if [ ${#days_with_data[@]} -gt 0 ]; then
        echo "  ✅ ${#days_with_data[@]} days with data: ${days_with_data[*]}"
    else
        echo "  ❌ No data found"
    fi
    echo ""
done

echo "=== Summary ==="
echo "2025 has all 12 months available"
echo "Use this information to determine upload date ranges"


