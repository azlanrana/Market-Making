# Data Setup & Upload — Quick Steps

## 1. SFTP Config (Crypto.com)

- **Host:** `data.crypto.com` · **User:** `user080` · **Key:** `user080 (1) 2`
- Path: `exchange/book_l2_150_0010/yyyy/mm/dd/cdc/PAIR/` (e.g. `BTC_USDT`, `ETH_USDT`)
- `chmod 600` on key file

## 2. Check Available Dates

```bash
cd rust_mm_system && cargo build --release --bin mm-cli
./target/release/mm-cli check-dates --trading-pair BTC_USDT
# Or: ./scripts/check_available_dates.sh BTC_USDT
```

## 3. S3 Setup

```bash
aws s3 mb s3://backtest-data --region us-east-1
aws configure  # or export AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
```

## 4. Upload to S3

**Option A — Direct SFTP → S3 (recommended)**

```bash
./target/release/mm-cli upload \
  --s3-bucket backtest-data --s3-prefix BTC_USDT/ \
  --start-date 2025-01-01 --end-date 2025-01-31 --trading-pair BTC_USDT
```

**Option B — From local cache**

```bash
./scripts/upload_to_s3.sh backtest-data BTC_USDT 2023-10-25 2023-10-31
```

**Option C — Manual aws sync**

```bash
aws s3 sync ./cache/ s3://backtest-data/BTC_USDT/ --exclude "*" --include "*.gz"
```

## 5. Run Backtest

```bash
export S3_BUCKET=backtest-data S3_PREFIX=BTC_USDT/ TRADING_PAIR=BTC_USDT
cargo test -p mm-engine backtest_s3_rebate_mm --release -- --ignored --nocapture
```

**Optional:** `MAX_FILES=200` (quick test), `MAX_CONCURRENT_DOWNLOADS=64` (faster)

## 6. Verify

```bash
aws s3 ls s3://backtest-data/BTC_USDT/ --recursive | wc -l
```
