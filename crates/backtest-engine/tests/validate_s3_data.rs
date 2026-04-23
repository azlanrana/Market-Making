// Data validation test - verifies S3 orderbook data integrity
// Run with: S3_BUCKET=btcusdt2025 S3_PREFIX=BTC_USDT/2025/11/ AWS_REGION=us-east-1 cargo test --test validate_s3_data validate_s3_data -- --nocapture --ignored

use data_loader::{parse_s3_inclusive_date_range_from_env, DataLoader, S3Loader};

#[tokio::test]
#[ignore] // Requires AWS credentials and S3 access
async fn validate_s3_data() {
    let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET required");
    let prefix = std::env::var("S3_PREFIX").unwrap_or_else(|_| "BTC_USDT/2025/11/".to_string());
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let max_files_cap = std::env::var("MAX_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let key_date_range = parse_s3_inclusive_date_range_from_env()
        .expect("S3_START_DATE / S3_END_DATE: set both as YYYY-MM-DD or neither");

    println!("\n=== S3 Data Validation ===\n");
    println!(
        "Bucket: {}, Prefix: {}, Max files: {}",
        bucket, prefix, max_files_cap
    );

    let loader = S3Loader::new(bucket, prefix, region, 1)
        .await
        .expect("Failed to create S3 loader")
        .with_max_files(Some(max_files_cap))
        .with_s3_key_date_range(key_date_range);

    let snapshots: Vec<_> = loader
        .load_snapshots()
        .expect("Failed to load")
        .collect::<Result<Vec<_>, _>>()
        .expect("Parse errors");

    println!("\nLoaded {} snapshots\n", snapshots.len());

    if snapshots.is_empty() {
        println!("No snapshots - cannot validate");
        return;
    }

    // 1. Timestamp ordering
    let mut timestamp_errors = 0;
    let mut last_ts = 0.0;
    for (i, s) in snapshots.iter().enumerate() {
        if s.timestamp < last_ts {
            timestamp_errors += 1;
            if timestamp_errors <= 3 {
                println!(
                    "⚠️  Timestamp out of order at snapshot {}: {} < {} (prev)",
                    i, s.timestamp, last_ts
                );
            }
        }
        last_ts = s.timestamp;
    }
    if timestamp_errors == 0 {
        println!("✅ Timestamp ordering: OK (monotonic)");
    } else {
        println!("❌ Timestamp ordering: {} violations", timestamp_errors);
    }

    // 2. Book sanity (best_bid < best_ask, no crossed book)
    let mut crossed_errors = 0;
    let mut spread_errors = 0;
    for (i, s) in snapshots.iter().enumerate() {
        if s.best_bid >= s.best_ask {
            crossed_errors += 1;
            if crossed_errors <= 3 {
                println!(
                    "⚠️  Crossed book at snapshot {}: best_bid={} >= best_ask={}",
                    i, s.best_bid, s.best_ask
                );
            }
        }
        if s.bids.first().map(|(p, _)| *p) != Some(s.best_bid)
            || s.asks.first().map(|(p, _)| *p) != Some(s.best_ask)
        {
            spread_errors += 1;
            if spread_errors <= 3 {
                let bb = s.bids.first().map(|(p, _)| *p);
                let ba = s.asks.first().map(|(p, _)| *p);
                println!("⚠️  best_bid/ask mismatch at {}: best_bid={} first_bid={:?} best_ask={} first_ask={:?}", i, s.best_bid, bb, s.best_ask, ba);
            }
        }
    }
    if crossed_errors == 0 {
        println!("✅ Book sanity (no crossed): OK");
    } else {
        println!("❌ Crossed book: {} snapshots", crossed_errors);
    }
    if spread_errors == 0 {
        println!("✅ best_bid/ask matches first level: OK");
    } else {
        println!("❌ best_bid/ask mismatch: {} snapshots", spread_errors);
    }

    // 3. Sample first snapshot
    let first = &snapshots[0];
    println!("\n--- First snapshot ---");
    println!("  timestamp: {}", first.timestamp);
    println!("  mid_price: {}", first.mid_price);
    println!(
        "  best_bid: {} best_ask: {}",
        first.best_bid, first.best_ask
    );
    println!("  spread_bps: {:.2}", first.spread_bps);
    println!(
        "  bids count: {} asks count: {}",
        first.bids.len(),
        first.asks.len()
    );
    if !first.bids.is_empty() {
        println!(
            "  first 3 bids: {:?}",
            &first.bids[..first.bids.len().min(3)]
        );
    }
    if !first.asks.is_empty() {
        println!(
            "  first 3 asks: {:?}",
            &first.asks[..first.asks.len().min(3)]
        );
    }

    // 4. Time range
    let first_ts = snapshots.first().unwrap().timestamp;
    let last_ts = snapshots.last().unwrap().timestamp;
    let hours = (last_ts - first_ts) / 3600.0;
    println!("\n--- Time range ---");
    println!(
        "  First: {} Last: {} Span: {:.2} hours",
        first_ts, last_ts, hours
    );

    let all_ok = timestamp_errors == 0 && crossed_errors == 0 && spread_errors == 0;
    println!(
        "\n=== Result: {} ===",
        if all_ok {
            "✅ All checks passed"
        } else {
            "❌ Issues found"
        }
    );
}
