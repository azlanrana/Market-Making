// S3 backtest - supports multiple trading pairs with different bucket/prefix strategies

use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use rust_decimal_macros::dec;
use std::time::Instant;

/// Example: Backtest BTC_USDT from dedicated bucket
#[tokio::test]
#[ignore]
async fn backtest_s3_btcusdt() {
    run_backtest_for_pair("BTC_USDT", "backtest-btcusdt-2025", "2023/10/25/cdc/BTC_USDT/").await;
}

/// Example: Backtest ETH_USDT from dedicated bucket
#[tokio::test]
#[ignore]
async fn backtest_s3_ethusdt() {
    run_backtest_for_pair("ETH_USDT", "backtest-ethusdt-2025", "2023/10/25/cdc/ETH_USDT/").await;
}

/// Example: Backtest from single bucket with prefixes
#[tokio::test]
#[ignore]
async fn backtest_s3_single_bucket() {
    // Single bucket, different prefixes per pair
    run_backtest_for_pair("BTC_USDT", "backtest-data", "BTC_USDT/2023/10/25/cdc/BTC_USDT/").await;
}

async fn run_backtest_for_pair(pair: &str, bucket: &str, prefix: &str) {
    println!("\n=== Running 7-Day S3 Backtest for {} ===", pair);
    let start_time = Instant::now();
    
    let region = std::env::var("AWS_REGION")
        .unwrap_or_else(|_| "us-east-1".to_string());
    
    let max_files = std::env::var("MAX_FILES")
        .ok()
        .and_then(|s| s.parse().ok());
    let key_date_range = parse_s3_inclusive_date_range_from_env()
        .expect("S3_START_DATE / S3_END_DATE: set both as YYYY-MM-DD or neither");
    
    let max_concurrent = std::env::var("MAX_CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    
    println!("S3 Configuration:");
    println!("  Pair: {}", pair);
    println!("  Bucket: {}", bucket);
    println!("  Prefix: {}", prefix);
    println!("  Region: {}", region);
    println!("  Max files: {:?}", max_files);
    println!();
    
    let loader = S3Loader::new(
        bucket.to_string(),
        prefix.to_string(),
        region.clone(),
        max_concurrent,
    )
    .await
    .expect("Failed to create S3 loader")
    .with_max_files(max_files)
    .with_s3_key_date_range(key_date_range);
    
    let strategy = BalancedMMStrategy::new_with_config(
        dec!(0.1),
        5.0,
        0.5,
    )
    .with_target_inventory_pct(0.15)
    .with_inventory_limits(0.35, 0.50);
    
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000),
        dec!(92797),
        dec!(0.25),
        false,
        dec!(0.01), // Tick size
    );
    
    println!("Downloading data from S3...\n");
    
    match runner.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();
            
            println!("=== {} Backtest Results ===", pair);
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!("Total return: {:.4}%", results.stats.return_pct * 100.0);
            println!("Total volume: ${}", results.stats.total_volume);
            println!("Total fills: {}", results.simulator_stats.total_fills);
        }
        Err(e) => {
            eprintln!("❌ Backtest failed for {}: {}", pair, e);
        }
    }
}

