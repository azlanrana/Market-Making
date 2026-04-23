//! S3 backtest for QueueFarmer strategy — NEW ENGINE
//!
//! Uses BacktestEngine (mm-engine) instead of BacktestRunner (backtest-engine).
//! QueueFarmer v1.0: tight two-sided quotes, maker rebates, hard inventory stop.
//!
//! Set S3_BUCKET, S3_PREFIX, AWS_REGION to run. Set TRADING_PAIR for pair config.

use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use mm_engine::{BacktestEngine, SimpleFeeModel};
use queue_farmer::QueueFarmerStrategy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Deserialize)]
struct PairConfig {
    #[allow(dead_code)]
    pair: String,
    order_amount: f64,
    tick_size: f64,
}

fn load_pair_config(pair: &str) -> Result<PairConfig, String> {
    let config_name = format!("{}.yaml", pair.to_lowercase().replace('_', "_"));
    let config_paths = [
        PathBuf::from("configs").join(&config_name),
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../configs")
            .join(&config_name),
    ];
    for path in &config_paths {
        if path.exists() {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| format!("Failed to read {:?}: {}", path, e))?;
            return serde_yaml::from_str(&contents)
                .map_err(|e| format!("Failed to parse {:?}: {}", path, e));
        }
    }
    Err(format!(
        "Config not found for {}. Tried: {:?}. Create configs/{}",
        pair, config_paths, config_name
    ))
}

#[tokio::test]
#[ignore] // Ignore by default - requires AWS credentials and S3 bucket
async fn backtest_s3_queue_farmer() {
    let bucket = match std::env::var("S3_BUCKET") {
        Ok(b) => b,
        Err(_) => {
            println!("Skipping: S3_BUCKET not set. Set S3_BUCKET, S3_PREFIX, AWS_REGION to run.");
            return;
        }
    };

    let pair = std::env::var("TRADING_PAIR").unwrap_or_else(|_| "ETH_USDT".to_string());
    let pair_config = match load_pair_config(&pair) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: {}", e);
            return;
        }
    };

    println!("\n=== QueueFarmer S3 Backtest (NEW ENGINE) ===");
    println!("Pair config: {:?}", pair_config);
    let start_time = Instant::now();

    let prefix = std::env::var("S3_PREFIX").unwrap_or_else(|_| format!("{}/", pair));
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let max_files = std::env::var("MAX_FILES").ok().and_then(|s| s.parse().ok());
    let key_date_range = parse_s3_inclusive_date_range_from_env()
        .expect("S3_START_DATE / S3_END_DATE: set both as YYYY-MM-DD or neither");
    let max_concurrent = std::env::var("MAX_CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    println!("S3 Configuration:");
    println!("  Bucket: {}", bucket);
    println!("  Prefix: {}", prefix);
    println!("  Region: {}", region);
    println!("  Max files: {:?}", max_files);
    println!();

    let loader = S3Loader::new(
        bucket.clone(),
        prefix.clone(),
        region.clone(),
        max_concurrent,
    )
    .await
    .expect("Failed to create S3 loader")
    .with_max_files(max_files)
    .with_s3_key_date_range(key_date_range);

    let order_amount = Decimal::from_f64_retain(pair_config.order_amount).unwrap_or(dec!(1.0));
    let tick_size = Decimal::from_f64_retain(pair_config.tick_size).unwrap_or(dec!(0.01));

    let strategy = QueueFarmerStrategy::new(order_amount, 0.75)
        .with_backtest_mode()
        .with_daily_loss_limit(dec!(2000));

    // Maker rebate -0.75 bps (we receive), taker fee 1.5 bps
    let fee_model = SimpleFeeModel::new(dec!(-0.75), dec!(1.5));

    let mut engine = BacktestEngine::new(strategy, dec!(1000000), dec!(1), fee_model, tick_size);

    println!("Strategy: QueueFarmer v1.0 (new mm-engine)");
    println!(
        "Config: $1M AUM, -0.75 bps maker rebate, {} ETH order size",
        order_amount
    );
    println!("Downloading data from S3...\n");

    match engine.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();
            let s = &results.stats;

            let pnl_display = s.total_pnl.round_dp(2);
            let realized_display = s.realized_pnl.round_dp(2);

            println!("=== QueueFarmer S3 Backtest Results (NEW ENGINE) ===");
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!("\n--- Portfolio Performance ---");
            println!("Total PnL: ${}", pnl_display);
            println!("Realized PnL: ${}", realized_display);
            println!("\n--- Risk Metrics ---");
            println!("Win rate: {:.1}%", s.win_rate * 100.0);
            println!("Sharpe ratio: {:.2}", s.sharpe);
            println!("Max drawdown: {:.2}%", s.max_drawdown * 100.0);
            println!("Calmar ratio: {:.2}", s.calmar);
            println!("\n--- Trading Activity ---");
            println!("Round trips: {}", s.round_trip_count);

            if s.total_pnl > Decimal::ZERO {
                println!("\n✅ Positive return: ${}", pnl_display);
            } else {
                println!("\n⚠️  Negative return: ${}", pnl_display);
            }
        }
        Err(e) => {
            eprintln!("❌ Backtest failed: {}", e);
            eprintln!("Error details: {:?}", e);
        }
    }
}
