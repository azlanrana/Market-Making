//! S3 backtest for QueueFarmer v5 — investor-ready with colo assumptions
//!
//! v5 = v4 strategy + improved fill model for colo simulation:
//!   - touch_queue_pct: 0.2 — when matching the touch, assume only 20% of level ahead (colo priority)
//!   - Spread distribution from actual L2 data
//!   - Assumptions section for investor transparency
//!
//! Run: cargo test -p mm-engine backtest_s3_queue_farmer_v5 --release -- --ignored --nocapture

use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use mm_engine::{BacktestEngine, QueueModelConfig, SimpleFeeModel};
use queue_farmer_v4::QueueFarmerV4;
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
async fn backtest_s3_queue_farmer_v5() {
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

    println!("\n=== QueueFarmer v5 S3 Backtest (colo assumptions) ===");
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

    let strategy = QueueFarmerV4::new(order_amount, tick_size)
        .with_book_imbalance(0.80, 3)
        .with_spread_filter(3.0)
        .with_inventory_stop(0.65);

    let fee_model = SimpleFeeModel::new(dec!(-0.75), dec!(1.5));

    // v5: Colo queue assumption — at touch, only 20% of level ahead (vs 100% default)
    let queue_config = QueueModelConfig {
        price_improving_queue_pct: 0.5,
        touch_queue_pct: 0.2,
        queue_decay_enabled: false,
        queue_depletion_enabled: false,
        queue_churn_enabled: false,
        crossed_book_fill_enabled: true,
        delta_trade_fraction: 0.5,
        min_delta_for_fill: 0.001,
        queue_turnover_rate_per_sec: 0.0,
        cancel_ahead_fraction: 1.0,
        crossed_book_survival_rate: 1.0,
        ..QueueModelConfig::default()
    };

    let mut engine = BacktestEngine::new(strategy, dec!(1000000), dec!(1), fee_model, tick_size)
        .with_queue_config(queue_config);

    println!("Strategy: QueueFarmer v5.0 (v4 logic + colo queue model)");
    println!(
        "Config: $1M AUM, -0.75 bps maker, {} size, touch_queue_pct=0.2",
        order_amount
    );
    println!("Downloading data from S3...\n");

    match engine.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();
            let s = &results.stats;

            let pnl_display = s.total_pnl.round_dp(2);
            let realized_display = s.realized_pnl.round_dp(2);
            let unrealized = (s.total_pnl - s.realized_pnl).round_dp(2);
            let volume_display = s.total_volume.round_dp(0);

            let sim_duration_sec = (results.last_ts - results.first_ts).max(1.0);
            let sim_hours = sim_duration_sec / 3600.0;
            let sim_days = sim_hours / 24.0;
            let round_trips_per_day = if sim_days > 0.0 {
                s.round_trip_count as f64 / sim_days
            } else {
                0.0
            };
            let avg_pnl_per_rt = if s.round_trip_count > 0 {
                (s.realized_pnl / Decimal::from(s.round_trip_count)).round_dp(4)
            } else {
                Decimal::ZERO
            };

            // Spread distribution — 0, 1, 2 ticks; collapse 3+
            let total_spread_snapshots: u64 = results.spread_distribution.values().sum();
            let pct = |c: u64| {
                if total_spread_snapshots > 0 {
                    c as f64 / total_spread_snapshots as f64 * 100.0
                } else {
                    0.0
                }
            };
            let c0 = results.spread_distribution.get(&0).copied().unwrap_or(0);
            let c1 = results.spread_distribution.get(&1).copied().unwrap_or(0);
            let c2 = results.spread_distribution.get(&2).copied().unwrap_or(0);
            let c3plus: u64 = results
                .spread_distribution
                .iter()
                .filter(|(k, _)| **k >= 3)
                .map(|(_, v)| *v)
                .sum();

            println!("=== QueueFarmer v5 S3 Backtest Results ===");
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!(
                "Simulated period: {:.1} hours ({:.2} days)",
                sim_hours, sim_days
            );
            println!("Snapshots processed: {}", results.snapshot_count);

            println!("\n--- Backtest Assumptions ---");
            println!("Queue at touch (colo):     20% of level ahead (touch_queue_pct=0.2)");
            println!("Queue when price-improve:  50% of level ahead");
            println!("Data:                     L2 snapshots only, no trade tape");
            println!("Fees:                     Maker -0.75 bps, Taker 1.5 bps");
            println!("Deployment:               Japan colo (queue priority modeled)");

            println!("\n--- Spread Distribution (from L2 data) ---");
            println!("  <1 tick:  {:>7} snapshots ({:.1}%)", c0, pct(c0));
            println!("  1 tick:   {:>7} snapshots ({:.1}%)", c1, pct(c1));
            println!("  2 ticks: {:>7} snapshots ({:.1}%)", c2, pct(c2));
            if c3plus > 0 {
                println!("  3+ ticks: {:>6} snapshots ({:.1}%)", c3plus, pct(c3plus));
            }

            println!("\n--- Portfolio Performance ---");
            println!("Total PnL:        ${}", pnl_display);
            println!("Realized PnL:     ${}", realized_display);
            println!(
                "Unrealized PnL:   ${} (inventory mark-to-market)",
                unrealized
            );

            println!("\n--- Risk Metrics ---");
            println!("Win rate:         {:.1}%", s.win_rate * 100.0);
            println!("Sharpe ratio:     {:.2}", s.sharpe);
            println!("Max drawdown:     {:.2}%", s.max_drawdown * 100.0);
            println!("Calmar ratio:     {:.2}", s.calmar);

            println!("\n--- Trading Activity ---");
            println!("Fills:            {}", s.fill_count);
            println!("Total volume:     ${}", volume_display);
            println!("Round trips:      {}", s.round_trip_count);
            println!("Round trips/day:  {:.0}", round_trips_per_day);
            println!("Avg PnL/round:    ${}", avg_pnl_per_rt);

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
