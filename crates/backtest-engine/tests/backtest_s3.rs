// S3 backtest - downloads orderbook data from AWS S3
//
// Balanced MM strategy. Pair-specific config (order_amount, tick_size) loaded from configs/{pair}.yaml.
// Set TRADING_PAIR=ETH_USDT to run on ETH (default: BTC_USDT).

use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Deserialize)]
struct PairConfig {
    #[allow(dead_code)]
    pair: String,
    order_amount: f64,
    #[allow(dead_code)]
    tick_size: f64,
    /// Disable ping-pong scratch orders (default: true). Set false for ETH - scratch orders lose.
    #[serde(default = "default_pingpong")]
    pingpong_enabled: bool,
}

fn default_pingpong() -> bool {
    true
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
async fn backtest_s3_7days() {
    let pair = std::env::var("TRADING_PAIR").unwrap_or_else(|_| "BTC_USDT".to_string());
    let pair_config = match load_pair_config(&pair) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: {}", e);
            return;
        }
    };

    println!("\n=== Running Balanced MM S3 Backtest ===");
    println!("Pair config: {:?}", pair_config);
    let start_time = Instant::now();

    // S3 Configuration
    // Option 1: Multiple buckets (one per pair)
    //   S3_BUCKET=backtest-btcusdt-2025
    //   S3_PREFIX=2023/10/25/cdc/BTC_USDT/
    //
    // Option 2: Single bucket with prefixes (recommended)
    //   S3_BUCKET=backtest-data
    //   S3_PREFIX=BTC_USDT/2023/10/25/cdc/BTC_USDT/
    let bucket = std::env::var("S3_BUCKET")
        .expect("S3_BUCKET environment variable must be set");
    
    // Prefix format examples:
    // - Single bucket: "BTC_USDT/2023/10/25/cdc/BTC_USDT/"
    // - Multiple buckets: "2023/10/25/cdc/BTC_USDT/"
    // - Or just: "BTC_USDT/" to get all dates for that pair
    let prefix = std::env::var("S3_PREFIX")
        .unwrap_or_else(|_| format!("{}/", pair));
    
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
        .unwrap_or(100); // Default: 100 parallel downloads
    
    println!("S3 Configuration:");
    println!("  Bucket: {}", bucket);
    println!("  Prefix: {}", prefix);
    println!("  Region: {}", region);
    println!("  Max files: {:?}", max_files);
    println!("  Max concurrent downloads: {}", max_concurrent);
    println!();
    
    // Create S3 loader
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
    
    let order_amount = Decimal::from_f64_retain(pair_config.order_amount).unwrap_or(dec!(0.1));
    let tick_size = Decimal::from_f64_retain(pair_config.tick_size).unwrap_or(dec!(0.01));

    // Forensic surgery: 0.1s refresh (kill latency), 1.0s fill delay (let market breathe)
    // Widened spreads [10, 20, 35] bps for ETH volatility, skew (80 bps, 15 sensitivity) for 50% target
    let strategy = BalancedMMStrategy::new_with_config(
        order_amount,
        0.1,        // Refresh every 0.1s - move with micro-price
        1.0,        // Fill delay 1.0s - don't jump back immediately
    )
    .with_base_spreads_bps(&[10.0, 20.0, 35.0])  // Harvest spread for volatile pairs
    .with_target_inventory_pct(0.5)   // Target 50% neutral (balanced)
    .with_inventory_limits(0.35, 0.50)
    .with_skew_config(80.0, 15.0)     // Aggressive skew: 80 bps max, 15x sensitivity
    .with_pingpong(pair_config.pingpong_enabled, 0.5, 0.25);
    
    // Create backtest runner with Crypto.com fees
    // initial_price is unused (prices come from data) - pass placeholder
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000), // $1M initial capital
        dec!(1),       // Placeholder - prices from snapshot data
        dec!(0.25),   // Maker rebate: 0.25 bps = 0.0025%
        false,        // No latency simulation
        tick_size,
    );
    
    println!("Strategy: Multi-layer MM (10/20/35 bps), {} orders, ping-pong={}", order_amount, pair_config.pingpong_enabled);
    println!("Config: Refresh=0.1s, FillDelay=1.0s, Skew 80bps/15x, Target 50%");
    println!("Initial capital: $1,000,000");
    println!("Maker rebate: -0.0025%\n");
    println!("Downloading data from S3...\n");
    
    match runner.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();
            
            println!("=== 7-Day S3 Backtest Results ===");
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!("\n--- Portfolio Performance ---");
            println!("Initial portfolio value: ${}", results.stats.initial_value);
            println!("Final portfolio value: ${}", results.stats.final_value);
            println!("Total return: ${}", results.stats.total_return);
            println!("Return %: {:.4}%", results.stats.return_pct * 100.0);
            
            if let Some(n) = max_files {
                let hours = (n as f64 / 85.0).max(1e-6);
                let annualized_return = results.stats.return_pct * (365.0 * 24.0 / hours) * 100.0;
                println!(
                    "Annualized return (scaled from ~{:.1}h via MAX_FILES heuristic): {:.2}%",
                    hours, annualized_return
                );
            }
            
            println!("\n--- P&L Breakdown ---");
            println!("Strategy P&L (realized): ${}", results.stats.realized_pnl);
            println!("Unrealized P&L (mark-to-market): ${}", results.stats.unrealized_pnl);
            println!("Total P&L (portfolio value change): ${}", results.stats.realized_pnl + results.stats.unrealized_pnl);
            
            println!("\n--- Risk Metrics ---");
            println!("Max drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            println!("Max portfolio value: ${}", results.stats.max_portfolio_value);
            println!("Min portfolio value: ${}", results.stats.min_portfolio_value);
            
            println!("\n--- Inventory Management ---");
            println!("Final inventory: {:.2}%", results.stats.final_inventory_pct * 100.0);
            println!("Average inventory: {:.2}%", results.stats.avg_inventory_pct * 100.0);
            
            println!("\n--- Trading Activity ---");
            println!("Total fills: {}", results.simulator_stats.total_fills);
            println!("Filled orders: {}", results.simulator_stats.filled_orders);
            println!("Partially filled orders: {}", results.simulator_stats.partially_filled_orders);
            println!("Total volume: ${}", results.stats.total_volume);
            println!("Total fees (rebates): ${}", results.stats.total_fees);
            
            // Calculate key metrics
            let avg_fill_size = if results.simulator_stats.total_fills > 0 {
                results.stats.total_volume / rust_decimal::Decimal::from(results.simulator_stats.total_fills)
            } else {
                rust_decimal::Decimal::ZERO
            };
            println!("Average fill size: ${}", avg_fill_size);
            
            let fill_rate = if results.simulator_stats.filled_orders > 0 {
                (results.simulator_stats.total_fills as f64 / results.simulator_stats.filled_orders as f64) * 100.0
            } else {
                0.0
            };
            println!("Fill rate: {:.2}%", fill_rate);
            
            println!("\n--- Directional Analysis ---");
            println!("Buy volume: ${}", results.stats.buy_volume);
            println!("Sell volume: ${}", results.stats.sell_volume);
            println!("Buy fills: {}", results.stats.buy_fills);
            println!("Sell fills: {}", results.stats.sell_fills);
            println!("Buy P&L: ${}", results.stats.buy_pnl);
            println!("Sell P&L: ${}", results.stats.sell_pnl);
            println!("Net position: {}", results.stats.net_position_over_time);
            
            println!("\n--- Trade Quality ---");
            println!("Total trades: {}", results.stats.total_trades);
            println!("Win rate: {:.2}%", results.stats.win_rate * 100.0);
            println!("Profit factor: {:.2}", results.stats.profit_factor);
            println!("Average win: ${:.2}", results.stats.avg_win);
            println!("Average loss: ${:.2}", results.stats.avg_loss);
            println!("Largest win: ${:.2}", results.stats.largest_win);
            println!("Largest loss: ${:.2}", results.stats.largest_loss);
            
            println!("\n--- Layer Performance ---");
            let mut layers: Vec<u32> = results.stats.pnl_by_layer.keys().copied().collect();
            layers.sort();
            for layer in layers {
                let pnl = results.stats.pnl_by_layer.get(&layer).unwrap_or(&rust_decimal::Decimal::ZERO);
                let volume = results.stats.volume_by_layer.get(&layer).unwrap_or(&rust_decimal::Decimal::ZERO);
                let fills = results.stats.fills_by_layer.get(&layer).unwrap_or(&0);
                println!("  Layer {} bps: P&L=${}, Volume=${}, Fills={}", layer, pnl, volume, fills);
            }
            
            println!("\n--- Inventory Extremes ---");
            println!("Max inventory reached: {:.2}%", results.stats.max_inventory_reached * 100.0);
            println!("Min inventory reached: {:.2}%", results.stats.min_inventory_reached * 100.0);

            println!("\n--- Forensic: Fill Price vs Mid (Decision DNA) ---");
            if !results.stats.fill_gap_bps_by_hour.is_empty() {
                let mut hours: Vec<u32> = results.stats.fill_gap_bps_by_hour.keys().copied().collect();
                hours.sort();
                for h in hours {
                    let (sum_bps, count) = results.stats.fill_gap_bps_by_hour.get(&h).unwrap_or(&(0.0, 0));
                    let avg_bps = if *count > 0 { sum_bps / *count as f64 } else { 0.0 };
                    let pnl = results.stats.pnl_by_hour.get(&h).map(|d| d.to_f64().unwrap_or(0.0)).unwrap_or(0.0);
                    println!("  Hour {}: avg fill gap {:+.1} bps ({} fills), P&L ${:+.0}", h, avg_bps, count, pnl);
                }
            } else {
                println!("  (no fill data)");
            }

            println!("\n--- Temporal Analysis ---");
            println!("Max drawdown duration: {:.2} hours", results.stats.max_drawdown_duration / 3600.0);
            
            // Show top 5 best/worst hours
            let mut hour_pnl: Vec<(u32, f64)> = results.stats.pnl_by_hour.iter()
                .map(|(h, pnl)| (*h, pnl.to_f64().unwrap_or(0.0)))
                .collect();
            hour_pnl.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            
            if !hour_pnl.is_empty() {
                println!("Top 5 best hours:");
                for (hour, pnl) in hour_pnl.iter().take(5) {
                    println!("  Hour {}: ${:.2}", hour, pnl);
                }
                println!("Top 5 worst hours:");
                for (hour, pnl) in hour_pnl.iter().rev().take(5) {
                    println!("  Hour {}: ${:.2}", hour, pnl);
                }
            }
            
            // Strategy P&L by day (realized only — market-making edge)
            if !results.stats.realized_pnl_by_day.is_empty() {
                println!("\n--- Strategy P&L by Day (realized only) ---");
                let mut days: Vec<_> = results.stats.realized_pnl_by_day.keys().collect();
                days.sort();
                for day in days.iter().take(10) {
                    let pnl = results.stats.realized_pnl_by_day.get(*day).unwrap_or(&rust_decimal::Decimal::ZERO);
                    println!("  {}: ${:+}", day, pnl);
                }
            }

            // Top 5 best/worst days (portfolio value — includes mark-to-market)
            let mut day_pnl: Vec<(String, f64)> = results.stats.pnl_by_day.iter()
                .map(|(d, pnl)| (d.clone(), pnl.to_f64().unwrap_or(0.0)))
                .collect();
            day_pnl.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

            if !day_pnl.is_empty() {
                println!("\n--- Portfolio P&L by Day (includes mark-to-market) — Top/Worst ---");
                println!("Top 5 best days:");
                for (day, pnl) in day_pnl.iter().take(5) {
                    println!("  {}: ${:.2}", day, pnl);
                }
                println!("Top 5 worst days:");
                for (day, pnl) in day_pnl.iter().rev().take(5) {
                    println!("  {}: ${:.2}", day, pnl);
                }
            }
            
            println!("\n--- Risk-Adjusted Metrics ---");
            println!("Sharpe ratio: {:.2}", results.stats.sharpe_ratio);
            println!("Sortino ratio: {:.2}", results.stats.sortino_ratio);
            println!("Calmar ratio: {:.2}", results.stats.calmar_ratio);
            println!("Volatility (annualized): {:.2}%", results.stats.volatility * 100.0);
            
            println!("\n--- Performance Summary ---");
            if results.stats.return_pct > 0.0 {
                println!("✅ Positive return: {:.4}%", results.stats.return_pct * 100.0);
            } else {
                println!("⚠️  Negative return: {:.4}%", results.stats.return_pct * 100.0);
            }
            
            if results.stats.final_inventory_pct > 0.4 && results.stats.final_inventory_pct < 0.6 {
                println!("✅ Inventory well balanced: {:.2}%", results.stats.final_inventory_pct * 100.0);
            } else {
                println!("⚠️  Inventory imbalance: {:.2}%", results.stats.final_inventory_pct * 100.0);
            }
            
            if results.stats.max_drawdown < 0.01 {
                println!("✅ Low drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            } else {
                println!("⚠️  Higher drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            }
        }
        Err(e) => {
            eprintln!("❌ Backtest failed: {}", e);
            eprintln!("Error details: {:?}", e);
            eprintln!("\nMake sure you have:");
            eprintln!("1. Set S3_BUCKET environment variable");
            eprintln!("2. Set S3_PREFIX environment variable (optional, default: backtest-data/2023/10/25/cdc/BTC_USDT/)");
            eprintln!("3. Set AWS_REGION environment variable (optional, default: us-east-1)");
            eprintln!("4. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables");
            eprintln!("   OR configure AWS credentials file (~/.aws/credentials)");
            eprintln!("5. Set MAX_FILES to cap object count after filters (optional; omit for no cap)");
            eprintln!("6. Set MAX_CONCURRENT_DOWNLOADS (default: 100)");
        }
    }
}

