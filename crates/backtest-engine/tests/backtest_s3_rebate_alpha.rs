//! S3 backtest for Rebate-Alpha strategy
//!
//! Rebate-Aggressive, Alpha-Conservative strategy tuned for:
//! - $1M initial balance
//! - -0.75 bps maker rebate (rebate farming)
//!
//! Pair-specific config (order_amount) loaded from configs/{pair}.yaml.
//! Set TRADING_PAIR=ETH_USDT to run on ETH (default: BTC_USDT).

use backtest_engine::BacktestRunner;
use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use rebate_alpha::RebateAlphaStrategy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Debug, Deserialize)]
struct PairConfig {
    pair: String,
    order_amount: f64,
    #[allow(dead_code)]
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
async fn backtest_s3_rebate_alpha() {
    let bucket = match std::env::var("S3_BUCKET") {
        Ok(b) => b,
        Err(_) => {
            println!("Skipping: S3_BUCKET not set. Set S3_BUCKET, S3_PREFIX, AWS_REGION to run.");
            return;
        }
    };

    let pair = std::env::var("TRADING_PAIR").unwrap_or_else(|_| "BTC_USDT".to_string());
    let pair_config = match load_pair_config(&pair) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: {}", e);
            return;
        }
    };

    println!("\n=== Running Rebate-Alpha S3 Backtest ===");
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
        .unwrap_or(100);

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

    let order_amount = Decimal::from_f64_retain(pair_config.order_amount).unwrap_or(dec!(0.1));

    // Sniper mode: NO Cross-Chaser — natural spreads only (missed trade > $72k loss)
    let strategy = RebateAlphaStrategy::new_mid_hft(
        order_amount,
        0.75, // Maker rebate: 0.75 bps (pass positive; strategy uses for scratch pricing)
    );

    // Backtest runner: maker_fee_bps negative = rebate (we receive)
    // initial_price is unused (prices come from data) - pass placeholder
    let tick_size = Decimal::from_f64_retain(pair_config.tick_size).unwrap_or(dec!(0.01));
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000), // $1M initial capital
        dec!(1),       // Unused - prices from snapshot data
        dec!(-0.75),   // Maker rebate: -0.75 bps (we receive)
        false,         // No latency simulation
        tick_size,
    );

    println!(
        "Strategy: Liquidity Sniper (15/40/80 bps, 30s cooldown, 2s refresh, no Cross-Chaser)"
    );
    println!(
        "Config: $1M AUM, -0.75 bps rebate farming, {} order size",
        order_amount
    );
    println!("Initial capital: $1,000,000");
    println!("Maker rebate: -0.75 bps (rebate)\n");
    println!("Downloading data from S3...\n");

    match runner.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();

            println!("=== Rebate-Alpha S3 Backtest Results ===");
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!("\n--- Portfolio Performance ---");
            println!("Initial capital: $1,000,000");
            println!(
                "Portfolio value (first snapshot): ${}",
                results.stats.initial_value
            );
            println!("Final portfolio value: ${}", results.stats.final_value);
            println!("Total return: ${:+}", results.stats.total_return);
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
            println!("Strategy P&L (realized): ${:+}", results.stats.realized_pnl);
            println!(
                "Unrealized P&L (mark-to-market): ${:+}",
                results.stats.unrealized_pnl
            );
            println!(
                "Total P&L (portfolio value change): ${:+}",
                results.stats.realized_pnl + results.stats.unrealized_pnl
            );

            println!("\n--- Risk Metrics ---");
            println!("Max drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            println!(
                "Max drawdown duration: {:.0}s",
                results.stats.max_drawdown_duration
            );
            println!(
                "Peak portfolio value: ${}",
                results.stats.max_portfolio_value
            );
            println!(
                "Trough portfolio value: ${}",
                results.stats.min_portfolio_value
            );
            println!("Sharpe ratio: {:.2}", results.stats.sharpe_ratio);
            println!("Sortino ratio: {:.2}", results.stats.sortino_ratio);
            println!("Calmar ratio: {:.2}", results.stats.calmar_ratio);
            println!(
                "Annualized volatility: {:.4}%",
                results.stats.volatility * 100.0
            );

            println!("\n--- Inventory ---");
            println!(
                "Final inventory: {:.2}%",
                results.stats.final_inventory_pct * 100.0
            );
            println!(
                "Avg inventory: {:.2}%",
                results.stats.avg_inventory_pct * 100.0
            );
            println!(
                "Inventory range: {:.2}% .. {:.2}%",
                results.stats.min_inventory_reached * 100.0,
                results.stats.max_inventory_reached * 100.0
            );

            println!("\n--- Trading Activity ---");
            println!("Total fills: {}", results.simulator_stats.total_fills);
            println!(
                "Partial fills: {}",
                results.simulator_stats.total_partial_fills
            );
            println!("Total volume: ${}", results.stats.total_volume);
            // total_fees < 0 means we received rebates; display as positive
            let rebates = -results.stats.total_fees;
            println!("Rebates received: ${}", rebates);
            println!(
                "Buy volume: ${} ({} fills)",
                results.stats.buy_volume, results.stats.buy_fills
            );
            println!(
                "Sell volume: ${} ({} fills)",
                results.stats.sell_volume, results.stats.sell_fills
            );

            println!("\n--- Trade Quality ---");
            println!("Total round-trips: {}", results.stats.total_trades);
            println!("Win rate: {:.1}%", results.stats.win_rate * 100.0);
            println!("Profit factor: {:.2}", results.stats.profit_factor);
            println!("Avg win: ${:.2}", results.stats.avg_win);
            println!("Avg loss: ${:.2}", results.stats.avg_loss);
            println!("Largest win: ${:.2}", results.stats.largest_win);
            println!("Largest loss: ${:.2}", results.stats.largest_loss);

            println!("\n--- Layer Performance ---");
            let mut layers: Vec<u32> = results.stats.pnl_by_layer.keys().copied().collect();
            layers.sort();
            for layer in layers {
                let pnl = results
                    .stats
                    .pnl_by_layer
                    .get(&layer)
                    .unwrap_or(&rust_decimal::Decimal::ZERO);
                let volume = results
                    .stats
                    .volume_by_layer
                    .get(&layer)
                    .unwrap_or(&rust_decimal::Decimal::ZERO);
                let fills = results.stats.fills_by_layer.get(&layer).unwrap_or(&0);
                println!(
                    "  Layer {}: P&L=${:+}, Volume=${}, Fills={}",
                    layer, pnl, volume, fills
                );
            }

            if !results.stats.realized_pnl_by_day.is_empty() {
                println!("\n--- Strategy P&L by Day (realized only — market-making edge) ---");
                let mut days: Vec<_> = results.stats.realized_pnl_by_day.keys().collect();
                days.sort();
                for day in days.iter().take(10) {
                    let pnl = results
                        .stats
                        .realized_pnl_by_day
                        .get(*day)
                        .unwrap_or(&Decimal::ZERO);
                    println!("  {}: ${:+}", day, pnl);
                }
                if days.len() > 10 {
                    println!("  ... ({} days total)", days.len());
                }
            }

            if !results.stats.pnl_by_day.is_empty() {
                println!("\n--- Portfolio P&L by Day (includes mark-to-market on inventory) ---");
                let mut days: Vec<_> = results.stats.pnl_by_day.keys().collect();
                days.sort();
                for day in days.iter().take(7) {
                    let pnl = results.stats.pnl_by_day.get(*day).unwrap_or(&Decimal::ZERO);
                    println!("  {}: ${:+}", day, pnl);
                }
                if days.len() > 7 {
                    println!("  ... ({} days total)", days.len());
                }
            }

            if !results.stats.pnl_by_day_by_layer.is_empty() {
                println!("\n--- P&L by Day by Layer ---");
                let mut days: Vec<_> = results.stats.pnl_by_day_by_layer.keys().collect();
                days.sort();
                for day in days.iter() {
                    if let Some(layers) = results.stats.pnl_by_day_by_layer.get(*day) {
                        let mut layer_vec: Vec<_> = layers.iter().collect();
                        layer_vec.sort_by_key(|(k, _)| *k);
                        let parts: Vec<String> = layer_vec
                            .iter()
                            .map(|(l, p)| format!("L{}=${:+}", l, p))
                            .collect();
                        let vol = results.stats.volume_by_day_by_layer.get(*day);
                        let fills = results.stats.fills_by_day_by_layer.get(*day);
                        println!("  {}: {}", day, parts.join(", "));
                        if let (Some(v), Some(f)) = (vol, fills) {
                            let mut v_vec: Vec<_> = v.iter().collect();
                            v_vec.sort_by_key(|(k, _)| *k);
                            let mut f_vec: Vec<_> = f.iter().collect();
                            f_vec.sort_by_key(|(k, _)| *k);
                            let v_parts: Vec<String> = v_vec
                                .iter()
                                .map(|(l, amt)| format!("L{}:${}", l, amt))
                                .collect();
                            let f_parts: Vec<String> =
                                f_vec.iter().map(|(l, n)| format!("L{}:{}", l, n)).collect();
                            println!(
                                "       vol: {} | fills: {}",
                                v_parts.join(" "),
                                f_parts.join(" ")
                            );
                        }
                    }
                }
            }

            if results.stats.return_pct > 0.0 {
                println!(
                    "\n✅ Positive return: {:.4}%",
                    results.stats.return_pct * 100.0
                );
            } else {
                println!(
                    "\n⚠️  Negative return: {:.4}%",
                    results.stats.return_pct * 100.0
                );
            }
        }
        Err(e) => {
            eprintln!("❌ Backtest failed: {}", e);
            eprintln!("Error details: {:?}", e);
        }
    }
}
