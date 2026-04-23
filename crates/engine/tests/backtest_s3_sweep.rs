//! Parameter sweep for RebateMM — test many configs, output CSV.
//!
//! Run: SWEEP_CONFIG=configs/sweep_rebate_mm.yaml S3_BUCKET=ethusdt2025 \
//!      S3_PREFIX=ETH_USDT/ AWS_REGION=us-east-1 \
//!      cargo test -p mm-engine backtest_s3_sweep --release -- --ignored --nocapture
//!
//! Or per-asset defaults: `REBATE_MM_PROFILE=eth` / `btc` → `configs/rebate_mm_{profile}.yaml`.
//! Harness defaults + capital: `configs/backtest_engine_harness.yaml` (`BACKTEST_ENGINE_HARNESS` to override path).
//!
//! Config supports:
//! - `base`: default params for all runs
//! - `grid`: param -> [values]; cartesian product of all params
//! - `experiments`: explicit list of {name, ...overrides}
//!
//! Results written to SWEEP_OUTPUT_CSV (default: sweep_results.csv)

mod common;

use common::rebate_mm_sweep_builder::{
    apply_crossed_book_survival_env, build_strategy, engine_capital_from_harness,
    experiments_from_sweep, get_bool, get_f64, merge_profile_with_engine_defaults,
    order_amount_from_config, queue_model_from_config, read_backtest_engine_harness,
    read_sweep_yaml, simple_fee_model_from_config, tick_size_from_config,
};
use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use mm_engine::BacktestEngine;
use std::path::PathBuf;
use std::time::Instant;

#[tokio::test]
#[ignore]
async fn backtest_s3_sweep() {
    let (sweep, used_path) = match read_sweep_yaml() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    };
    eprintln!("Using config: {}", used_path.display());

    let (engine_harness, harness_path) = read_backtest_engine_harness();
    if let Some(ref hp) = harness_path {
        eprintln!("Engine harness: {}", hp.display());
    }

    let bucket = std::env::var("S3_BUCKET").unwrap_or_else(|_| {
        eprintln!("S3_BUCKET required");
        std::process::exit(1);
    });
    let pair = std::env::var("TRADING_PAIR").unwrap_or_else(|_| "ETH_USDT".to_string());
    let prefix = std::env::var("S3_PREFIX").unwrap_or_else(|_| format!("{}/", pair));
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let max_files = std::env::var("MAX_FILES").ok().and_then(|s| s.parse().ok());
    let key_date_range = match parse_s3_inclusive_date_range_from_env() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{}", e);
            std::process::exit(1);
        }
    };
    let max_concurrent = std::env::var("MAX_CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);

    let experiments = experiments_from_sweep(&sweep);
    if experiments.is_empty() {
        eprintln!("sweep config must have 'grid' or 'experiments'");
        std::process::exit(1);
    }

    println!("\n=== RebateMM Parameter Sweep ===");
    println!("Config: {}", used_path.display());
    println!("Experiments: {}", experiments.len());
    println!(
        "Dataset: max_files={:?}, key_date_range={:?}",
        max_files, key_date_range
    );

    println!(
        "Harness: backtest_engine_harness.yaml defaults + profile row; CROSSED_BOOK_SURVIVAL_RATE overrides survival when set"
    );

    let output_csv =
        std::env::var("SWEEP_OUTPUT_CSV").unwrap_or_else(|_| "sweep_results.csv".to_string());
    let abs_csv = if PathBuf::from(&output_csv).is_absolute() {
        PathBuf::from(&output_csv)
    } else {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(&output_csv)
    };

    let mut csv_writer = csv::Writer::from_path(&abs_csv).expect("Failed to create CSV");
    let _ = csv_writer.write_record(&[
        "run_id",
        "total_pnl",
        "realized_pnl",
        "net_edge_bps",
        "spread_capture_bps",
        "rebate_bps",
        "fill_rate_pct",
        "round_trips",
        "volume",
        "sharpe",
        "max_dd_pct",
        "phase1_calm",
        "phase1_mid",
        "phase1_min",
        "mid_impulse_lo",
        "mid_impulse_hi",
        "tight_spread_bps",
        "mid_regime_size_mult",
        "mp_edge_scale_enabled",
        "mp_edge_scale_k_bps",
        "mp_edge_scale_min_mult",
        "mp_edge_scale_max_mult",
        "elapsed_sec",
    ]);

    let (initial_quote, initial_base) = engine_capital_from_harness(&engine_harness);

    for (idx, (name, profile_cfg)) in experiments.iter().enumerate() {
        print!("\rRun {}/{}: {} ... ", idx + 1, experiments.len(), name);
        let _ = std::io::Write::flush(&mut std::io::stdout());

        let config = merge_profile_with_engine_defaults(&engine_harness, profile_cfg);
        let order_amount = order_amount_from_config(&config);
        let tick_size = tick_size_from_config(&config);
        let strategy = build_strategy(&config, order_amount, tick_size);
        let fee_model = simple_fee_model_from_config(&config);
        let queue_config = apply_crossed_book_survival_env(queue_model_from_config(&config));

        let loader = S3Loader::new(
            bucket.clone(),
            prefix.clone(),
            region.clone(),
            max_concurrent,
        )
        .await
        .expect("Failed to create S3 loader")
        .with_pair_filter(&pair)
        .with_max_files(max_files)
        .with_s3_key_date_range(key_date_range);

        let mut engine =
            BacktestEngine::new(strategy, initial_quote, initial_base, fee_model, tick_size)
                .with_queue_config(queue_config);

        let start = Instant::now();
        let result = match engine.run(loader).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("\nRun {} failed: {}", name, e);
                continue;
            }
        };
        let elapsed = start.elapsed().as_secs_f64();

        let s = &result.stats;
        let d = result.dashboard.as_ref();
        let net_edge = d.map(|x| x.net_edge_bps).unwrap_or(0.0);
        let spread_bps = d.map(|x| x.realized_spread_capture_bps).unwrap_or(0.0);
        let rebate_bps = d.map(|x| x.rebate_earned_bps).unwrap_or(0.0);
        let fill_rate = d.map(|x| x.fill_rate_pct).unwrap_or(0.0);

        let _ = csv_writer.write_record(&[
            name,
            &s.total_pnl.to_string(),
            &s.realized_pnl.to_string(),
            &format!("{:.4}", net_edge),
            &format!("{:.4}", spread_bps),
            &format!("{:.4}", rebate_bps),
            &format!("{:.2}", fill_rate),
            &s.round_trip_count.to_string(),
            &s.total_volume.to_string(),
            &format!("{:.2}", s.sharpe),
            &format!("{:.2}", s.max_drawdown * 100.0),
            &get_f64(&config, "impulse_phase1_calm", 1.2).to_string(),
            &get_f64(&config, "impulse_phase1_mid", 0.8).to_string(),
            &get_f64(&config, "impulse_phase1_min", 0.6).to_string(),
            &get_f64(&config, "mid_impulse_lo", 0.5).to_string(),
            &get_f64(&config, "mid_impulse_hi", 1.0).to_string(),
            &get_f64(&config, "tight_spread_bps", 3.0).to_string(),
            &get_f64(&config, "mid_regime_size_mult", 0.7).to_string(),
            &get_bool(&config, "microprice_edge_size_scale_enabled", false).to_string(),
            &get_f64(&config, "microprice_edge_scale_k_bps", 0.05).to_string(),
            &get_f64(&config, "microprice_edge_scale_min_mult", 0.3).to_string(),
            &get_f64(&config, "microprice_edge_scale_max_mult", 1.0).to_string(),
            &format!("{:.1}", elapsed),
        ]);
    }

    csv_writer.flush().expect("Failed to flush CSV");
    println!("\n\nResults written to {}", abs_csv.display());
}
