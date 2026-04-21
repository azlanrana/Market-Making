//! S3 backtest for RebateMM — uses the same strategy graph as `backtest_s3_sweep`
//! (`rebate_mm_sweep_builder::build_strategy` + `configs/sweep_rebate_mm.yaml`).
//!
//! Run: S3_BUCKET=ethusdt2025 S3_PREFIX=ETH_USDT/ AWS_REGION=us-east-1 \
//!      cargo test -p mm-engine backtest_s3_rebate_mm --release -- --ignored --nocapture
//!
//! Data window (optional):
//! - Narrow with `S3_PREFIX` (recommended), e.g. `2025/2025/01/` for January.
//! - Or set both `S3_START_DATE` and `S3_END_DATE` as `YYYY-MM-DD` (filters by `/YYYY/MM/DD/` in keys).
//! - `MAX_FILES` caps listing after filters (optional; omit for no cap).
//! - `BACKTEST_FAST_MODE=1` disables per-fill markout tracking to reduce CPU/RAM on dense months.
//!
//! Pick which sweep experiment to mirror (same merged YAML as sweep run N):
//!   REBATE_MM_SWEEP_RUN_INDEX=0   (default; first grid / experiment row)
//! Or ignore `grid` and use `base` only (same as `grid: {}` in YAML):
//!   REBATE_MM_BASE_ONLY=1
//!
//! Multi-layer passive L2 add-on (`REBATE_MM_L2_ADDON` / `with_passive_l2_addon`) is **not** in this
//! tree — see `scaling.md` Layer 3 (paused). Single-layer RebateMM only.
//!
//! With `BACKTEST_OUTPUT_CSV=…`, also writes `<stem>_fills.csv` and `<stem>_metrics.csv` for
//! `scripts/visualize_backtest.py` (metrics summary table).
//!
//! Config merge order: `configs/backtest_engine_harness.yaml` `defaults` + `capital`, then profile/sweep
//! `base` (profile wins on conflicts). Override harness file with `BACKTEST_ENGINE_HARNESS`.
//! `CROSSED_BOOK_SURVIVAL_RATE` still overrides YAML when set.

mod common;

use mm_engine::BacktestEngine;
use mm_metrics::TOXIC_FLOW_THRESHOLD_BPS;
use common::rebate_mm_sweep_builder::{
    apply_crossed_book_survival_env, build_strategy, engine_capital_from_harness, experiments_from_sweep, merge,
    merge_profile_with_engine_defaults, order_amount_from_config, queue_model_from_config, read_backtest_engine_harness,
    read_sweep_yaml, simple_fee_model_from_config, tick_size_from_config,
};
use data_loader::{parse_s3_inclusive_date_range_from_env, S3Loader};
use mm_core_types::{Fill as CoreFill, Side};
use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Instant;

fn print_dashboard(d: &mm_engine::MMDashboardSummary) {
    println!("\n--- MM Dashboard ---");
    println!("  Fill rate:         {:.2}%", d.fill_rate_pct);
    println!("  Maker ratio:       {:.1}%", d.maker_ratio_pct);
    println!(
        "  Maker reasons:     {:.1}% queue depletion ({}) / {:.1}% crossed ({})",
        d.queue_depletion_fill_pct,
        d.queue_depletion_fill_count,
        d.crossed_book_fill_pct,
        d.crossed_book_fill_count
    );
    println!(
        "  Cancel-ahead adv.: {} events / {:.4} total",
        d.cancel_ahead_advance_events,
        d.cancel_ahead_advance_total
    );
    println!("  Net edge/trade:    {:+.2} bps", d.net_edge_bps);
    println!("  1s markout:        {:+.2} bps", d.markout_1s_bps);
    println!("  5s markout:        {:+.2} bps", d.markout_5s_bps);
    println!("  1s adverse sel.:   {:+.2} bps", d.adverse_selection_1s_bps);
    println!("  5s adverse sel.:   {:+.2} bps", d.adverse_selection_5s_bps);
    println!(
        "  Spread capture:    ${:+.2} ({:+.2} bps)",
        d.realized_spread_capture_pnl,
        d.realized_spread_capture_bps
    );
    println!(
        "  Rebate earned:     ${:+.2} ({:+.2} bps)",
        d.rebate_earned_pnl,
        d.rebate_earned_bps
    );
    println!(
        "  Inventory drag:    ${:+.2} ({:+.2} bps)",
        d.inventory_drag_pnl,
        d.inventory_drag_bps
    );
    println!("  Inventory PnL:     {:.1}% of total", d.inventory_pnl_ratio);
    println!("  Turnover:          {:.0}x daily", d.turnover_daily);
    println!("  Avg inventory:     {:.4}", d.avg_inventory);
    println!("  Max inventory:     {:.4}", d.max_inventory);
    println!("  Avg quote lifetime: {:.0} ms", d.avg_quote_lifetime_ms);
}

#[tokio::test]
#[ignore] // Requires S3 credentials and bucket
async fn backtest_s3_rebate_mm() {
    let bucket = match std::env::var("S3_BUCKET") {
        Ok(b) => b,
        Err(_) => {
            println!("Skipping: S3_BUCKET not set.");
            println!("Run: REBATE_MM_PROFILE=eth S3_BUCKET=... TRADING_PAIR=ETH_USD ... cargo test -p mm-engine backtest_s3_rebate_mm --release -- --ignored --nocapture");
            return;
        }
    };

    let (sweep, yaml_path) = match read_sweep_yaml() {
        Ok(x) => x,
        Err(e) => {
            eprintln!("{}", e);
            return;
        }
    };

    let (engine_harness, harness_path) = read_backtest_engine_harness();
    if let Some(ref hp) = harness_path {
        println!("Engine harness: {}", hp.display());
    }

    let base_only = std::env::var("REBATE_MM_BASE_ONLY").map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false);
    let (run_label, profile_config) = if base_only {
        (
            "base_only (REBATE_MM_BASE_ONLY)".to_string(),
            merge(&sweep.base, &HashMap::new()),
        )
    } else {
        let experiments = experiments_from_sweep(&sweep);
        if experiments.is_empty() {
            eprintln!("sweep config must have 'grid' or 'experiments'");
            return;
        }
        let idx: usize = std::env::var("REBATE_MM_SWEEP_RUN_INDEX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if idx >= experiments.len() {
            eprintln!(
                "REBATE_MM_SWEEP_RUN_INDEX {} out of range ({} experiments). Valid: 0..{}",
                idx,
                experiments.len(),
                experiments.len().saturating_sub(1)
            );
            return;
        }
        let (name, cfg) = &experiments[idx];
        (format!("{} [index {}]", name, idx), cfg.clone())
    };

    let config = merge_profile_with_engine_defaults(&engine_harness, &profile_config);

    let pair = std::env::var("TRADING_PAIR").unwrap_or_else(|_| "ETH_USDT".to_string());

    println!("\n=== RebateMM S3 Backtest ===");
    println!("Sweep YAML: {}", yaml_path.display());
    println!("Experiment: {}", run_label);
    println!("Strategy: same graph as backtest_s3_sweep (see rebate_mm_sweep_builder.rs)");

    let prefix = std::env::var("S3_PREFIX").unwrap_or_else(|_| format!("{}/", pair));
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let max_files = std::env::var("MAX_FILES")
        .ok()
        .and_then(|s| s.parse().ok());
    let key_date_range = parse_s3_inclusive_date_range_from_env()
        .expect("S3_START_DATE / S3_END_DATE: set both as YYYY-MM-DD or neither");
    let max_concurrent = std::env::var("MAX_CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);

    let loader = S3Loader::new(bucket.clone(), prefix.clone(), region.clone(), max_concurrent)
        .await
        .expect("Failed to create S3 loader")
        .with_pair_filter(&pair)
        .with_max_files(max_files)
        .with_s3_key_date_range(key_date_range);

    let order_amount = order_amount_from_config(&config);
    let tick_size = tick_size_from_config(&config);

    let strategy = build_strategy(&config, order_amount, tick_size);

    let fee_model = simple_fee_model_from_config(&config);

    let queue_config = apply_crossed_book_survival_env(queue_model_from_config(&config));

    println!(
        "Harness: order_amount={} tick_size={} maker_fee_bps={} taker_fee_bps={} touch_queue_pct={} crossed_book_survival_rate={} (env CROSSED_BOOK_SURVIVAL_RATE overrides)",
        order_amount,
        tick_size,
        fee_model.maker_bps,
        fee_model.taker_bps,
        queue_config.touch_queue_pct,
        queue_config.crossed_book_survival_rate
    );

    let (initial_quote, initial_base) = engine_capital_from_harness(&engine_harness);
    let output_csv = std::env::var("BACKTEST_OUTPUT_CSV").ok();
    let fast_mode = std::env::var("BACKTEST_FAST_MODE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if fast_mode {
        println!("Fast mode: markout tracking disabled (BACKTEST_FAST_MODE=1)");
    }

    let fills = output_csv
        .as_ref()
        .map(|_| Arc::new(Mutex::new(Vec::<CoreFill>::new())));

    let mut engine = BacktestEngine::new(strategy, initial_quote, initial_base, fee_model, tick_size)
        .with_queue_config(queue_config)
        .with_markout_enabled(!fast_mode);

    if let Some(ref fills) = fills {
        let fills_clone = fills.clone();
        engine = engine.with_fill_callback(move |f: &CoreFill| {
            fills_clone.lock().unwrap().push(f.clone());
        });
    }

    let start_time = Instant::now();
    let results = match engine.run(loader).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Backtest failed: {}", e);
            return;
        }
    };
    let elapsed = start_time.elapsed();

    let s = &results.stats;
    let sim_duration_sec = (results.last_ts - results.first_ts).max(1.0);
    let sim_hours = sim_duration_sec / 3600.0;
    let sim_days = sim_hours / 24.0;

    println!("\n=== RebateMM Results ===");
    println!("Backtest time:   {:.1}s", elapsed.as_secs_f64());
    println!("Simulated:      {:.1}h ({:.2} days)", sim_hours, sim_days);
    println!("Snapshots:      {}", results.snapshot_count);

    println!("\n--- P&L ---");
    println!("Total PnL:       ${}", s.total_pnl.round_dp(2));
    println!("Realized PnL:   ${}", s.realized_pnl.round_dp(2));
    println!("Round trips:    {}", s.round_trip_count);
    println!("Volume:         ${}", s.total_volume.round_dp(0));

    println!("\n--- Risk ---");
    println!("Win rate:       {:.1}%", s.win_rate * 100.0);
    println!("Sharpe:         {:.2}", s.sharpe);
    println!("Max drawdown:   {:.2}%", s.max_drawdown * 100.0);

    if let Some(ref d) = results.dashboard {
        print_dashboard(d);
    }

    if let Some(ref gates) = results.gate_diagnostics {
        println!("\n{}", gates);
    }

    if let Some(csv_path) = output_csv {
        let path = std::path::Path::new(&csv_path);
        let abs_path = if path.is_relative() {
            let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
            let workspace_root = manifest.ancestors().nth(2).unwrap_or(manifest);
            workspace_root.join(path)
        } else {
            path.to_path_buf()
        };
        if let Some(parent) = abs_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut f) = std::fs::File::create(&abs_path) {
            let _ = writeln!(f, "timestamp,portfolio_value");
            for (ts, pv) in &results.portfolio_snapshots {
                let _ = writeln!(f, "{},{}", ts, pv);
            }
            println!(
                "\nExported {} points to {} for visualization",
                results.portfolio_snapshots.len(),
                abs_path.display()
            );
        } else {
            eprintln!("Failed to create {}", abs_path.display());
        }

        if let Some(fills) = fills.as_ref() {
            let fills_guard = fills.lock().unwrap();
            if !fills_guard.is_empty() {
                let fills_path = abs_path.with_extension("");
                let fills_path = format!("{}_fills.csv", fills_path.display());
                let fills_path = std::path::Path::new(&fills_path);
                if let Ok(mut f) = std::fs::File::create(fills_path) {
                    let _ = writeln!(f, "timestamp,side,price,amount,value_usd,order_id,layer,fill_reason");
                    for fill in fills_guard.iter() {
                        let side = match fill.side {
                            Side::Buy => "BUY",
                            Side::Sell => "SELL",
                        };
                        let value = (fill.price * fill.amount).to_string().trim().to_string();
                        let reason = fill
                            .fill_reason
                            .map(|r| format!("{:?}", r))
                            .unwrap_or_else(|| String::new());
                        let _ = writeln!(
                            f,
                            "{},{},{},{},{},{},{},{}",
                            fill.timestamp,
                            side,
                            fill.price,
                            fill.amount,
                            value,
                            fill.order_id,
                            fill.layer,
                            reason
                        );
                    }
                    println!("Exported {} fills to {}", fills_guard.len(), fills_path.display());
                }
            }
        }

        let markout_path = format!("{}_markout_1s.csv", abs_path.with_extension("").display());
        if let Ok(mut mf) = std::fs::File::create(&markout_path) {
            let _ = writeln!(
                mf,
                "fill_timestamp,order_id,side,mid_at_fill,mid_1s,markout_bps,adverse_bps,fill_reason,toxic_threshold_bps"
            );
            for r in &results.resolved_1s_markouts {
                let side = match r.side {
                    Side::Buy => "BUY",
                    Side::Sell => "SELL",
                };
                let reason = r
                    .fill_reason
                    .map(|x| format!("{:?}", x))
                    .unwrap_or_default();
                let _ = writeln!(
                    mf,
                    "{},{},{},{},{},{},{},{},{}",
                    r.fill_timestamp,
                    r.order_id,
                    side,
                    r.mid_at_fill,
                    r.mid_1s,
                    r.markout_bps,
                    r.adverse_bps,
                    reason,
                    TOXIC_FLOW_THRESHOLD_BPS
                );
            }
            println!(
                "Exported {} rows to {} (1s markout / adverse per fill)",
                results.resolved_1s_markouts.len(),
                markout_path
            );
        } else {
            eprintln!("Failed to create {}", markout_path);
        }

        // One-row CSV for visualize_backtest.py metrics panel (same stem as portfolio CSV).
        let metrics_name = abs_path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| format!("{}_metrics.csv", s))
            .unwrap_or_else(|| "backtest_metrics.csv".to_string());
        let metrics_path = abs_path.with_file_name(&metrics_name);
        let dash = results.dashboard.as_ref();
        let (
            fr,
            ne,
            scu,
            scb,
            ru,
            rb,
            td,
            gf,
            nf,
            tf,
            tb,
            ta,
            adv1,
            mo1,
        ) = match dash {
            Some(d) => (
                format!("{:.4}", d.fill_rate_pct),
                format!("{:.4}", d.net_edge_bps),
                format!("{:.2}", d.realized_spread_capture_pnl),
                format!("{:.4}", d.realized_spread_capture_bps),
                format!("{:.2}", d.rebate_earned_pnl),
                format!("{:.4}", d.rebate_earned_bps),
                format!("{:.4}", d.turnover_daily),
                format!("{:.4}", d.good_fill_pct),
                format!("{:.4}", d.neutral_fill_pct),
                format!("{:.4}", d.toxic_fill_pct),
                format!("{:.4}", d.toxic_bid_pct),
                format!("{:.4}", d.toxic_ask_pct),
                format!("{:.4}", d.adverse_selection_1s_bps),
                format!("{:.4}", d.markout_1s_bps),
            ),
            None => (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ),
        };
        if let Ok(mut mf) = std::fs::File::create(&metrics_path) {
            let _ = writeln!(
                mf,
                "win_rate_pct,sharpe,max_drawdown_pct,fill_rate_pct,net_edge_bps,spread_capture_usd,spread_capture_bps,rebate_earned_usd,rebate_earned_bps,turnover_daily,good_fill_pct,neutral_fill_pct,toxic_fill_pct,toxic_bid_pct,toxic_ask_pct,adverse_selection_1s_bps,markout_1s_bps,volume_usd,final_pnl_usd"
            );
            let _ = writeln!(
                mf,
                "{:.4},{:.4},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                s.win_rate * 100.0,
                s.sharpe,
                s.max_drawdown * 100.0,
                fr,
                ne,
                scu,
                scb,
                ru,
                rb,
                td,
                gf,
                nf,
                tf,
                tb,
                ta,
                adv1,
                mo1,
                s.total_volume.round_dp(2),
                s.total_pnl.round_dp(2),
            );
            println!("Exported metrics to {}", metrics_path.display());
        } else {
            eprintln!("Failed to create {}", metrics_path.display());
        }
    }
}
