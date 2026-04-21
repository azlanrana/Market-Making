//! Shared RebateMM strategy construction for `backtest_s3_sweep` and `backtest_s3_rebate_mm`.
//! Keep sweep YAML and this builder in sync — both tests use `build_strategy` here.
//!
//! Merge order: `read_backtest_engine_harness` `defaults` + profile/sweep row (profile wins);
//! see `configs/backtest_engine_harness.yaml`.

use mm_engine::{QueueModelConfig, SimpleFeeModel};
use rebate_mm::RebateMMStrategy;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct SweepConfig {
    pub base: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub grid: Option<HashMap<String, serde_yaml::Value>>,
    #[serde(default)]
    pub experiments: Option<Vec<HashMap<String, serde_yaml::Value>>>,
}

pub fn cartesian_product(grid: &HashMap<String, serde_yaml::Value>) -> Vec<HashMap<String, serde_yaml::Value>> {
    let mut keys: Vec<&String> = grid.keys().collect();
    if keys.is_empty() {
        return vec![HashMap::new()];
    }
    keys.sort();

    let mut result = vec![HashMap::new()];
    for key in keys {
        let values = match &grid[key] {
            serde_yaml::Value::Sequence(seq) => seq.clone(),
            serde_yaml::Value::Number(n) => vec![serde_yaml::Value::Number(n.clone())],
            serde_yaml::Value::Bool(b) => vec![serde_yaml::Value::Bool(*b)],
            serde_yaml::Value::String(s) => vec![serde_yaml::Value::String(s.clone())],
            _ => continue,
        };
        let mut next = Vec::with_capacity(result.len() * values.len());
        for combo in result {
            for v in &values {
                let mut c = combo.clone();
                c.insert(key.clone(), v.clone());
                next.push(c);
            }
        }
        result = next;
    }
    result
}

pub fn merge(base: &HashMap<String, serde_yaml::Value>, overrides: &HashMap<String, serde_yaml::Value>) -> HashMap<String, serde_yaml::Value> {
    let mut m = base.clone();
    for (k, v) in overrides {
        m.insert(k.clone(), v.clone());
    }
    m
}

/// Master file: shared fees, queue sim, optional tick; merged under profile/sweep (`merge` = profile wins).
#[derive(Debug, Deserialize, Default)]
pub struct BacktestEngineHarnessYaml {
    #[serde(default)]
    pub defaults: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    pub capital: Option<HarnessCapitalYaml>,
}

#[derive(Debug, Deserialize, Default)]
pub struct HarnessCapitalYaml {
    #[serde(default = "default_capital_quote")]
    pub initial_quote: f64,
    #[serde(default = "default_capital_base")]
    pub initial_base: f64,
}

fn default_capital_quote() -> f64 {
    1_000_000.0
}

fn default_capital_base() -> f64 {
    1.0
}

fn harness_config_candidate_paths() -> Vec<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let configs_dir = manifest_dir.join("../../configs");
    let mut paths = Vec::new();
    if let Ok(p) = std::env::var("BACKTEST_ENGINE_HARNESS") {
        let p = p.trim();
        if !p.is_empty() {
            paths.push(PathBuf::from(p));
        }
    }
    paths.push(configs_dir.join("backtest_engine_harness.yaml"));
    paths.push(PathBuf::from("configs/backtest_engine_harness.yaml"));
    paths.push(PathBuf::from("mm/configs/backtest_engine_harness.yaml"));
    paths
}

/// Load master harness YAML. If no file is found, returns empty defaults (Rust fallbacks in `*_from_config` apply).
pub fn read_backtest_engine_harness() -> (BacktestEngineHarnessYaml, Option<PathBuf>) {
    for p in harness_config_candidate_paths() {
        if let Ok(s) = std::fs::read_to_string(&p) {
            let h = serde_yaml::from_str::<BacktestEngineHarnessYaml>(&s).unwrap_or_else(|e| {
                panic!("Invalid backtest engine harness YAML {}: {}", p.display(), e);
            });
            return (h, Some(p));
        }
    }
    (BacktestEngineHarnessYaml::default(), None)
}

/// `engine.defaults` then `profile` (profile wins on key collision).
pub fn merge_profile_with_engine_defaults(
    engine: &BacktestEngineHarnessYaml,
    profile: &HashMap<String, serde_yaml::Value>,
) -> HashMap<String, serde_yaml::Value> {
    merge(&engine.defaults, profile)
}

pub fn engine_capital_from_harness(engine: &BacktestEngineHarnessYaml) -> (Decimal, Decimal) {
    match &engine.capital {
        Some(c) => (
            Decimal::from_f64_retain(c.initial_quote).unwrap_or(dec!(1000000)),
            Decimal::from_f64_retain(c.initial_base).unwrap_or(dec!(1)),
        ),
        None => (dec!(1000000), dec!(1)),
    }
}

pub fn get_f64(m: &HashMap<String, serde_yaml::Value>, key: &str, default: f64) -> f64 {
    m.get(key)
        .and_then(|v| match v {
            serde_yaml::Value::Number(n) => n.as_f64(),
            _ => None,
        })
        .unwrap_or(default)
}

pub fn get_bool(m: &HashMap<String, serde_yaml::Value>, key: &str, default: bool) -> bool {
    m.get(key)
        .and_then(|v| match v {
            serde_yaml::Value::Bool(b) => Some(*b),
            _ => None,
        })
        .unwrap_or(default)
}

fn get_opt_f64(m: &HashMap<String, serde_yaml::Value>, key: &str) -> Option<f64> {
    m.get(key).and_then(|v| match v {
        serde_yaml::Value::Number(n) => n.as_f64(),
        serde_yaml::Value::Null => None,
        _ => None,
    })
}

pub fn run_name(config: &HashMap<String, serde_yaml::Value>, idx: usize) -> String {
    let mut parts = Vec::new();
    let keys = [
        "mid_impulse_lo",
        "mid_impulse_hi",
        "tight_spread_bps",
        "mid_regime_size_mult",
        "impulse_phase1_calm",
        "impulse_phase1_mid",
        "impulse_phase1_min",
        "impulse_phase1_sizing",
        "directional_skew_enabled",
        "directional_suppress_threshold_ticks",
        "microprice_edge_threshold_bps",
        "microprice_edge_size_scale_enabled",
        "microprice_edge_scale_k_bps",
        "microprice_edge_scale_min_mult",
        "microprice_edge_scale_max_mult",
        "impulse_kill_threshold_bps",
        "impulse_kill_asymmetric",
        "queue_join_safe_side_threshold_bps",
        "queue_join_safe_side_max_touch_qty",
        "dynamic_conditional_touch_enabled",
        "dynamic_conditional_touch_enter_bps",
        "dynamic_conditional_touch_exit_bps",
        "dynamic_order_max",
        "state_passive_max_depth_ticks",
        "base_spread",
        "spread_depth_prob_touch_p_good",
        "spread_depth_prob_touch_p_neutral",
        "spread_depth_prob_touch_p_bad",
    ];
    for k in keys {
        if let Some(v) = config.get(k) {
            let s = match v {
                serde_yaml::Value::Bool(b) => format!("{}={}", k, b),
                serde_yaml::Value::Number(n) => format!("{}={}", k, n),
                _ => continue,
            };
            parts.push(s);
        }
    }
    if parts.is_empty() {
        format!("run_{}", idx)
    } else {
        parts.join("_")
    }
}

/// Same experiment list as `backtest_s3_sweep` (grid cartesian product or named experiments).
pub fn experiments_from_sweep(sweep: &SweepConfig) -> Vec<(String, HashMap<String, serde_yaml::Value>)> {
    if let Some(grid) = &sweep.grid {
        let combos = cartesian_product(grid);
        combos
            .into_iter()
            .enumerate()
            .map(|(i, overrides)| {
                let merged = merge(&sweep.base, &overrides);
                let name = run_name(&merged, i);
                (name, merged)
            })
            .collect()
    } else if let Some(exps) = &sweep.experiments {
        exps.iter()
            .enumerate()
            .map(|(i, overrides)| {
                let name = overrides
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&format!("run_{}", i))
                    .to_string();
                let mut merged = merge(&sweep.base, overrides);
                merged.remove("name");
                (name, merged)
            })
            .collect()
    } else {
        vec![]
    }
}

/// Resolve config file paths: optional `REBATE_MM_PROFILE=eth|btc|btcusdt` → `rebate_mm_{profile}.yaml`,
/// then `SWEEP_CONFIG`, then default `sweep_rebate_mm.yaml`.
fn sweep_config_candidate_paths() -> Vec<PathBuf> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let configs_dir = manifest_dir.join("../../configs");
    let mut paths = Vec::new();

    if let Ok(profile) = std::env::var("REBATE_MM_PROFILE") {
        let slug = profile.trim().to_lowercase();
        if !slug.is_empty() {
            let name = format!("rebate_mm_{}.yaml", slug);
            paths.push(configs_dir.join(&name));
            paths.push(PathBuf::from("configs").join(&name));
            paths.push(PathBuf::from("mm/configs").join(&name));
        }
    }

    if let Ok(p) = std::env::var("SWEEP_CONFIG") {
        let p = p.trim();
        if !p.is_empty() {
            // Integration tests often run with cwd = crates/engine; relative paths like
            // `configs/foo.yaml` must resolve under mm/configs/, not cwd.
            if let Some(rest) = p
                .strip_prefix("configs/")
                .or_else(|| p.strip_prefix("./configs/"))
            {
                paths.push(configs_dir.join(rest));
            } else if let Some(rest) = p.strip_prefix("mm/configs/") {
                paths.push(configs_dir.join(rest));
            } else if !p.contains('/') && !p.contains('\\') {
                paths.push(configs_dir.join(p));
            }
            paths.push(PathBuf::from(p));
        }
    }

    paths.push(configs_dir.join("sweep_rebate_mm.yaml"));
    paths.push(PathBuf::from("configs/sweep_rebate_mm.yaml"));
    paths.push(PathBuf::from("mm/configs/sweep_rebate_mm.yaml"));
    paths
}

pub fn read_sweep_yaml() -> Result<(SweepConfig, PathBuf), String> {
    let config_paths = sweep_config_candidate_paths();
    for p in &config_paths {
        if let Ok(s) = std::fs::read_to_string(p) {
            let sweep: SweepConfig = serde_yaml::from_str(&s).map_err(|e| e.to_string())?;
            return Ok((sweep, p.clone()));
        }
    }
    Err(format!(
        "Failed to read sweep config. Tried: {:?}. Set REBATE_MM_PROFILE (eth|btc|btcusdt) or SWEEP_CONFIG.",
        config_paths
    ))
}

/// Per-order size from merged YAML (`order_amount`). Default matches legacy harness.
pub fn order_amount_from_config(m: &HashMap<String, serde_yaml::Value>) -> Decimal {
    Decimal::from_f64_retain(get_f64(m, "order_amount", 0.5)).unwrap_or(dec!(0.5))
}

/// Instrument tick from merged YAML (`tick_size`). Default matches legacy harness.
pub fn tick_size_from_config(m: &HashMap<String, serde_yaml::Value>) -> Decimal {
    Decimal::from_f64_retain(get_f64(m, "tick_size", 0.01)).unwrap_or(dec!(0.01))
}

/// Maker/taker fee schedule in bps (negative maker = rebate). Keys optional; defaults match legacy harness.
pub fn simple_fee_model_from_config(m: &HashMap<String, serde_yaml::Value>) -> SimpleFeeModel {
    let maker = Decimal::from_f64_retain(get_f64(m, "maker_fee_bps", -0.75)).unwrap_or(dec!(-0.75));
    let taker = Decimal::from_f64_retain(get_f64(m, "taker_fee_bps", 1.5)).unwrap_or(dec!(1.5));
    SimpleFeeModel::new(maker, taker)
}

/// Queue simulation tuning. Defaults match the previous hardcoded S3 harness; optional YAML overrides per asset.
pub fn queue_model_from_config(m: &HashMap<String, serde_yaml::Value>) -> QueueModelConfig {
    let mut q = QueueModelConfig::default();
    q.touch_queue_pct = get_f64(m, "touch_queue_pct", 0.4);
    q.queue_depletion_enabled = get_bool(m, "queue_depletion_enabled", true);
    q.queue_churn_enabled = get_bool(m, "queue_churn_enabled", false);
    q.delta_trade_fraction = get_f64(m, "delta_trade_fraction", 0.4);
    q.min_delta_for_fill = get_f64(m, "min_delta_for_fill", 0.001);
    q.queue_turnover_rate_per_sec = get_f64(m, "queue_turnover_rate_per_sec", 0.5);
    q.cancel_ahead_fraction = get_f64(m, "cancel_ahead_fraction", 0.5);
    q.crossed_book_survival_rate = get_f64(m, "crossed_book_survival_rate", 0.5);
    q.crossed_book_fill_enabled = get_bool(m, "crossed_book_fill_enabled", true);
    q.price_improving_queue_pct = get_f64(m, "price_improving_queue_pct", q.price_improving_queue_pct);
    q.price_improving_bid_depletion_blend =
        get_f64(m, "price_improving_bid_depletion_blend", q.price_improving_bid_depletion_blend);
    q.price_improving_ask_depletion_blend =
        get_f64(m, "price_improving_ask_depletion_blend", q.price_improving_ask_depletion_blend);
    q.queue_decay_enabled = get_bool(m, "queue_decay_enabled", q.queue_decay_enabled);
    q
}

/// Apply `CROSSED_BOOK_SURVIVAL_RATE` env override when set (parsed as f64).
pub fn apply_crossed_book_survival_env(mut q: QueueModelConfig) -> QueueModelConfig {
    if let Ok(s) = std::env::var("CROSSED_BOOK_SURVIVAL_RATE") {
        if let Ok(v) = s.parse::<f64>() {
            q.crossed_book_survival_rate = v;
        }
    }
    q
}

/// Identical to the strategy graph in `backtest_s3_sweep`.
pub fn build_strategy(config: &HashMap<String, serde_yaml::Value>, order_amount: Decimal, tick_size: Decimal) -> RebateMMStrategy {
    let base_spread = get_f64(config, "base_spread", 4.0);
    let vol_lookback = config.get("vol_lookback").and_then(|v| v.as_u64()).unwrap_or(50) as usize;
    let vol_threshold = get_f64(config, "vol_threshold_bps", 2.0);
    let max_spread_mult = get_f64(config, "max_spread_multiplier", 4.0);
    let inv_limit = Decimal::from_f64_retain(get_f64(config, "inventory_limit", 1.0)).unwrap_or(dec!(1.0));
    let hedge_limit = Decimal::from_f64_retain(get_f64(config, "hedge_limit", 1.0)).unwrap_or(dec!(1.0));
    let inv_skew = get_f64(config, "inventory_skew_k", 0.25);
    let book_imb_thresh = get_f64(config, "book_imbalance_threshold", 0.80);
    let book_imb_levels = config.get("book_imbalance_levels").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
    let mp_imp_lookback = config.get("microprice_impulse_lookback").and_then(|v| v.as_u64()).unwrap_or(4) as usize;
    let mp_imp_thresh = get_f64(config, "microprice_impulse_threshold_bps", 1.0);
    let mp_imp_pause = get_f64(config, "microprice_impulse_pause_sec", 1.0);
    let microprice_edge_thresh = get_f64(config, "microprice_edge_threshold_bps", 0.0);
    let mp_edge_scale = get_bool(config, "microprice_edge_size_scale_enabled", false);
    let mp_edge_scale_k = get_f64(config, "microprice_edge_scale_k_bps", 0.05);
    let mp_edge_scale_min = get_f64(config, "microprice_edge_scale_min_mult", 0.3);
    let mp_edge_scale_max = get_f64(config, "microprice_edge_scale_max_mult", 1.0);
    let mp_tape_regime = get_bool(config, "microprice_edge_tape_regime_enabled", false);
    let mp_tape_alpha = get_f64(config, "microprice_edge_tape_spread_ewma_alpha", 0.02);
    let mp_tape_low = get_f64(config, "microprice_edge_tape_relax_low_spread_bps", -0.02);
    let mp_tape_high = get_f64(config, "microprice_edge_tape_relax_high_spread_bps", 0.08);
    let mp_tape_min_fills = config
        .get("microprice_edge_tape_min_fills")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as u32;
    let mp_tape_upside = get_bool(config, "microprice_edge_tape_upside_enabled", false);
    let mp_tape_upside_max = get_f64(config, "microprice_edge_tape_upside_max_mult", 1.15);
    let mp_tape_upside_low = get_f64(config, "microprice_edge_tape_upside_low_spread_bps", 0.10);
    let mp_tape_upside_high = get_f64(config, "microprice_edge_tape_upside_high_spread_bps", 0.20);
    let mp_tape_upside_min_fills = config
        .get("microprice_edge_tape_upside_min_fills")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as u32;
    let mp_tape_upside_regime_band =
        get_bool(config, "microprice_edge_tape_upside_use_regime_band", true);
    let phase1_sizing = get_bool(config, "impulse_phase1_sizing", true);
    let phase1_calm = get_f64(config, "impulse_phase1_calm", 1.2);
    let phase1_mid = get_f64(config, "impulse_phase1_mid", 0.8);
    let phase1_min = get_f64(config, "impulse_phase1_min", 0.6);
    let impulse_kill_thresh = get_f64(config, "impulse_kill_threshold_bps", 2.0);
    let impulse_kill_asym = get_bool(config, "impulse_kill_asymmetric", false);
    let impulse_kill_hybrid = get_opt_f64(config, "impulse_kill_hybrid_extreme_bps");
    let impulse_size_skew = get_bool(config, "impulse_size_skew_enabled", false);
    let impulse_skew_favored = get_f64(config, "impulse_skew_favored", 1.15);
    let impulse_skew_lean = get_f64(config, "impulse_skew_lean", 0.75);
    let directional_skew = get_bool(config, "directional_skew_enabled", false);
    let directional_signal = get_f64(config, "directional_signal_threshold_ticks", 0.5);
    let directional_suppress = get_f64(config, "directional_suppress_threshold_ticks", 1.0);
    let directional_widen = config.get("directional_widen_ticks").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let drift_enabled = get_bool(config, "microprice_drift_enabled", false);
    let drift_lookback = config.get("microprice_drift_lookback").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
    let drift_thresh = get_f64(config, "microprice_drift_threshold_bps", 2.0);
    let microprice_fade = get_bool(config, "microprice_fade_enabled", false);
    let microprice_fade_min_edge = get_f64(config, "microprice_fade_min_edge_bps", 0.02);
    let microprice_fade_ticks = config
        .get("microprice_fade_ticks")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;
    let microprice_fade_impulse_min_bps = get_f64(config, "microprice_fade_impulse_min_bps", 0.0);
    let microprice_fade_impulse_bucketed =
        get_bool(config, "microprice_fade_impulse_bucketed", false);
    let microprice_fade_bucket_lo = get_f64(config, "microprice_fade_bucket_impulse_lo_bps", 0.3);
    let microprice_fade_bucket_hi = get_f64(config, "microprice_fade_bucket_impulse_hi_bps", 0.8);
    let microprice_fade_bucket_max_ticks = config
        .get("microprice_fade_bucket_max_ticks")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as u32;
    let conditional_touch_join = get_bool(config, "conditional_touch_join_enabled", false);
    let conditional_touch_max_impulse =
        get_f64(config, "conditional_touch_max_impulse_bps", 0.6);
    let conditional_touch_max_edge = get_f64(config, "conditional_touch_max_edge_bps", 0.02);
    let dynamic_conditional_touch = get_bool(config, "dynamic_conditional_touch_enabled", false);
    let dynamic_conditional_touch_alpha =
        get_f64(config, "dynamic_conditional_touch_ewma_alpha", 0.02);
    let dynamic_conditional_touch_min_fills = config
        .get("dynamic_conditional_touch_min_fills")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as u32;
    let dynamic_conditional_touch_enter =
        get_f64(config, "dynamic_conditional_touch_enter_bps", -0.30);
    let dynamic_conditional_touch_exit =
        get_f64(config, "dynamic_conditional_touch_exit_bps", -0.15);
    let fill_touch_brake = get_bool(config, "fill_touch_brake_enabled", false);
    let fill_touch_brake_bad = get_f64(config, "fill_touch_brake_bad_spread_bps", -0.5);
    let fill_touch_brake_sec = get_f64(config, "fill_touch_brake_sec", 0.2);
    let spread_depth_regime = get_bool(config, "spread_depth_regime_enabled", false);
    let spread_depth_alpha = get_f64(config, "spread_depth_regime_ewma_alpha", 0.02);
    let spread_depth_min_fills = config
        .get("spread_depth_regime_min_fills")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as u32;
    let spread_depth_bad_ent = get_f64(config, "spread_depth_bad_enter_bps", -0.3);
    let spread_depth_bad_ex = get_f64(config, "spread_depth_bad_exit_bps", -0.2);
    let spread_depth_good_ent = get_f64(config, "spread_depth_good_enter_bps", 0.1);
    let spread_depth_good_ex = get_f64(config, "spread_depth_good_exit_bps", 0.05);
    let spread_depth_ticks_bad = config
        .get("spread_depth_ticks_bad")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as u32;
    let spread_depth_ticks_neutral = config
        .get("spread_depth_ticks_neutral")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;
    let spread_depth_ticks_good = config
        .get("spread_depth_ticks_good")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let spread_depth_part_floor = get_bool(config, "spread_depth_participation_floor_enabled", false);
    let spread_depth_part_window = get_f64(config, "spread_depth_participation_window_sec", 900.0);
    let spread_depth_part_min = config
        .get("spread_depth_participation_min_fills")
        .and_then(|v| v.as_u64())
        .unwrap_or(2) as u32;
    let spread_depth_prob_touch = get_bool(config, "spread_depth_prob_touch_enabled", false);
    let spread_depth_prob_p_good = get_f64(config, "spread_depth_prob_touch_p_good", 0.85);
    let spread_depth_prob_p_neutral = get_f64(config, "spread_depth_prob_touch_p_neutral", 0.5);
    let spread_depth_prob_p_bad = get_f64(config, "spread_depth_prob_touch_p_bad", 0.05);
    let spread_toxicity_brake = get_bool(config, "spread_toxicity_brake_enabled", false);
    let spread_toxicity_alpha = get_f64(config, "spread_toxicity_ewma_alpha", 0.02);
    let spread_toxicity_min_fills = config
        .get("spread_toxicity_min_fills")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as u32;
    let spread_toxicity_enter = get_f64(config, "spread_toxicity_enter_bps", -0.7);
    let spread_toxicity_exit = get_f64(config, "spread_toxicity_exit_bps", -0.4);
    let spread_toxicity_touch_mult = get_f64(config, "spread_toxicity_touch_mult", 0.65);
    let spread_toxicity_forced_ticks = config
        .get("spread_toxicity_forced_passive_ticks")
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as u32;
    let dynamic_spread = get_bool(config, "dynamic_spread_enabled", false);
    let queue_join_touch = get_bool(config, "queue_join_touch_enabled", true);
    let wide_spread_max = get_f64(config, "wide_spread_max_bps", 6.0);
    let asym_passive = get_bool(config, "asymmetric_passive_depth", true);
    let qj_thresh = get_f64(config, "queue_join_safe_side_threshold_bps", 0.45);
    let qj_max_touch = get_f64(config, "queue_join_safe_side_max_touch_qty", 30.0);
    let state_max_depth = config.get("state_passive_max_depth_ticks").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
    let depth_bias = get_bool(config, "passive_depth_spread_capture_bias", true);
    let stick_min_hold = get_f64(config, "quote_stickiness_min_hold_sec", 0.25);
    let stick_hyst = config.get("quote_stickiness_hysteresis_ticks").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
    let stick_depth_delta = config.get("quote_stickiness_depth_min_delta_ticks").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
    let refresh = get_f64(config, "order_refresh_sec", 0.75);
    let warmup = get_f64(config, "warmup_sec", 30.0);

    let mid_regime_enabled =
        get_bool(config, "mid_regime_spread_penalty_enabled", false) || config.contains_key("mid_impulse_lo");
    let mid_impulse_lo = get_f64(config, "mid_impulse_lo", 0.5);
    let mid_impulse_hi = get_f64(config, "mid_impulse_hi", 1.0);
    let tight_spread_bps = get_f64(config, "tight_spread_bps", 3.0);
    let mid_regime_size_mult = get_f64(config, "mid_regime_size_mult", 0.7);

    let s = RebateMMStrategy::new(order_amount, tick_size)
        .with_base_spread(base_spread)
        .with_volatility(vol_lookback, vol_threshold, max_spread_mult)
        .with_inventory_limits(inv_limit, hedge_limit)
        .with_inventory_skew(inv_skew)
        .with_book_imbalance(book_imb_thresh, book_imb_levels)
        .with_microprice_impulse_filter(mp_imp_lookback, mp_imp_thresh, mp_imp_pause)
        .with_microprice_edge_filter(microprice_edge_thresh)
        .with_microprice_edge_size_scale(
            mp_edge_scale,
            mp_edge_scale_k,
            mp_edge_scale_min,
            mp_edge_scale_max,
        )
        .with_microprice_edge_tape_regime(
            mp_tape_regime,
            mp_tape_alpha,
            mp_tape_low,
            mp_tape_high,
            mp_tape_min_fills,
        )
        .with_microprice_edge_tape_upside(
            mp_tape_upside,
            mp_tape_upside_max,
            mp_tape_upside_low,
            mp_tape_upside_high,
            mp_tape_upside_min_fills,
        )
        .with_microprice_edge_tape_upside_use_regime_band(mp_tape_upside_regime_band)
        .with_impulse_phase1_sizing(phase1_sizing)
        .with_impulse_phase1_buckets(phase1_calm, phase1_mid, phase1_min)
        .with_impulse_kill_switch(impulse_kill_thresh);
    let s = if let Some(hybrid_bps) = impulse_kill_hybrid {
        s.with_impulse_kill_hybrid_extreme_bps(hybrid_bps)
    } else {
        s.with_impulse_kill_asymmetric(impulse_kill_asym)
    };
    let s = if impulse_size_skew {
        s.with_impulse_size_skew(true).with_impulse_size_skew_multipliers(impulse_skew_favored, impulse_skew_lean)
    } else {
        s
    };
    let s = if directional_skew {
        s.with_directional_skew(directional_signal, directional_suppress, directional_widen)
    } else {
        s
    };
    let s = if drift_enabled {
        s.with_microprice_drift_filter(drift_lookback, drift_thresh)
    } else {
        s
    };
    let s = s
        .with_microprice_fade(microprice_fade, microprice_fade_min_edge, microprice_fade_ticks)
        .with_microprice_fade_impulse_gate(microprice_fade_impulse_min_bps)
        .with_microprice_fade_impulse_buckets(
            microprice_fade_impulse_bucketed,
            microprice_fade_bucket_lo,
            microprice_fade_bucket_hi,
            microprice_fade_bucket_max_ticks,
        )
        .with_conditional_touch_join(
            conditional_touch_join,
            conditional_touch_max_impulse,
            conditional_touch_max_edge,
        )
        .with_dynamic_conditional_touch(
            dynamic_conditional_touch,
            dynamic_conditional_touch_alpha,
            dynamic_conditional_touch_min_fills,
            dynamic_conditional_touch_enter,
            dynamic_conditional_touch_exit,
        )
        .with_fill_touch_brake(fill_touch_brake, fill_touch_brake_bad, fill_touch_brake_sec);
    let s = if dynamic_spread { s.with_dynamic_spread() } else { s };
    let s = if mid_regime_enabled {
        s.with_mid_regime_spread_penalty(mid_impulse_lo, mid_impulse_hi, tight_spread_bps, mid_regime_size_mult)
    } else {
        s
    };
    let s = s.with_queue_join_touch(queue_join_touch);
    let s = s
        .with_wide_spread_no_quotes(wide_spread_max)
        .with_asymmetric_passive_depth(asym_passive)
        .with_queue_aware_safe_side_touch_join(qj_thresh, Decimal::from_f64_retain(qj_max_touch).unwrap_or(dec!(30)))
        .with_state_dependent_multi_tick_passive(state_max_depth)
        .with_passive_depth_spread_capture_bias(depth_bias)
        .with_quote_stickiness(stick_min_hold, stick_hyst)
        .with_quote_stickiness_depth(stick_depth_delta)
        .with_refresh(refresh)
        .with_warmup(warmup)
        .with_spread_depth_regime(
            spread_depth_regime,
            spread_depth_alpha,
            spread_depth_min_fills,
            spread_depth_bad_ent,
            spread_depth_bad_ex,
            spread_depth_good_ent,
            spread_depth_good_ex,
            spread_depth_ticks_bad,
            spread_depth_ticks_neutral,
            spread_depth_ticks_good,
        )
        .with_spread_depth_participation_floor(
            spread_depth_part_floor,
            spread_depth_part_window,
            spread_depth_part_min,
        )
        .with_spread_depth_probabilistic_touch(
            spread_depth_prob_touch,
            spread_depth_prob_p_good,
            spread_depth_prob_p_neutral,
            spread_depth_prob_p_bad,
        )
        .with_spread_toxicity_brake(
            spread_toxicity_brake,
            spread_toxicity_alpha,
            spread_toxicity_min_fills,
            spread_toxicity_enter,
            spread_toxicity_exit,
            spread_toxicity_touch_mult,
            spread_toxicity_forced_ticks,
        );

    let dyn_sizing = get_bool(config, "dynamic_order_sizing", false);
    if dyn_sizing {
        let oa_f = order_amount.to_f64().unwrap_or(0.1);
        let dmin = Decimal::from_f64_retain(get_f64(
            config,
            "dynamic_order_min",
            (oa_f * 0.2).max(1e-6),
        ))
        .unwrap_or(dec!(0.01));
        let dmax = Decimal::from_f64_retain(get_f64(
            config,
            "dynamic_order_max",
            (oa_f * 4.0).max(oa_f),
        ))
        .unwrap_or(dec!(10.0));
        let maker = get_f64(config, "maker_fee_bps", -0.75);
        s.with_dynamic_order_sizing(
            dmin,
            dmax,
            maker,
            get_f64(config, "dynamic_step_up", 1.1),
            get_f64(config, "dynamic_step_down", 0.85),
            get_f64(config, "dynamic_cooldown_sec", 60.0),
            get_f64(config, "dynamic_edge_alpha_fast", 0.15),
            get_f64(config, "dynamic_edge_alpha_slow", 0.02),
            get_f64(config, "dynamic_edge_deadband_bps", 0.15),
            config
                .get("dynamic_bad_edge_streak")
                .and_then(|v| v.as_u64())
                .unwrap_or(5) as u32,
            get_f64(config, "dynamic_flow_window_sec", 120.0),
            get_f64(config, "dynamic_flow_low_ratio", 0.85),
            get_f64(config, "dynamic_flow_ref_alpha", 0.05),
            config
                .get("dynamic_min_fills_before_resize")
                .and_then(|v| v.as_u64())
                .unwrap_or(30) as u32,
            get_f64(config, "dynamic_flow_ref_min_for_step_up", 0.005),
        )
    } else {
        s
    }
}
