use anyhow::{Context, Result};
use clap::ValueEnum;
use crypto_com_api::{MarketEvent, MarketStreamConfig, PublicTrade, WebSocketClient};
use mm_core_types::{Fill as CoreFill, FillReason, Side};
use mm_engine::{
    BacktestEngine, LatencyModel, MMDashboardSummary, QueueModelConfig, SimpleFeeModel,
};
use rebate_mm::RebateMMStrategy;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use serde::Serialize;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct PairConfig {
    #[allow(dead_code)]
    pair: String,
    order_amount: f64,
    tick_size: f64,
}

pub struct LivePaperConfig {
    pub trading_pair: String,
    pub depth: u32,
    pub dashboard_interval_ms: u64,
    pub queue_depth_pct: f64,
    pub latency_profile: LatencyProfile,
    pub record_trades_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LatencyProfile {
    Disabled,
    Default,
    Colo,
}

impl LatencyProfile {
    fn to_model(self) -> LatencyModel {
        match self {
            Self::Disabled => LatencyModel::disabled(),
            Self::Default => LatencyModel::default(),
            Self::Colo => LatencyModel::colo(),
        }
    }
}

pub async fn run_live_paper(config: LivePaperConfig) -> Result<()> {
    let pair_config = load_pair_config(&config.trading_pair)?;
    let tick_size = Decimal::from_f64_retain(pair_config.tick_size).unwrap_or(dec!(0.01));
    let order_amount = Decimal::from_f64_retain(pair_config.order_amount).unwrap_or(dec!(0.5));

    let strategy = RebateMMStrategy::new(order_amount, tick_size)
        .with_base_spread(3.0)
        .with_volatility(50, 2.0, 4.0)
        .with_inventory_limits(dec!(1.0), dec!(1.0))
        .with_inventory_skew(0.25)
        .with_book_imbalance(0.80, 3)
        .with_microprice_impulse_filter(4, 1.0, 1.0)
        .with_impulse_phase1_sizing(true)
        .with_impulse_phase1_buckets(1.2, 0.8, 0.6)
        .with_mid_regime_spread_penalty(0.6, 0.9, 2.5, 0.8) // NEWBEST24MARCH baseline
        .with_impulse_kill_switch(1.0) // BEST25MARCH — kill threshold sweep winner (asymmetric base)
        .with_impulse_kill_asymmetric(true)
        .with_wide_spread_no_quotes(6.0)
        .with_asymmetric_passive_depth(true)
        .with_queue_aware_safe_side_touch_join(0.45, dec!(30.0))
        .with_state_dependent_multi_tick_passive(2)
        .with_passive_depth_spread_capture_bias(true)
        .with_quote_stickiness(0.25, 1)
        .with_quote_stickiness_depth(2)
        .with_refresh(0.75)
        .with_warmup(30.0);

    let fee_model = SimpleFeeModel::new(dec!(-0.75), dec!(1.5));
    let queue_config = QueueModelConfig {
        touch_queue_pct: config.queue_depth_pct,
        queue_depletion_enabled: true,
        queue_churn_enabled: false,
        crossed_book_fill_enabled: true,
        delta_trade_fraction: 0.5,
        min_delta_for_fill: 0.001,
        queue_turnover_rate_per_sec: 0.5,
        cancel_ahead_fraction: 0.5,
        ..QueueModelConfig::default()
    };

    let mut engine = BacktestEngine::new(strategy, dec!(1000000), dec!(1), fee_model, tick_size)
        .with_queue_config(queue_config)
        .with_latency(config.latency_profile.to_model());

    if let Some(ref root) = config.record_trades_dir {
        let mut simulated_recorder = SimulatedFillRecorder::new(root, &config.trading_pair)?;
        engine = engine.with_fill_callback(move |fill: &CoreFill| {
            simulated_recorder.record(fill);
        });
    }

    let client = WebSocketClient::new().with_reconnect_delay(Duration::from_secs(2));
    let mut stream = client
        .stream_market_data(
            MarketStreamConfig::new(config.trading_pair.clone(), config.depth)
                .with_trades(config.record_trades_dir.is_some()),
        )
        .await?;

    let mut dashboard_interval =
        tokio::time::interval(Duration::from_millis(config.dashboard_interval_ms.max(250)));
    let mut trade_recorder = config
        .record_trades_dir
        .as_ref()
        .map(|root| TradeRecorder::new(root, &config.trading_pair))
        .transpose()?;

    println!("Starting live paper mode for {}", config.trading_pair);
    println!("Latency profile: {:?}", config.latency_profile);
    println!("  trd=trades vol=volume mk=maker qd=queue_dep x=crossed spd=spread reb=rebate adv=adv_sel m1/m5=markout_bps good/neut/toxic=flow tb/ta=toxic_bid/ask ql=quote_life_ms");
    if config.record_trades_dir.is_some() {
        println!(
            "Recording: public tape + simulated fills -> {:?}",
            config
                .record_trades_dir
                .as_ref()
                .unwrap()
                .join(&config.trading_pair)
        );
    }
    println!("Press Ctrl+C to stop.");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nStopping live paper mode.");
                break;
            }
            _ = dashboard_interval.tick() => {
                engine.record_live_snapshot();
                if let Some(pv) = engine.current_portfolio_value() {
                    let dashboard = engine.current_dashboard_summary();
                    print_dashboard(&config.trading_pair, engine.snapshot_count(), pv, &dashboard);
                }
            }
            maybe_event = stream.recv() => {
                match maybe_event {
                    Some(Ok(MarketEvent::Book(snapshot))) => {
                        engine.process_snapshot(snapshot);
                    }
                    Some(Ok(MarketEvent::Trade(trade))) => {
                        if let Some(recorder) = trade_recorder.as_mut() {
                            recorder.record(&trade)?;
                        }
                    }
                    Some(Err(err)) => {
                        eprintln!("[live-paper] websocket warning: {err}");
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn load_pair_config(pair: &str) -> Result<PairConfig> {
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
                .with_context(|| format!("Failed to read {:?}", path))?;
            return serde_yaml::from_str(&contents)
                .with_context(|| format!("Failed to parse {:?}", path));
        }
    }
    Err(anyhow::anyhow!(
        "Config not found for {}. Create configs/{}",
        pair,
        config_name
    ))
}

fn format_volume(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("${:.1}M", v / 1_000_000.0)
    } else if v >= 1_000.0 {
        format!("${:.1}K", v / 1_000.0)
    } else {
        format!("${:.2}", v)
    }
}

fn format_pv_with_commas(v: f64) -> String {
    let s = format!("{:.0}", v.abs());
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len();
    for (i, c) in chars.into_iter().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    let formatted = if v < 0.0 {
        format!("-{}", result)
    } else {
        result
    };
    format!("${}", formatted)
}

fn print_dashboard(
    trading_pair: &str,
    snapshot_count: u64,
    portfolio_value: Decimal,
    dashboard: &MMDashboardSummary,
) {
    let pv = portfolio_value.to_f64().unwrap_or(0.0);
    println!(
        "[live:{}] snaps={} pv={} trd={} vol={} fill={:.1}% mk={:.0}% qd={:.0}% x={:.0}% edge={:+.2} spd={:+.2} reb={:+.2} adv={:+.2} m1={:+.2} m5={:+.2} good={:.0}% neut={:.0}% toxic={:.0}% tb={:.0}% ta={:.0}% inv={:.2} ql={:.0}ms",
        trading_pair,
        snapshot_count,
        format_pv_with_commas(pv),
        dashboard.fill_count,
        format_volume(dashboard.total_volume),
        dashboard.fill_rate_pct,
        dashboard.maker_ratio_pct,
        dashboard.queue_depletion_fill_pct,
        dashboard.crossed_book_fill_pct,
        dashboard.net_edge_bps,
        dashboard.realized_spread_capture_bps,
        dashboard.rebate_earned_bps,
        dashboard.adverse_selection_1s_bps,
        dashboard.markout_1s_bps,
        dashboard.markout_5s_bps,
        dashboard.good_fill_pct,
        dashboard.neutral_fill_pct,
        dashboard.toxic_fill_pct,
        dashboard.toxic_bid_pct,
        dashboard.toxic_ask_pct,
        dashboard.avg_inventory,
        dashboard.avg_quote_lifetime_ms,
    );
}

/// Record for tape comparison: simulated fill written to JSONL.
#[derive(Debug, Serialize)]
struct SimulatedFillRecord {
    ts: f64,
    side: String,
    price: f64,
    amount: f64,
    order_id: String,
    fill_reason: Option<String>,
}

struct SimulatedFillRecorder {
    file: File,
}

impl SimulatedFillRecorder {
    fn new(root: &Path, trading_pair: &str) -> Result<Self> {
        let day = chrono_like_day_string();
        let dir = root.join(trading_pair);
        fs::create_dir_all(&dir).with_context(|| format!("Failed to create dir {:?}", dir))?;
        let path = dir.join(format!("simulated_{day}.jsonl"));
        let file = File::options()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to open {:?}", path))?;
        Ok(Self { file })
    }

    fn record(&mut self, fill: &CoreFill) {
        let fill_reason = fill.fill_reason.map(|r| match r {
            FillReason::QueueDepletion => "QueueDepletion".to_string(),
            FillReason::CrossedBook => "CrossedBook".to_string(),
        });
        let record = SimulatedFillRecord {
            ts: fill.timestamp,
            side: match fill.side {
                Side::Buy => "BUY".to_string(),
                Side::Sell => "SELL".to_string(),
            },
            price: fill.price.to_f64().unwrap_or(0.0),
            amount: fill.amount.to_f64().unwrap_or(0.0),
            order_id: fill.order_id.clone(),
            fill_reason,
        };
        if let Ok(line) = serde_json::to_string(&record) {
            let _ = writeln!(self.file, "{line}");
            let _ = self.file.flush();
        }
    }
}

struct TradeRecorder {
    file: File,
}

impl TradeRecorder {
    fn new(root: &Path, trading_pair: &str) -> Result<Self> {
        let day = chrono_like_day_string();
        let dir = root.join(trading_pair);
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create trade recorder directory {:?}", dir))?;
        let path = dir.join(format!("{day}.jsonl"));
        let file = File::options()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to open trade recorder file {:?}", path))?;
        Ok(Self { file })
    }

    fn record(&mut self, trade: &PublicTrade) -> Result<()> {
        let line = serde_json::to_string(trade)?;
        writeln!(self.file, "{line}")?;
        self.file.flush()?;
        Ok(())
    }
}

fn chrono_like_day_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default();
    chrono::DateTime::<chrono::Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
