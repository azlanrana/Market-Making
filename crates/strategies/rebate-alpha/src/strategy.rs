//! Rebate-Alpha strategy: "Liquidity Sniper" Edition v2.6 (two-layer, ETH/USDT)
//!
//! Optimized for $1M - $6M AUM and -0.75 bps rebate environments.
//! **v2.6**: L1 (maker, 0.1×, 15–20 bps) + L0 scratch (optional, disabled by default).
//! Scratch disabled = L1-only: positions run off via L1 quotes on opposite side.
//! - **Scratch queue depth** (max 5); **trip history flushed** on layer re-enable
//! - **Vol sampled at 1s** (not tick-by-tick) to avoid choppy overstatement
//! - **buy/sell_allowed**: strict >/< when micro==mid (both false at equilibrium)
//! - **Asymmetric Toxic Gates** + **Toxicity Memory**; **Orderbook Imbalance** ±6 bps
use chrono::TimeZone;
use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::strategy::{Fill, OrderIntent, OrderType, Strategy, StrategyError};
use mm_core::Portfolio;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Clone, PartialEq)]
enum Regime {
    Ranging,
    HighVolRange,
    Trending,
}

/// REBATE-AGGRESSIVE, ALPHA-CONSERVATIVE STRATEGY
#[allow(dead_code)]
pub struct RebateAlphaStrategy {
    order_amount: Decimal,
    base_layer_spreads_bps: Vec<Decimal>,
    // Order tracking
    active_bid_orders: HashMap<u32, Option<String>>,
    active_ask_orders: HashMap<u32, Option<String>>,
    last_bid_fill_times: HashMap<u32, Option<f64>>,
    last_ask_fill_times: HashMap<u32, Option<f64>>,
    // Timers
    last_refresh_time: f64,
    order_refresh_time: f64,
    filled_order_delay: f64,
    // Alpha: Micro-price & Volatility for adaptive scalar
    last_micro: Decimal,
    price_history: VecDeque<(f64, f64)>,
    vol_lookback_sec: f64,
    vol_threshold_bps: f64,
    current_vol_bps: f64,
    // Inventory & Skew
    target_inventory_pct: f64,
    base_target_inventory_pct: f64, // Neutral target (50%)
    crash_target_pct: f64,          // Lean short in downtrend (30%)
    trend_lookback_sec: f64,        // 5-min for Dynamic Beta
    hard_limit_pct: f64,
    max_skew_bps: f64,
    inventory_sensitivity: f64,
    // Rebate Engine
    // (side, price, amount, created_ts, source_layer, initial_price, entry_regime, entry_trend_bps, entry_vol_bps)
    pending_scratches: VecDeque<(
        OrderSide,
        Decimal,
        Decimal,
        f64,
        u32,
        Decimal,
        String,
        f64,
        f64,
    )>,
    pingpong_spread_bps: f64,
    maker_fee_bps: f64,
    scratch_timeout_sec: f64,
    scratch_drift_kill_bps: f64,
    scratch_placed: bool,
    scratch_taker_sec: f64, // After this, force Market (taker) scratch
    // Expectancy gate: (net_pnl, volume) - net_pnl includes rebate; expectancy_bps = net_pnl/vol*10000
    layer_round_trips: HashMap<u32, VecDeque<(f64, f64)>>,
    layer_disabled_until: HashMap<u32, f64>,
    expectancy_threshold_bps: f64,
    layer_disable_sec: f64,
    layer_expectancy_penalty: HashMap<u32, Decimal>, // Shame multiplier per layer (1.0..3.0)
    // Hysteresis-Smoothing: 3-sample drift + asymmetric cooling
    drift_history: VecDeque<Decimal>,
    last_buy_gate_block_ts: f64,
    last_sell_gate_block_ts: f64,
    cooling_buy_sec: f64,
    cooling_sell_sec: f64,
    // Toxicity memory: decayed average for gentler spread modulation
    toxicity_score: f64,
    // Scratch: avoid message storms at 0.5s refresh
    last_scratch_refresh_ts: f64,
    min_scratch_refresh_sec: f64,
    max_scratch_queue_depth: usize,
    // Vol: sampled at intervals to avoid tick-by-tick overstatement
    last_vol_sample_ts: f64,
    vol_sample_interval_sec: f64,
    // Cross-Chaser: for backtest only - tighten spreads by N% to force more crossed fills
    // (Simulator only fills when crossed; 0.8 = 20% tighter to stress-test gates)
    backtest_spread_multiplier: f64,
    // Regime: Ranging / HighVolRange / Trending — block buys in crash, sells in pump
    trend_crash_threshold_bps: f64,
    trend_pump_threshold_bps: f64,
    high_vol_threshold_bps: f64,
    last_trending_first_log_ts: f64, // 0 = not yet logged first Trending this session
    last_logged_inv_pct: f64,        // For inv rebalance log (5% bands)
    last_regime_str: String,         // For scratch log (regime when fill occurs)
    last_logged_day: Option<String>, // For [DAY_START] log
    last_regime_sample_ts: f64, // For [REGIME_SAMPLE] once per 60s — compare Dec 10/13 vs losing days
    // Impulse detection: short-term momentum over 5-15s (not 5-min trend)
    impulse_price_buffer: VecDeque<(f64, f64)>, // (ts, mid) for last 20s
    impulse_cooldown_until: f64,                // Don't quote until this ts
    last_trend_market_reduce_ts: f64,           // Throttle partial market reduce
}
impl RebateAlphaStrategy {
    /// Create strategy tuned for rebate farming (legacy Nuclear Fade)
    pub fn new_for_rebate_farming(order_amount: Decimal, maker_rebate_bps: f64) -> Self {
        Self::new_mid_hft(order_amount, maker_rebate_bps) // Delegate to adaptive version
    }
    /// Adaptive Mid-HFT: volatility scalar, momentum gate, adaptive scratch
    pub fn new_mid_hft(order_amount: Decimal, maker_rebate_bps: f64) -> Self {
        let mut active_bid = HashMap::new();
        let mut active_ask = HashMap::new();
        let mut last_bid_t = HashMap::new();
        let mut last_ask_t = HashMap::new();
        for layer in 1..=1 {
            active_bid.insert(layer, None);
            active_ask.insert(layer, None);
            last_bid_t.insert(layer, None);
            last_ask_t.insert(layer, None);
        }
        Self {
            order_amount,
            // L1 dynamic (20-30 bps from vol); L2/L3 fixed
            base_layer_spreads_bps: vec![dec!(25.0)], // Single layer: L1 only
            active_bid_orders: active_bid,
            active_ask_orders: active_ask,
            last_bid_fill_times: last_bid_t,
            last_ask_fill_times: last_ask_t,
            last_refresh_time: 0.0,
            order_refresh_time: 0.5,  // 0.5s — ETH moves fast; 2s was stale
            filled_order_delay: 15.0, // 15s cooldown — catch mean-reversion bounces
            last_micro: dec!(0),
            price_history: VecDeque::new(),
            vol_lookback_sec: 180.0, // 3 min sliding window
            vol_threshold_bps: 20.0, // Base volatility target
            current_vol_bps: 0.0,
            target_inventory_pct: 0.5,
            base_target_inventory_pct: 0.5, // Neutral in stability
            crash_target_pct: 0.30,         // Lean short in crash
            trend_lookback_sec: 300.0,      // 5-min trend for Dynamic Beta
            hard_limit_pct: 0.60,           // 40–60% band (max base 60%, min 40%)
            max_skew_bps: 120.0,
            inventory_sensitivity: 18.0, // Stronger skew when inv > 25% — inventory mgmt > vol filtering
            pending_scratches: VecDeque::new(),
            pingpong_spread_bps: 0.0,
            maker_fee_bps: maker_rebate_bps,
            scratch_timeout_sec: 25.0,
            scratch_drift_kill_bps: 3.0,
            scratch_placed: false,
            scratch_taker_sec: 90.0, // Parachute: only when OLD + drifted + Danger Zone (|inv|>80%)
            layer_round_trips: HashMap::new(),
            layer_disabled_until: HashMap::new(),
            expectancy_threshold_bps: 0.2, // Lower threshold: widen aggressively when bleeding rebates
            layer_disable_sec: 300.0,      // Disable for 5 minutes
            layer_expectancy_penalty: HashMap::new(),
            drift_history: VecDeque::new(),
            last_buy_gate_block_ts: 0.0,
            last_sell_gate_block_ts: 0.0,
            cooling_buy_sec: 1.0,  // Dips revert faster
            cooling_sell_sec: 3.0, // Squeezes are faster
            toxicity_score: 0.0,
            last_scratch_refresh_ts: 0.0,
            min_scratch_refresh_sec: 2.0, // Avoid message storms at 0.5s refresh
            max_scratch_queue_depth: 0,   // 0 = scratch disabled (L1-only); 5 = scratch enabled
            last_vol_sample_ts: 0.0,
            vol_sample_interval_sec: 1.0, // Sample at 1s to avoid choppy overstatement
            backtest_spread_multiplier: 1.0, // Live: no change. Use with_backtest_mode() for 20% tighter
            trend_crash_threshold_bps: -60.0,
            trend_pump_threshold_bps: 80.0,
            high_vol_threshold_bps: 3.0, // Per-tick vol scale
            last_trending_first_log_ts: 0.0,
            last_logged_inv_pct: 0.5,
            last_regime_str: String::new(),
            last_logged_day: None,
            last_regime_sample_ts: 0.0,
            impulse_price_buffer: VecDeque::new(),
            impulse_cooldown_until: 0.0,
            last_trend_market_reduce_ts: 0.0,
        }
    }
    /// Enable Cross-Chaser mode for backtest: tighten spreads by 20% to force more
    /// crossed fills (simulator only fills when market crosses our price).
    /// Call when running backtests to stress-test the gates.
    pub fn with_backtest_mode(mut self) -> Self {
        self.backtest_spread_multiplier = 0.8;
        self
    }
    /// Re-enable the scratch engine (default is disabled, L1-only). Pass 5 for typical depth.
    pub fn with_scratch_enabled(mut self, queue_depth: usize) -> Self {
        self.max_scratch_queue_depth = queue_depth;
        self
    }
    /// Returns expectancy in bps (net P&L per dollar * 10000). Single convention.
    fn get_layer_expectancy_bps(&self, layer: u32) -> f64 {
        let trips = match self.layer_round_trips.get(&layer) {
            Some(t) if t.len() >= 5 => t,
            _ => return 0.0,
        };
        let total_net_pnl: f64 = trips.iter().map(|(p, _)| *p).sum();
        let total_vol: f64 = trips.iter().map(|(_, v)| *v).sum();
        if total_vol <= 0.0 {
            0.0
        } else {
            (total_net_pnl / total_vol) * 10000.0 // bps
        }
    }
    fn record_layer_round_trip(&mut self, layer: u32, gross_pnl: f64, volume: f64) {
        // Dynamic rebate: maker_fee_bps per leg (e.g. 0.75 * 2 = 1.5 bps round-trip)
        let rebate_bps = self.maker_fee_bps * 2.0;
        let total_rebate = volume * (rebate_bps / 10000.0);
        let net_pnl = gross_pnl + total_rebate;
        let trips = self
            .layer_round_trips
            .entry(layer)
            .or_insert_with(VecDeque::new);
        trips.push_back((net_pnl, volume));
        while trips.len() > 100 {
            trips.pop_front();
        }
        // Shame multiplier: slower adjustments (1.05 / 0.98) for stability
        let expectancy = if volume > 0.0 { net_pnl / volume } else { 0.0 };
        let entry = self
            .layer_expectancy_penalty
            .entry(layer)
            .or_insert(dec!(1.0));
        if expectancy < 0.0 {
            *entry = (*entry * dec!(1.05)).min(dec!(3.0));
        } else {
            *entry = (*entry * dec!(0.98)).max(dec!(1.0));
        }
    }
    fn calculate_volatility(&mut self, timestamp: f64, mid_price: f64) {
        // Sample at intervals to avoid tick-by-tick overstatement in choppy markets
        if self.last_vol_sample_ts > 0.0
            && timestamp - self.last_vol_sample_ts < self.vol_sample_interval_sec
        {
            return;
        }
        self.last_vol_sample_ts = timestamp;
        self.price_history.push_back((timestamp, mid_price));
        // Keep data for both vol (180s) and trend (300s) — use longer retention
        let retain_sec = self.vol_lookback_sec.max(self.trend_lookback_sec);
        let cutoff = timestamp - retain_sec;
        while self
            .price_history
            .front()
            .map_or(false, |(t, _)| *t < cutoff)
        {
            self.price_history.pop_front();
        }
        if self.price_history.len() < 10 {
            return;
        }
        let prices: Vec<f64> = self.price_history.iter().map(|(_, p)| *p).collect();
        let mut returns = Vec::new();
        for i in 1..prices.len() {
            if prices[i - 1] > 0.0 {
                returns.push((prices[i] - prices[i - 1]) / prices[i - 1]);
            }
        }
        if returns.len() < 5 {
            return;
        }
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        self.current_vol_bps = (variance.sqrt() * 10000.0).abs();
    }
    /// Dynamic Beta: Short-term price trend (5-min). Returns bps return.
    /// Positive = price up, negative = price down. Used to lean short in crashes.
    /// Uses oldest available price when < 300s of history (e.g. first 5 min of backtest).
    fn calculate_short_term_trend(&self, current_price: f64, timestamp: f64) -> f64 {
        let cutoff = timestamp - self.trend_lookback_sec;
        // Prefer price from full 300s lookback; fallback to oldest available (handles first 5 min)
        let old_price = self
            .price_history
            .iter()
            .find(|(t, _)| *t >= cutoff)
            .or_else(|| self.price_history.front())
            .map(|(_, p)| *p);
        match old_price {
            Some(p) if p > 0.0 => ((current_price - p) / p) * 10000.0, // bps
            _ => 0.0,
        }
    }
    fn detect_regime(&self, trend_bps: f64) -> Regime {
        let trending_down = trend_bps < self.trend_crash_threshold_bps;
        let trending_up = trend_bps > self.trend_pump_threshold_bps;
        let high_vol = self.current_vol_bps > self.high_vol_threshold_bps;

        if (trending_down || trending_up) && high_vol {
            Regime::Trending
        } else if high_vol {
            Regime::HighVolRange
        } else {
            Regime::Ranging
        }
    }
    /// ALPHA: Calculate Micro-Price to avoid being sniped by toxic L1 flow
    fn calculate_micro_price(&self, orderbook: &OrderBook) -> Option<Decimal> {
        let (best_bid, bid_qty) = orderbook.bids.first().map(|(p, q)| (*p, *q))?;
        let (best_ask, ask_qty) = orderbook.asks.first().map(|(p, q)| (*p, *q))?;
        let total_qty = bid_qty + ask_qty;
        if total_qty <= Decimal::ZERO {
            return Some((best_bid + best_ask) / dec!(2));
        }
        Some((best_bid * ask_qty + best_ask * bid_qty) / total_qty)
    }
    /// Orderbook imbalance skew in bps: bid-heavy -> positive (lean up), ask-heavy -> negative (lean down).
    /// Applied on every OB update to front-run quotes toward likely next tick.
    fn orderbook_imbalance_skew_bps(&self, orderbook: &OrderBook) -> f64 {
        let (bid_qty, ask_qty) = match (
            orderbook.bids.first().map(|(_, q)| *q),
            orderbook.asks.first().map(|(_, q)| *q),
        ) {
            (Some(b), Some(a)) => (b, a),
            _ => return 0.0,
        };
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return 0.0;
        }
        let imbalance = (bid_qty - ask_qty) / total; // -1 to 1
        let imb_f64 = imbalance.to_f64().unwrap_or(0.0);
        // Max ±6 bps skew when fully imbalanced (stronger bias without inventory blowouts)
        (imb_f64 * 6.0).clamp(-6.0, 6.0)
    }
}
impl Strategy for RebateAlphaStrategy {
    fn name(&self) -> &str {
        "adaptive_mid_hft"
    }
    fn on_orderbook_update(
        &mut self,
        ob: &OrderBook,
        port: &Portfolio,
        ts: f64,
    ) -> Vec<OrderIntent> {
        let mut intents = Vec::new();
        let current_micro = match self.calculate_micro_price(ob) {
            Some(m) => m,
            None => return intents,
        };
        let mid_price = ob.mid_price().unwrap_or(current_micro);
        let mid_f64 = mid_price.to_f64().unwrap_or(0.0);
        // [DAY_START] log at beginning of each new day to correlate timestamps with files
        if let Some(dt) = chrono::Utc.timestamp_opt(ts as i64, 0).single() {
            let day_str = dt.format("%Y-%m-%d").to_string();
            if self.last_logged_day.as_ref() != Some(&day_str) {
                eprintln!("[DAY_START] ts={:.0} day={}", ts, day_str);
                self.last_logged_day = Some(day_str);
            }
        }
        self.calculate_volatility(ts, mid_f64);
        // --- IMPULSE DETECTION: short-term momentum (5-15s), not 5-min trend ---
        self.impulse_price_buffer.push_back((ts, mid_f64));
        let impulse_retain = 20.0;
        while self
            .impulse_price_buffer
            .front()
            .map_or(false, |(t, _)| *t < ts - impulse_retain)
        {
            self.impulse_price_buffer.pop_front();
        }
        let impulse_cooldown_sec = 5.0;
        if ts < self.impulse_cooldown_until {
            intents.push(OrderIntent {
                side: OrderSide::Buy,
                price: Decimal::ZERO,
                amount: Decimal::ZERO,
                order_type: OrderType::Cancel,
                layer: 0,
            });
            intents.push(OrderIntent {
                side: OrderSide::Sell,
                price: Decimal::ZERO,
                amount: Decimal::ZERO,
                order_type: OrderType::Cancel,
                layer: 0,
            });
            intents.push(OrderIntent {
                side: OrderSide::Buy,
                price: Decimal::ZERO,
                amount: Decimal::ZERO,
                order_type: OrderType::Cancel,
                layer: 1,
            });
            intents.push(OrderIntent {
                side: OrderSide::Sell,
                price: Decimal::ZERO,
                amount: Decimal::ZERO,
                order_type: OrderType::Cancel,
                layer: 1,
            });
            self.pending_scratches.clear();
            self.scratch_placed = false;
            return intents;
        }
        if self.impulse_price_buffer.len() >= 3 {
            let price_3s = self
                .impulse_price_buffer
                .iter()
                .filter(|(t, _)| *t <= ts - 2.0)
                .last()
                .map(|(_, p)| *p);
            let price_5s = self
                .impulse_price_buffer
                .iter()
                .filter(|(t, _)| *t <= ts - 4.0)
                .last()
                .map(|(_, p)| *p);
            let price_10s = self
                .impulse_price_buffer
                .iter()
                .filter(|(t, _)| *t <= ts - 9.0)
                .last()
                .map(|(_, p)| *p);
            let ret_bps = |old: f64| ((mid_f64 - old) / old) * 10000.0;
            let triggered = price_10s.map_or(false, |p| ret_bps(p).abs() > 15.0)
                || price_5s.map_or(false, |p| ret_bps(p).abs() > 8.0)
                || price_3s.map_or(false, |p| ret_bps(p).abs() > 6.0);
            if triggered {
                self.impulse_cooldown_until = ts + impulse_cooldown_sec;
                intents.push(OrderIntent {
                    side: OrderSide::Buy,
                    price: Decimal::ZERO,
                    amount: Decimal::ZERO,
                    order_type: OrderType::Cancel,
                    layer: 0,
                });
                intents.push(OrderIntent {
                    side: OrderSide::Sell,
                    price: Decimal::ZERO,
                    amount: Decimal::ZERO,
                    order_type: OrderType::Cancel,
                    layer: 0,
                });
                intents.push(OrderIntent {
                    side: OrderSide::Buy,
                    price: Decimal::ZERO,
                    amount: Decimal::ZERO,
                    order_type: OrderType::Cancel,
                    layer: 1,
                });
                intents.push(OrderIntent {
                    side: OrderSide::Sell,
                    price: Decimal::ZERO,
                    amount: Decimal::ZERO,
                    order_type: OrderType::Cancel,
                    layer: 1,
                });
                self.pending_scratches.clear();
                self.scratch_placed = false;
                return intents;
            }
        }
        // --- 1. 3-SAMPLE DRIFT SMOOTHING ---
        let raw_drift = if self.last_micro > dec!(0) {
            ((current_micro - self.last_micro).abs() / self.last_micro) * dec!(10000)
        } else {
            dec!(0)
        };
        self.last_micro = current_micro;
        self.drift_history.push_back(raw_drift);
        if self.drift_history.len() > 3 {
            self.drift_history.pop_front();
        }
        let avg_drift = if self.drift_history.is_empty() {
            dec!(0)
        } else {
            self.drift_history.iter().sum::<Decimal>()
                / Decimal::from(self.drift_history.len() as i64)
        };
        // --- 2. ASYMMETRIC HYSTERESIS + TOXICITY MEMORY ---
        // Block buys when price is crashing (micro below mid); block sells when pumping (micro above mid)
        let buying_toxic = avg_drift > dec!(2.0) && current_micro < mid_price;
        let selling_toxic = avg_drift > dec!(2.0) && current_micro > mid_price;
        if buying_toxic {
            self.last_buy_gate_block_ts = ts;
        }
        if selling_toxic {
            self.last_sell_gate_block_ts = ts;
        }
        // Toxicity memory: decayed average for gentler spread modulation
        let drift_f64 = avg_drift.to_f64().unwrap_or(0.0);
        let decay = 0.9;
        self.toxicity_score =
            decay * self.toxicity_score + (1.0 - decay) * if drift_f64 > 2.5 { 1.0 } else { 0.0 };
        // Side-specific cooling: dips revert faster (1s), squeezes persist (3s)
        let buy_is_cooling = self.last_buy_gate_block_ts > 0.0
            && (ts - self.last_buy_gate_block_ts) < self.cooling_buy_sec;
        let sell_is_cooling = self.last_sell_gate_block_ts > 0.0
            && (ts - self.last_sell_gate_block_ts) < self.cooling_sell_sec;
        let needs_refresh = self.last_refresh_time == 0.0
            || (ts - self.last_refresh_time) >= self.order_refresh_time;
        // --- 3. VOL-SQUARED: Exponential spread scaling in high vol (3× cap for ETH perps) ---
        let capped_vol = self.current_vol_bps.max(10.0);
        let vol_ratio = capped_vol / self.vol_threshold_bps.max(0.01);
        let vol_factor = (vol_ratio * vol_ratio).clamp(1.0, 3.0); // Squared, max 3× (was 6×)
                                                                  // --- 3a. Inventory % (needed for scratch danger zone and placement) ---
        let base_value = port.base_balance * mid_price;
        let total_value = base_value + port.quote_balance;
        // Clamp to [0,1] — raw calc can go negative when short (base < 0), breaking gates
        let inv_pct = if total_value > Decimal::ZERO {
            (base_value / total_value)
                .to_f64()
                .unwrap_or(0.5)
                .clamp(0.0, 1.0)
        } else {
            0.5
        };
        // --- v2.6: LAYER-0 SCRATCH — rebalancing-only; taker only when directionally skewed ---
        // Direction-aware: Sell scratch → taker only when dangerously long (inv>60%)
        //                  Buy scratch  → taker only when dangerously short (inv<40%)
        self.scratch_timeout_sec = (25.0 * vol_factor.sqrt()).clamp(20.0, 60.0);
        if let Some(&(side, price, amount, created_ts, _source_layer, _initial_price, _, _, _)) =
            self.pending_scratches.front()
        {
            let in_danger_zone = match side {
                OrderSide::Sell => inv_pct > 0.60, // Taker exit only when dangerously long
                OrderSide::Buy => inv_pct < 0.40,  // Taker exit only when dangerously short
            };
            let price_f64 = price.to_f64().unwrap_or(0.0);
            let age_sec = ts - created_ts;
            let drift_bps = if mid_f64 > 0.0 {
                ((mid_f64 - price_f64).abs() / mid_f64) * 10000.0
            } else {
                0.0
            };
            // v2.6: Taker-only when in danger zone — cross mid immediately, no limit scratch
            if in_danger_zone {
                if needs_refresh || !self.scratch_placed {
                    self.scratch_placed = true;
                    self.last_scratch_refresh_ts = ts;
                    intents.push(OrderIntent {
                        side,
                        price: mid_price,
                        amount,
                        order_type: OrderType::Market,
                        layer: 0,
                    });
                }
            } else if age_sec > self.scratch_timeout_sec || drift_bps > self.scratch_drift_kill_bps
            {
                self.pending_scratches.pop_front();
                self.scratch_placed = false;
                intents.push(OrderIntent {
                    side,
                    price: Decimal::ZERO,
                    amount: Decimal::ZERO,
                    order_type: OrderType::Cancel,
                    layer: 0,
                });
            } else if (needs_refresh || !self.scratch_placed)
                && (self.last_scratch_refresh_ts == 0.0
                    || ts - self.last_scratch_refresh_ts >= self.min_scratch_refresh_sec)
            {
                self.scratch_placed = true;
                self.last_scratch_refresh_ts = ts;
                intents.push(OrderIntent {
                    side,
                    price,
                    amount,
                    order_type: OrderType::Limit,
                    layer: 0,
                });
            }
        }
        // --- 3c. SMOOTH DYNAMIC BETA: tanh avoids oscillation near neutral ---
        let trend_bps = self.calculate_short_term_trend(mid_f64, ts);
        let regime = self.detect_regime(trend_bps);

        // First Trending trigger: when filter catches crash/pump (avoids spam, logs once per ~10s)
        if regime == Regime::Trending && self.last_trending_first_log_ts < ts - 10.0 {
            eprintln!(
                "[TRENDING_FIRST] ts={:.0} trend={:.1} vol={:.2} inv_pct={:.2}%",
                ts,
                trend_bps,
                self.current_vol_bps,
                inv_pct * 100.0
            );
            self.last_trending_first_log_ts = ts;
        }
        self.last_regime_str = format!("{:?}", regime);
        // [REGIME_SAMPLE] once per 60s by day — grep and aggregate to compare Dec 10/13 vs losing days
        if ts - self.last_regime_sample_ts >= 60.0 {
            self.last_regime_sample_ts = ts;
            if let Some(dt) = chrono::Utc.timestamp_opt(ts as i64, 0).single() {
                let day_str = dt.format("%Y-%m-%d").to_string();
                eprintln!("[REGIME_SAMPLE] day={} regime={:?} vol_bps={:.2} trend_bps={:.1} inv_pct={:.1}%",
            day_str, regime, self.current_vol_bps, trend_bps, inv_pct * 100.0);
            }
        }
        // Inventory rebalance log: when crossing 5% bands (are we ever getting back to 50%?)
        if (inv_pct - self.last_logged_inv_pct).abs() > 0.05 {
            eprintln!(
                "[INV] ts={:.0} inv={:.1}% regime={} trend={:.1}",
                ts,
                inv_pct * 100.0,
                self.last_regime_str,
                trend_bps
            );
            self.last_logged_inv_pct = inv_pct;
        }

        // Regime-based spread multiplier
        let regime_spread_mult = match regime {
            Regime::Ranging => 1.0,
            Regime::HighVolRange => 1.5,
            Regime::Trending => 2.0,
        };

        // Regime-based direction block
        let regime_block_buys = regime == Regime::Trending && trend_bps < 0.0;
        let regime_block_sells = regime == Regime::Trending && trend_bps > 0.0;
        // Cancel pending scratches when entering Trending
        if regime == Regime::Trending && !self.pending_scratches.is_empty() {
            if let Some(&(side, _, _, _, _, _, _, _, _)) = self.pending_scratches.front() {
                intents.push(OrderIntent {
                    side,
                    price: Decimal::ZERO,
                    amount: Decimal::ZERO,
                    order_type: OrderType::Cancel,
                    layer: 0,
                });
            }
            self.pending_scratches.clear();
            self.scratch_placed = false;
        }
        // Trend flatten DISABLED: amplifies inventory shocks on ETH/USDT; use spread-dominated MM
        // (Re-enable only when expectancy clearly positive)
        let trend_flatten_ask = false;
        let trend_flatten_bid = false;
        let ask_regime_mult = regime_spread_mult;
        let bid_regime_mult = regime_spread_mult;

        // v2.5: Target near 50% unless trend strongly away — no crash-target drift to 30%
        let neutral_target = 0.50;
        let max_drift = 0.05; // 45–55%
        let scaled_trend = (trend_bps / 120.0).clamp(-1.0, 1.0);
        self.target_inventory_pct = neutral_target + max_drift * scaled_trend;
        // --- 4. CORE PLACEMENT (effective_mid with inventory + orderbook imbalance skew) ---
        let skew = ((inv_pct - self.target_inventory_pct) * self.inventory_sensitivity * 100.0)
            .clamp(-self.max_skew_bps, self.max_skew_bps);
        let skew_dec = Decimal::from_f64_retain(skew).unwrap_or(Decimal::ZERO) / dec!(10000);
        let imb_bps = self.orderbook_imbalance_skew_bps(ob);
        let imb_dec = Decimal::from_f64_retain(imb_bps).unwrap_or(Decimal::ZERO) / dec!(10000);
        // Micro-price: inventory skew + imbalance skew (front-run on every OB update)
        let effective_mid = current_micro * (dec!(1) - skew_dec + imb_dec);
        // Asymmetric gates; at micro==mid use inventory tie-breaker (avoid both true simultaneously)
        let drift_ok_to_reopen = avg_drift < dec!(10) / dec!(10); // 1.0 bps
        let buy_base =
            current_micro >= mid_price && !buying_toxic && (!buy_is_cooling || drift_ok_to_reopen);
        let sell_base = current_micro <= mid_price
            && !selling_toxic
            && (!sell_is_cooling || drift_ok_to_reopen);
        let both_would_allow = buy_base && sell_base;
        let mut buy_allowed =
            buy_base && !(both_would_allow && inv_pct >= 0.5) && !regime_block_buys;
        let mut sell_allowed =
            sell_base && !(both_would_allow && inv_pct < 0.5) && !regime_block_sells;
        // --- v2.6: HARD INVENTORY GOVERNANCE — no creep into 49.6% base ---
        if inv_pct > 0.60 {
            buy_allowed = false; // Long: don't buy more, prefer sell/flatten
        }
        if inv_pct < 0.40 {
            sell_allowed = false; // Short: don't sell more, prefer buy/flatten
        }
        // --- 5. L1 GATE: only enable side that trades toward target (target now 45–55%) ---
        let l1_bid_needed = inv_pct < self.target_inventory_pct;
        let l1_ask_needed = inv_pct > self.target_inventory_pct;
        // --- 6. EXPECTANCY: flush trip history on re-enable; L1 penalize-only; L2/L3 full disable ---
        let to_reenable: Vec<u32> = self
            .layer_disabled_until
            .iter()
            .filter(|(_, &until)| ts >= until)
            .map(|(&layer, _)| layer)
            .collect();
        for layer in &to_reenable {
            self.layer_disabled_until.remove(layer);
            self.layer_round_trips.remove(layer);
            self.layer_expectancy_penalty.insert(*layer, dec!(1.0)); // Reset penalty so stale 2× doesn't re-trigger
        }
        for layer in 1..=1 {
            let expectancy_bps = self.get_layer_expectancy_bps(layer);
            if expectancy_bps < -self.expectancy_threshold_bps {
                // Single layer: widen by penalty — sharper 1.2× ramp when bleeding
                let entry = self
                    .layer_expectancy_penalty
                    .entry(layer)
                    .or_insert(dec!(1.0));
                *entry = (*entry * dec!(12) / dec!(10)).min(dec!(3.0)); // 20% wider per bad epoch, max 3×
            }
        }
        if needs_refresh {
            // market_reduce DISABLED: amplifies inventory shocks; use limit-only spread MM
            for (layer_idx, base_spread) in self.base_layer_spreads_bps.iter().enumerate() {
                let layer = (layer_idx + 1) as u32;
                // --- v2.6: L1 tiny (0.1×) pure rebate capture; L0 = 0.3× mean-reverting scratch ---
                let base_layer_size = if layer == 1 {
                    self.order_amount * dec!(1) / dec!(10) // L1: 0.1× — maker-only, rebates-focused
                } else {
                    self.order_amount
                };
                // Asymmetric size: in Trend liquidation, boost flatten-side 2x to exit faster
                let (bid_size_mult, ask_size_mult) = if inv_pct > 0.30 {
                    let ask = if trend_flatten_ask { 2.0 } else { 1.2 }; // Long + TrendingDown: bigger asks
                    (0.6, ask)
                } else if inv_pct < 0.20 {
                    let bid = if trend_flatten_bid { 2.0 } else { 1.2 }; // Short + TrendingUp: bigger bids
                    (bid, 0.6)
                } else {
                    (1.0, 1.0)
                };
                let bid_size = if layer == 1 {
                    base_layer_size * Decimal::from_f64_retain(bid_size_mult).unwrap_or(dec!(1))
                } else {
                    base_layer_size
                };
                let ask_size = if layer == 1 {
                    base_layer_size * Decimal::from_f64_retain(ask_size_mult).unwrap_or(dec!(1))
                } else {
                    base_layer_size
                };
                // Trend Exit Mode: ignore expectancy penalty — flatten over rebate
                let penalty =
                    if (layer == 1 && trend_flatten_ask) || (layer == 1 && trend_flatten_bid) {
                        dec!(1.0)
                    } else {
                        self.layer_expectancy_penalty
                            .get(&layer)
                            .copied()
                            .unwrap_or(dec!(1.0))
                    };
                // L1: dynamic 20-30 bps from vol (fill-rate elasticity); L2/L3: fixed
                let toxicity_mult = 1.0 + 0.5 * self.toxicity_score; // Gentler spread modulation
                let spread = if layer >= 2 {
                    *base_spread
                        * Decimal::from_f64_retain(regime_spread_mult).unwrap_or(dec!(1))
                        * penalty
                        * Decimal::from_f64_retain(self.backtest_spread_multiplier)
                            .unwrap_or(dec!(1))
                } else {
                    // L1: floor 15–20 bps — pure rebate capture, no trend-taking
                    let l1_base = (20.0 + 0.4 * self.current_vol_bps.min(25.0)).max(18.0);
                    let l1_base_dec = Decimal::from_f64_retain(l1_base).unwrap_or(dec!(25.0));
                    l1_base_dec
                        * Decimal::from_f64_retain(vol_factor * toxicity_mult * bid_regime_mult)
                            .unwrap_or(dec!(1))
                        * penalty
                        * Decimal::from_f64_retain(self.backtest_spread_multiplier)
                            .unwrap_or(dec!(1))
                };
                let ask_spread = if layer >= 2 {
                    *base_spread
                        * Decimal::from_f64_retain(ask_regime_mult).unwrap_or(dec!(1))
                        * penalty
                        * Decimal::from_f64_retain(self.backtest_spread_multiplier)
                            .unwrap_or(dec!(1))
                } else {
                    let l1_base = (20.0 + 0.4 * self.current_vol_bps.min(25.0)).max(18.0);
                    let l1_base_dec = Decimal::from_f64_retain(l1_base).unwrap_or(dec!(25.0));
                    l1_base_dec
                        * Decimal::from_f64_retain(vol_factor * toxicity_mult * ask_regime_mult)
                            .unwrap_or(dec!(1))
                        * penalty
                        * Decimal::from_f64_retain(self.backtest_spread_multiplier)
                            .unwrap_or(dec!(1))
                };
                let disabled = self
                    .layer_disabled_until
                    .get(&layer)
                    .copied()
                    .map_or(false, |until| ts < until);
                let last_bid_fill = self.last_bid_fill_times.get(&layer).copied().flatten();
                let can_place_bid =
                    last_bid_fill.map_or(true, |t| ts - t >= self.filled_order_delay);
                let last_ask_fill = self.last_ask_fill_times.get(&layer).copied().flatten();
                let can_place_ask =
                    last_ask_fill.map_or(true, |t| ts - t >= self.filled_order_delay);
                if disabled {
                    // Cancel both sides for this layer
                    intents.push(OrderIntent {
                        side: OrderSide::Buy,
                        price: Decimal::ZERO,
                        amount: Decimal::ZERO,
                        order_type: OrderType::Cancel,
                        layer,
                    });
                    intents.push(OrderIntent {
                        side: OrderSide::Sell,
                        price: Decimal::ZERO,
                        amount: Decimal::ZERO,
                        order_type: OrderType::Cancel,
                        layer,
                    });
                    continue;
                }
                // Trend liquidation: exit inventory, don't pause — L1 only enables flatten side
                let l1_bid_ok = layer != 1 || l1_bid_needed;
                let l1_ask_ok = layer != 1 || l1_ask_needed;
                // Shift quotes down 1–3 bps when long (inv > 30%): less aggressive bid, more aggressive ask
                let inv_shift_bps = if inv_pct > 0.30 {
                    -2.0
                } else if inv_pct < 0.20 {
                    2.0
                } else {
                    0.0
                };
                let inv_shift_dec =
                    Decimal::from_f64_retain(inv_shift_bps / 10000.0).unwrap_or(Decimal::ZERO);
                if buy_allowed && inv_pct < self.hard_limit_pct && can_place_bid && l1_bid_ok {
                    intents.push(OrderIntent {
                        side: OrderSide::Buy,
                        price: effective_mid * (dec!(1) - spread / dec!(10000) + inv_shift_dec),
                        amount: bid_size,
                        order_type: OrderType::Limit,
                        layer,
                    });
                } else {
                    intents.push(OrderIntent {
                        side: OrderSide::Buy,
                        price: Decimal::ZERO,
                        amount: Decimal::ZERO,
                        order_type: OrderType::Cancel,
                        layer,
                    });
                }
                if sell_allowed
                    && inv_pct > (1.0 - self.hard_limit_pct)
                    && can_place_ask
                    && l1_ask_ok
                {
                    intents.push(OrderIntent {
                        side: OrderSide::Sell,
                        price: effective_mid * (dec!(1) + ask_spread / dec!(10000) + inv_shift_dec),
                        amount: ask_size,
                        order_type: OrderType::Limit,
                        layer,
                    });
                } else {
                    intents.push(OrderIntent {
                        side: OrderSide::Sell,
                        price: Decimal::ZERO,
                        amount: Decimal::ZERO,
                        order_type: OrderType::Cancel,
                        layer,
                    });
                }
            }
            self.last_refresh_time = ts;
        }
        intents
    }
    fn on_fill(&mut self, fill: &Fill, port: &mut Portfolio, ts: f64) {
        let layer = fill.layer;
        if layer == 0 {
            // Scratch filled - record round-trip expectancy, then pop
            if let Some(&(
                ref _side,
                ref _scratch_price,
                ref amount,
                created_ts,
                ref source_layer,
                ref initial_price,
                ref entry_regime,
                entry_trend_bps,
                entry_vol_bps,
            )) = self.pending_scratches.front()
            {
                let amount_f = amount.to_f64().unwrap_or(0.0);
                let init_f = initial_price.to_f64().unwrap_or(0.0);
                let fill_f = fill.price.to_f64().unwrap_or(init_f);
                let volume = amount_f * init_f;
                let age_sec = ts - created_ts;
                // Gross P&L: Sell = (sell_price - buy_price)*amt, Buy = (buy_price - sell_price)*amt
                let gross_pnl = match fill.side {
                    OrderSide::Buy => (init_f - fill_f) * amount_f, // We sold then bought back
                    OrderSide::Sell => (fill_f - init_f) * amount_f, // We bought then sold
                };
                let rebate_bps = self.maker_fee_bps * 2.0;
                let net_pnl = gross_pnl + volume * (rebate_bps / 10000.0);
                eprintln!("[SCRATCH] gross={:.4} net={:.4} age={:.1}s exit_regime={} entry_regime={} entry_trend={:.1} entry_vol={:.2}",
    gross_pnl, net_pnl, age_sec, self.last_regime_str, entry_regime, entry_trend_bps, entry_vol_bps);
                self.record_layer_round_trip(*source_layer, gross_pnl, volume);
            }
            self.pending_scratches.pop_front();
            self.scratch_placed = false;
            return;
        }
        // [L1_FILL] — entry conditions for diagnosing good vs bad days
        let mid_f64 = fill.price.to_f64().unwrap_or(0.0);
        let trend_bps = self.calculate_short_term_trend(mid_f64, ts);
        let base_value = port.base_balance * fill.price;
        let total_value = base_value + port.quote_balance;
        let inv_pct = if total_value > Decimal::ZERO {
            (base_value / total_value)
                .to_f64()
                .unwrap_or(0.5)
                .clamp(0.0, 1.0)
        } else {
            0.5
        };
        let day_str = chrono::Utc
            .timestamp_opt(ts as i64, 0)
            .single()
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "???".into());
        eprintln!("[L1_FILL] day={} ts={:.0} side={:?} price={} amt={} regime={} trend_bps={:.1} vol_bps={:.2} inv_pct={:.1}%",
    day_str, ts, fill.side, fill.price, fill.amount, self.last_regime_str, trend_bps, self.current_vol_bps, inv_pct * 100.0);
        // v2.6: Scratch must match fill amount — selling 0.3× after 0.1× buy = net short, death spiral
        let edge_bps = Decimal::from_f64_retain(self.pingpong_spread_bps).unwrap_or(Decimal::ZERO)
            / dec!(10000);
        let (scratch_side, scratch_price) = match fill.side {
            OrderSide::Buy => (OrderSide::Sell, fill.price * (dec!(1) + edge_bps)),
            OrderSide::Sell => (OrderSide::Buy, fill.price * (dec!(1) - edge_bps)),
        };
        let scratch_amount = fill.amount; // Match fill — don't oversell/overbuy
        if self.pending_scratches.len() < self.max_scratch_queue_depth {
            self.pending_scratches.push_back((
                scratch_side,
                scratch_price,
                scratch_amount,
                ts,
                layer,
                fill.price,
                self.last_regime_str.clone(),
                trend_bps,
                self.current_vol_bps,
            ));
        }
        // Reset layer tracking and record fill time for filled_order_delay
        if fill.side == OrderSide::Buy {
            self.active_bid_orders.insert(layer, None);
            self.last_bid_fill_times.insert(layer, Some(ts));
        } else {
            self.active_ask_orders.insert(layer, None);
            self.last_ask_fill_times.insert(layer, Some(ts));
        }
    }
    fn validate_config(&self) -> Result<(), StrategyError> {
        if self.order_amount <= dec!(0) {
            return Err(StrategyError::InvalidConfig("Amount <= 0".into()));
        }
        Ok(())
    }
}
