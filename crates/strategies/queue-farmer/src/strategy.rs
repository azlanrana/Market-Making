//! QueueFarmer v1.0 — Crypto.com ETH/USDT, Japan Colo
//!
//! Thesis: With colo queue priority, adverse selection is structurally reduced.
//! Post tight two-sided quotes continuously, collect maker rebates on high volume.
//! Never use taker except one hard inventory stop. No complex regime detection —
//! colo does that filtering for us at the matching engine level.
//!
//! Fee structure:
//!   Maker rebate: -0.0075% (-0.75 bps) collected per fill
//!   Taker fee:     0.015%  (+1.50 bps) paid — only on inventory stop
//!
//! Edge:
//!   Both legs maker = +1.5 bps per round-trip rebate
//!   Spread capture  = ~2 bps half-spread (net positive on mean-reverting flow)
//!   Total target    = ~3.5 bps per round-trip before adverse selection
//!
//! Risk:
//!   Hard inventory stop at 70/30 -> single taker flatten back to 50%
//!   Daily loss limit -> cancel + market flatten, halt until next day (directional circuit breaker)
//!   Quote skew handles routine rebalancing passively (no taker cost)
//!
//! Volume target for 25% annual on $1M:
//!   $685/day / 0.00015 (1.5 bps rebate) = ~$4.5M daily volume = 4.5x AUM turnover
//!
//! Key parameters to tune after first backtest:
//!   live_spread_bps:      what you actually post (2 bps)
//!   backtest_spread_bps:  wider for sim (~8 bps → $5-10M/day; if $50M+ try 12)
//!   skew_sensitivity:     higher = more aggressive passive rebalancing
//!   inventory_stop_pct:   tighter = less drawdown, more taker costs
//!   filled_order_delay:   longer = wait for mean reversion, fewer fills

use chrono::TimeZone;
use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::strategy::{Fill, OrderIntent, OrderType, Strategy, StrategyError};
use mm_core::Portfolio;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::VecDeque;

pub struct QueueFarmerStrategy {
    // --- Core config ---
    order_amount: Decimal,
    #[allow(dead_code)] // Stored for config; fee applied by backtest runner
    maker_rebate_bps: f64,
    live_spread_bps: f64,     // What you actually post live (e.g. 2.0)
    backtest_spread_bps: f64, // Sim fill-rate calibration (3.5 → ~500-2000 fills/day; if low try 2.5)
    is_backtest: bool,
    skew_sensitivity: f64,
    max_skew_bps: f64,
    inventory_stop_pct: f64,
    inventory_stop_min_fills: u64, // require before stop_short (avoid early taker)
    inventory_stop_min_sell_fills: u64, // require sells before stop_short
    filled_order_delay: f64,
    order_refresh_sec: f64,

    // --- Vol-based spread widening ---
    vol_lookback_sec: f64,
    vol_spread_scale: f64,
    vol_max_extra_bps: f64,

    // --- Impulse filter ---
    impulse_window_sec: f64,
    impulse_threshold_bps: f64,
    impulse_cooldown_sec: f64,

    // --- State ---
    last_refresh_ts: f64,
    last_bid_fill_ts: f64,
    last_ask_fill_ts: f64,
    impulse_cooldown_until: f64,
    inventory_stop_active: bool,
    inventory_stop_cooldown_until: f64, // Don't re-fire for 60s after flatten
    last_logged_day: Option<String>,

    // --- Daily circuit breaker (directional risk) ---
    daily_start_value: Decimal,
    daily_loss_limit: Option<Decimal>, // e.g. dec!(2000) — stop for the day at -$2k
    trading_halted: bool,

    // --- Price history ---
    price_history: VecDeque<(f64, f64)>,
    current_vol_bps: f64,
    last_vol_sample_ts: f64,

    // --- Diagnostics ---
    total_maker_fills: u64,
    total_buy_fills: u64,
    total_sell_fills: u64,
    total_taker_stops: u64,
    stops_today: u64, // Reset at day boundary; log in [STOP] to diagnose loops
    last_inv_log_pct: f64,
}

impl QueueFarmerStrategy {
    pub fn new(order_amount: Decimal, maker_rebate_bps: f64) -> Self {
        Self {
            order_amount,
            maker_rebate_bps,
            live_spread_bps: 2.0,
            backtest_spread_bps: 3.5,
            is_backtest: false,
            skew_sensitivity: 8.0,
            max_skew_bps: 20.0,
            inventory_stop_pct: 0.70,
            inventory_stop_min_fills: 10,
            inventory_stop_min_sell_fills: 5,
            filled_order_delay: 2.0,
            order_refresh_sec: 0.5,

            vol_lookback_sec: 120.0,
            vol_spread_scale: 0.5,
            vol_max_extra_bps: 10.0,

            impulse_window_sec: 10.0,
            impulse_threshold_bps: 12.0,
            impulse_cooldown_sec: 3.0,

            last_refresh_ts: 0.0,
            last_bid_fill_ts: f64::NEG_INFINITY,
            last_ask_fill_ts: f64::NEG_INFINITY,
            impulse_cooldown_until: 0.0,
            inventory_stop_active: false,
            inventory_stop_cooldown_until: 0.0,
            last_logged_day: None,

            daily_start_value: Decimal::ZERO,
            daily_loss_limit: None,
            trading_halted: false,

            price_history: VecDeque::new(),
            current_vol_bps: 0.0,
            last_vol_sample_ts: 0.0,

            total_maker_fills: 0,
            total_buy_fills: 0,
            total_sell_fills: 0,
            total_taker_stops: 0,
            stops_today: 0,
            last_inv_log_pct: 0.5,
        }
    }

    /// Use spread calibrated for ~500-2000 fills/day. 3.5 bps default; if volume too low try 2.5.
    /// At 2 bps we're price-improving constantly (queue=0) → $99M/day. At 8 bps almost no fills.
    pub fn with_backtest_mode(mut self) -> Self {
        self.is_backtest = true;
        self
    }

    /// Daily loss limit: cancel + market flatten, halt until next day boundary.
    /// E.g. dec!(2000) = stop for the day at -$2k. Solves directional risk (Dec 12).
    pub fn with_daily_loss_limit(mut self, limit: Decimal) -> Self {
        self.daily_loss_limit = Some(limit);
        self
    }

    // --- Helpers ---

    fn update_vol(&mut self, ts: f64, mid: f64) {
        if ts - self.last_vol_sample_ts < 1.0 {
            return;
        }
        self.last_vol_sample_ts = ts;
        self.price_history.push_back((ts, mid));

        let retain = self.vol_lookback_sec.max(self.impulse_window_sec + 5.0);
        while self
            .price_history
            .front()
            .map_or(false, |(t, _)| *t < ts - retain)
        {
            self.price_history.pop_front();
        }

        if self.price_history.len() < 10 {
            return;
        }

        let prices: Vec<f64> = self.price_history.iter().map(|(_, p)| *p).collect();
        let returns: Vec<f64> = prices
            .windows(2)
            .filter(|w| w[0] > 0.0)
            .map(|w| (w[1] - w[0]) / w[0])
            .collect();

        if returns.len() < 5 {
            return;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
        self.current_vol_bps = variance.sqrt() * 10000.0;
    }

    fn check_impulse(&self, ts: f64, mid: f64) -> bool {
        let old = self
            .price_history
            .iter()
            .find(|(t, _)| *t >= ts - self.impulse_window_sec)
            .map(|(_, p)| *p);
        match old {
            Some(p) if p > 0.0 => ((mid - p) / p * 10000.0).abs() > self.impulse_threshold_bps,
            _ => false,
        }
    }

    fn micro_price(&self, ob: &OrderBook) -> Option<Decimal> {
        let (best_bid, bid_qty) = ob.bids.first().map(|(p, q)| (*p, *q))?;
        let (best_ask, ask_qty) = ob.asks.first().map(|(p, q)| (*p, *q))?;
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return Some((best_bid + best_ask) / dec!(2));
        }
        Some((best_bid * ask_qty + best_ask * bid_qty) / total)
    }

    fn inv_pct(&self, port: &Portfolio, mid: Decimal) -> f64 {
        let base_val = port.base_balance * mid;
        let total = base_val + port.quote_balance;
        if total <= Decimal::ZERO {
            return 0.5;
        }
        (base_val / total).to_f64().unwrap_or(0.5).clamp(0.0, 1.0)
    }

    fn effective_spread_bps(&self) -> f64 {
        let base = if self.is_backtest {
            self.backtest_spread_bps
        } else {
            self.live_spread_bps
        };
        let vol_extra = (self.current_vol_bps * self.vol_spread_scale).min(self.vol_max_extra_bps);
        base + vol_extra
    }

    fn cancel(side: OrderSide, layer: u32) -> OrderIntent {
        OrderIntent {
            side,
            price: Decimal::ZERO,
            amount: Decimal::ZERO,
            order_type: OrderType::Cancel,
            layer,
        }
    }
}

impl Strategy for QueueFarmerStrategy {
    fn name(&self) -> &str {
        "queue_farmer"
    }

    fn on_orderbook_update(
        &mut self,
        ob: &OrderBook,
        port: &Portfolio,
        ts: f64,
    ) -> Vec<OrderIntent> {
        let mut intents = Vec::new();

        let micro = match self.micro_price(ob) {
            Some(m) => m,
            None => return intents,
        };
        let mid = ob.mid_price().unwrap_or(micro);
        let mid_f64 = mid.to_f64().unwrap_or(0.0);

        let current_value = port.base_balance * mid + port.quote_balance;

        // Day boundary: reset daily start value and trading halt
        if let Some(dt) = chrono::Utc.timestamp_opt(ts as i64, 0).single() {
            let day = dt.format("%Y-%m-%d").to_string();
            if self.last_logged_day.as_ref() != Some(&day) {
                let stops_yesterday = self.stops_today;
                self.daily_start_value = current_value;
                self.trading_halted = false;
                self.stops_today = 0;
                self.last_logged_day = Some(day.clone());
                eprintln!(
                    "[DAY_START] ts={:.0} day={} maker_fills={} taker_stops={} stops_yesterday={} daily_start={:.0}",
                    ts, day, self.total_maker_fills, self.total_taker_stops,
                    stops_yesterday, self.daily_start_value
                );
            }
        }

        // --- Daily loss limit (circuit breaker for directional risk) ---
        if let Some(limit) = self.daily_loss_limit {
            let daily_pnl = current_value - self.daily_start_value;
            if daily_pnl < -limit {
                if !self.trading_halted {
                    // First trigger: log, flatten, halt
                    eprintln!(
                        "[HALT] daily_pnl={} limit={} — halting until next day",
                        daily_pnl, limit
                    );
                    self.trading_halted = true;

                    intents.push(Self::cancel(OrderSide::Buy, 1));
                    intents.push(Self::cancel(OrderSide::Sell, 1));

                    let total_base_equiv = port.base_balance + port.quote_balance / mid;
                    let target_base = total_base_equiv * dec!(0.5);
                    let (side, amount) = if port.base_balance > target_base {
                        (
                            OrderSide::Sell,
                            (port.base_balance - target_base).max(Decimal::ZERO),
                        )
                    } else {
                        (
                            OrderSide::Buy,
                            (target_base - port.base_balance).max(Decimal::ZERO),
                        )
                    };
                    if amount > Decimal::ZERO {
                        intents.push(OrderIntent {
                            side,
                            price: mid,
                            amount,
                            order_type: OrderType::Market,
                            layer: 0,
                        });
                    }
                }
                // Every subsequent tick: just return (no more orders)
                return intents;
            }
        }

        // If we halted earlier today, stay halted — do NOT resume when daily_pnl bounces above limit.
        if self.trading_halted {
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            return intents;
        }

        self.update_vol(ts, mid_f64);

        let inv = self.inv_pct(port, mid);

        if (inv - self.last_inv_log_pct).abs() > 0.05 {
            eprintln!(
                "[INV] ts={:.0} inv={:.1}% vol_bps={:.2} spread_bps={:.2}",
                ts,
                inv * 100.0,
                self.current_vol_bps,
                self.effective_spread_bps()
            );
            self.last_inv_log_pct = inv;
        }

        // --- Hard inventory stop ---
        // Single taker order back to 50%. Cooldown 60s prevents rapid re-fire when inv
        // drifts back into danger zone before skew can rebalance.
        // Skip stop_short until we've drifted short from selling — not just low inv from one-sided buys.
        let stop_long = inv > self.inventory_stop_pct;
        let stop_short = inv < (1.0 - self.inventory_stop_pct)
            && self.total_maker_fills >= self.inventory_stop_min_fills
            && self.total_sell_fills >= self.inventory_stop_min_sell_fills;

        if stop_long || stop_short {
            // Cancel resting quotes immediately regardless
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));

            // Cooldown: don't re-fire for 60s after last stop — prevents $14k bleed from loops
            let in_cooldown = ts < self.inventory_stop_cooldown_until;
            if in_cooldown {
                return intents;
            }

            if !self.inventory_stop_active {
                self.inventory_stop_active = true;
                self.inventory_stop_cooldown_until = ts + 60.0;
                self.total_taker_stops += 1;
                self.stops_today += 1;

                // Size to flatten back to 50%
                let total_base_equiv = port.base_balance + port.quote_balance / mid;
                let target_base = total_base_equiv * dec!(0.5);
                let (side, amount) = if stop_long {
                    let excess = (port.base_balance - target_base).max(Decimal::ZERO);
                    (OrderSide::Sell, excess)
                } else {
                    let deficit = (target_base - port.base_balance).max(Decimal::ZERO);
                    (OrderSide::Buy, deficit)
                };

                if amount > Decimal::ZERO {
                    eprintln!(
                        "[STOP] ts={:.0} inv={:.1}% side={:?} amount={} stop_n={} stops_today={}",
                        ts,
                        inv * 100.0,
                        side,
                        amount,
                        self.total_taker_stops,
                        self.stops_today
                    );
                    intents.push(OrderIntent {
                        side,
                        price: mid,
                        amount,
                        order_type: OrderType::Market,
                        layer: 0,
                    });
                }
            }
            return intents;
        } else {
            // Reset stop flag once inventory is back in safe zone
            self.inventory_stop_active = false;
        }

        // --- Impulse filter ---
        if self.check_impulse(ts, mid_f64) && ts >= self.impulse_cooldown_until {
            self.impulse_cooldown_until = ts + self.impulse_cooldown_sec;
            eprintln!(
                "[IMPULSE] ts={:.0} vol_bps={:.2} cooling {:.1}s",
                ts, self.current_vol_bps, self.impulse_cooldown_sec
            );
        }

        if ts < self.impulse_cooldown_until {
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            return intents;
        }

        // --- Throttle refreshes ---
        let needs_refresh =
            self.last_refresh_ts == 0.0 || (ts - self.last_refresh_ts) >= self.order_refresh_sec;

        if !needs_refresh {
            return intents;
        }
        self.last_refresh_ts = ts;

        // --- Spread ---
        let spread = self.effective_spread_bps();
        let spread_dec = Decimal::from_f64_retain(spread / 10000.0).unwrap_or(dec!(0.0002));

        // --- Inventory skew ---
        // Shift both quotes toward reducing position.
        // Long: shift DOWN (discourage more buys, encourage sells)
        // Short: shift UP (encourage buys, discourage sells)
        let skew_bps = ((inv - 0.5) * self.skew_sensitivity * 100.0)
            .clamp(-self.max_skew_bps, self.max_skew_bps);
        let skew_dec = Decimal::from_f64_retain(skew_bps / 10000.0).unwrap_or(Decimal::ZERO);

        // Effective mid: micro-price shifted by inventory lean
        let effective_mid = micro * (dec!(1) - skew_dec);

        // --- Fill delays: don't immediately re-quote the same side ---
        let can_bid = ts - self.last_bid_fill_ts >= self.filled_order_delay;
        let can_ask = ts - self.last_ask_fill_ts >= self.filled_order_delay;

        // --- Soft inventory gates: don't add to a dangerously skewed position ---
        // These are softer than the hard stop — just prevents making things worse
        // passively while the skew is doing its job.
        let bid_allowed = inv < 0.65 && can_bid;
        let ask_allowed = inv > 0.35 && can_ask;

        // --- Place bid ---
        if bid_allowed {
            intents.push(OrderIntent {
                side: OrderSide::Buy,
                price: effective_mid * (dec!(1) - spread_dec),
                amount: self.order_amount,
                order_type: OrderType::Limit,
                layer: 1,
            });
        } else {
            intents.push(Self::cancel(OrderSide::Buy, 1));
        }

        // --- Place ask ---
        if ask_allowed {
            intents.push(OrderIntent {
                side: OrderSide::Sell,
                price: effective_mid * (dec!(1) + spread_dec),
                amount: self.order_amount,
                order_type: OrderType::Limit,
                layer: 1,
            });
        } else {
            intents.push(Self::cancel(OrderSide::Sell, 1));
        }

        intents
    }

    fn on_fill(&mut self, fill: &Fill, _port: &mut Portfolio, ts: f64) {
        // Layer 0 = taker stop fill, nothing to track
        if fill.layer == 0 {
            return;
        }

        match fill.side {
            OrderSide::Buy => {
                self.last_bid_fill_ts = ts;
                self.total_buy_fills += 1;
            }
            OrderSide::Sell => {
                self.last_ask_fill_ts = ts;
                self.total_sell_fills += 1;
            }
        }

        self.total_maker_fills += 1;

        eprintln!(
            "[FILL] ts={:.0} side={:?} price={} amt={} vol_bps={:.2} n={}",
            ts, fill.side, fill.price, fill.amount, self.current_vol_bps, self.total_maker_fills
        );
    }

    fn validate_config(&self) -> Result<(), StrategyError> {
        if self.order_amount <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "order_amount must be > 0".into(),
            ));
        }
        if self.live_spread_bps <= 0.0 {
            return Err(StrategyError::InvalidConfig(
                "live_spread_bps must be > 0".into(),
            ));
        }
        if self.backtest_spread_bps <= 0.0 {
            return Err(StrategyError::InvalidConfig(
                "backtest_spread_bps must be > 0".into(),
            ));
        }
        if self.inventory_stop_pct <= 0.5 || self.inventory_stop_pct >= 1.0 {
            return Err(StrategyError::InvalidConfig(
                "inventory_stop_pct must be in (0.5, 1.0)".into(),
            ));
        }
        if let Some(limit) = self.daily_loss_limit {
            if limit <= Decimal::ZERO {
                return Err(StrategyError::InvalidConfig(
                    "daily_loss_limit must be > 0".into(),
                ));
            }
        }
        Ok(())
    }
}
