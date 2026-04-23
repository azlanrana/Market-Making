//! QueueFarmer v4.0 — Crypto.com ETH/USDT, Japan Colo
//!
//! Major change vs v3: tick-based quoting instead of spread_bps.
//!
//! v3 problem: quoting at 1.0 bps in a 0.03 bps market = sitting 33x the
//! spread away from mid. Only fills on big directional sweeps = pure adverse
//! selection. 101k round trips, -$46k realized, 13 bps avg adverse selection.
//!
//! v4 approach: quote AT the touch (match best bid/ask). This is what colo
//! is actually for — getting queue priority at the touch, not quoting wide.
//!
//! Adverse selection defense without flow data:
//!   1. Book imbalance filter — if top 3 levels are 80%+ one-sided, pull quotes.
//!      Heavy one-sided stacking = someone knows something or is spoofing.
//!   2. Spread filter — if the real bid-ask spread widens beyond normal
//!      (e.g. >2x typical), it signals volatility/uncertainty. Pull quotes.
//!      Spread widening is computable from a single snapshot and is a genuine
//!      signal that the book is stressed.
//!   3. Inventory skew — don't quote both sides equally. When long, only post
//!      ask at touch; suppress bid (or post bid one tick worse). This reduces
//!      the chance of doubling down on a losing position.
//!
//! Fill model interaction:
//!   Quoting AT best_bid/best_ask → queue_position = full level size (back of queue).
//!   Quoting AT best_bid+tick / best_ask-tick → queue_position = 50% of touch level.
//!   We use price-improving (one tick better) to get the 50% queue assumption.
//!   In live with colo, queue position is actually much better than 50%.
//!
//! Key parameters:
//!   tick_size               — must match exchange (0.01 for ETH/USDT)
//!   price_improve           — quote one tick better than touch (recommended: true)
//!   book_imbalance_threshold — pull quotes when book is >X% one-sided (0.80)
//!   spread_widening_factor  — pull quotes when spread > X * typical_spread (3.0)
//!   inventory_stop_pct      — hard taker flatten at 65/35
//!   skew_levels             — how aggressively to suppress one side on inventory

use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::strategy::{Fill, OrderIntent, OrderType, Strategy, StrategyError};
use mm_core::Portfolio;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::VecDeque;

pub struct QueueFarmerV4 {
    // --- Config ---
    order_amount: Decimal,
    tick_size: Decimal,
    price_improve: bool, // quote one tick better than touch

    // Inventory skew: suppress the side that adds to our position
    // 0.0 = always quote both sides, 1.0 = only quote the reducing side
    skew_sensitivity: f64,

    inventory_stop_pct: f64,
    inventory_stop_cooldown_sec: f64,

    // Book imbalance filter
    book_imbalance_threshold: f64,
    book_imbalance_levels: usize,

    // Spread widening filter
    // Track a rolling typical spread; if current spread > factor * typical, pull quotes
    spread_widening_factor: f64,
    spread_history_len: usize,

    order_refresh_sec: f64,
    warmup_sec: f64,

    // Microprice: (bid*ask_qty + ask*bid_qty)/(bid_qty+ask_qty). Suppress side when microprice signals against us.
    microprice_threshold_bps: f64,
    microprice_enabled: bool,

    // Momentum: suppress bid on down move, ask on up move
    momentum_lookback: usize,
    momentum_threshold_bps: f64,
    momentum_enabled: bool,

    // Volatility: pull quotes when rolling vol exceeds threshold
    vol_lookback: usize,
    vol_threshold_bps: f64,
    vol_enabled: bool,

    // --- State ---
    start_ts: Option<f64>,
    last_refresh_ts: f64,
    inventory_stop_active: bool,
    inventory_stop_cooldown_until: f64,
    spread_history: VecDeque<f64>, // rolling typical spread in bps
    mid_history: VecDeque<f64>,    // for momentum
    return_history: VecDeque<f64>, // for volatility (simple returns)

    // --- Diagnostics ---
    total_maker_fills: u64,
    total_taker_stops: u64,
    total_imbalance_pauses: u64,
    total_spread_pauses: u64,
    imbalance_blocks: u64,
    spread_blocks: u64,
    microprice_blocks: u64,
    momentum_blocks: u64,
    vol_blocks: u64,
    crossed_blocks: u64,
    refresh_throttled: u64,
    bid_suppressed: u64,
    ask_suppressed: u64,
    quotes_placed: u64,
}

impl QueueFarmerV4 {
    pub fn new(order_amount: Decimal, tick_size: Decimal) -> Self {
        Self {
            order_amount,
            tick_size,
            price_improve: true,

            skew_sensitivity: 0.6, // 60% suppression of adding side per 10% inventory deviation

            inventory_stop_pct: 0.65,
            inventory_stop_cooldown_sec: 60.0,

            book_imbalance_threshold: 0.80,
            book_imbalance_levels: 3,

            spread_widening_factor: 3.0,
            spread_history_len: 50, // ~25 seconds of history at 2 snapshots/sec

            order_refresh_sec: 0.5,
            warmup_sec: 30.0,

            microprice_threshold_bps: 0.2,
            microprice_enabled: false,
            momentum_lookback: 10,
            momentum_threshold_bps: 0.3,
            momentum_enabled: false,
            vol_lookback: 50,
            vol_threshold_bps: 2.0,
            vol_enabled: false,

            start_ts: None,
            last_refresh_ts: 0.0,
            inventory_stop_active: false,
            inventory_stop_cooldown_until: 0.0,
            spread_history: VecDeque::new(),
            mid_history: VecDeque::new(),
            return_history: VecDeque::new(),

            total_maker_fills: 0,
            total_taker_stops: 0,
            total_imbalance_pauses: 0,
            total_spread_pauses: 0,
            imbalance_blocks: 0,
            spread_blocks: 0,
            microprice_blocks: 0,
            momentum_blocks: 0,
            vol_blocks: 0,
            crossed_blocks: 0,
            refresh_throttled: 0,
            bid_suppressed: 0,
            ask_suppressed: 0,
            quotes_placed: 0,
        }
    }

    pub fn with_price_improve(mut self, improve: bool) -> Self {
        self.price_improve = improve;
        self
    }

    pub fn with_inventory_stop(mut self, stop_pct: f64) -> Self {
        self.inventory_stop_pct = stop_pct;
        self
    }

    pub fn with_book_imbalance(mut self, threshold: f64, levels: usize) -> Self {
        self.book_imbalance_threshold = threshold;
        self.book_imbalance_levels = levels;
        self
    }

    pub fn with_spread_filter(mut self, widening_factor: f64) -> Self {
        self.spread_widening_factor = widening_factor;
        self
    }

    pub fn with_warmup(mut self, warmup_sec: f64) -> Self {
        self.warmup_sec = warmup_sec;
        self
    }

    pub fn with_microprice(mut self, threshold_bps: f64) -> Self {
        self.microprice_enabled = true;
        self.microprice_threshold_bps = threshold_bps;
        self
    }

    pub fn with_momentum(mut self, lookback: usize, threshold_bps: f64) -> Self {
        self.momentum_enabled = true;
        self.momentum_lookback = lookback;
        self.momentum_threshold_bps = threshold_bps;
        self
    }

    pub fn with_volatility_filter(mut self, lookback: usize, threshold_bps: f64) -> Self {
        self.vol_enabled = true;
        self.vol_lookback = lookback;
        self.vol_threshold_bps = threshold_bps;
        self
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn inv_pct(port: &Portfolio, mid: Decimal) -> f64 {
        let base_val = port.base_balance * mid;
        let total = base_val + port.quote_balance;
        if total <= Decimal::ZERO {
            return 0.5;
        }
        (base_val / total).to_f64().unwrap_or(0.5).clamp(0.0, 1.0)
    }

    fn book_imbalance(&self, ob: &OrderBook) -> f64 {
        let bid_qty: Decimal = ob
            .bids
            .iter()
            .take(self.book_imbalance_levels)
            .map(|(_, q)| *q)
            .sum();
        let ask_qty: Decimal = ob
            .asks
            .iter()
            .take(self.book_imbalance_levels)
            .map(|(_, q)| *q)
            .sum();
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return 0.0;
        }
        ((bid_qty - ask_qty) / total).to_f64().unwrap_or(0.0)
    }

    fn current_spread_bps(ob: &OrderBook) -> Option<f64> {
        let best_bid = ob.bids.first().map(|(p, _)| *p)?;
        let best_ask = ob.asks.first().map(|(p, _)| *p)?;
        let mid = (best_bid + best_ask) / dec!(2);
        if mid <= Decimal::ZERO {
            return None;
        }
        Some(
            ((best_ask - best_bid) / mid * dec!(10000))
                .to_f64()
                .unwrap_or(0.0),
        )
    }

    fn typical_spread(&self) -> Option<f64> {
        if self.spread_history.len() < 10 {
            return None;
        }
        let sum: f64 = self.spread_history.iter().sum();
        Some(sum / self.spread_history.len() as f64)
    }

    /// Microprice: volume-weighted mid. (bid*ask_qty + ask*bid_qty) / (bid_qty + ask_qty)
    fn microprice(ob: &OrderBook) -> Option<f64> {
        let (bid, bid_qty) = ob
            .bids
            .first()
            .and_then(|(p, q)| Some((p.to_f64()?, q.to_f64()?)))?;
        let (ask, ask_qty) = ob
            .asks
            .first()
            .and_then(|(p, q)| Some((p.to_f64()?, q.to_f64()?)))?;
        let total = bid_qty + ask_qty;
        if total <= 0.0 {
            return None;
        }
        Some((bid * ask_qty + ask * bid_qty) / total)
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

impl Strategy for QueueFarmerV4 {
    fn name(&self) -> &str {
        "queue_farmer_v4"
    }

    fn on_orderbook_update(
        &mut self,
        ob: &OrderBook,
        port: &Portfolio,
        ts: f64,
    ) -> Vec<OrderIntent> {
        let mut intents = Vec::new();

        let start_ts = *self.start_ts.get_or_insert(ts);
        let warmed_up = (ts - start_ts) >= self.warmup_sec;

        let best_bid = match ob.bids.first().map(|(p, _)| *p) {
            Some(p) => p,
            None => return intents,
        };
        let best_ask = match ob.asks.first().map(|(p, _)| *p) {
            Some(p) => p,
            None => return intents,
        };
        let mid = (best_bid + best_ask) / dec!(2);

        // Update spread history every tick
        if let Some(spread_bps) = Self::current_spread_bps(ob) {
            self.spread_history.push_back(spread_bps);
            if self.spread_history.len() > self.spread_history_len {
                self.spread_history.pop_front();
            }
        }

        // Update mid and return history (for momentum and volatility)
        let mid_f64 = mid.to_f64().unwrap_or(0.0);
        if mid_f64 > 0.0 {
            if let Some(&prev_mid) = self.mid_history.back() {
                let ret = (mid_f64 - prev_mid) / prev_mid;
                self.return_history.push_back(ret);
                if self.return_history.len() > self.vol_lookback + 10 {
                    self.return_history.pop_front();
                }
            }
            self.mid_history.push_back(mid_f64);
            if self.mid_history.len() > self.momentum_lookback + 10 {
                self.mid_history.pop_front();
            }
        }

        let inv = Self::inv_pct(port, mid);

        // -----------------------------------------------------------------------
        // 1. Hard inventory stop
        // -----------------------------------------------------------------------
        if warmed_up {
            let stop_long = inv > self.inventory_stop_pct;
            let stop_short = inv < (1.0 - self.inventory_stop_pct);

            if stop_long || stop_short {
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));

                if ts < self.inventory_stop_cooldown_until {
                    return intents;
                }

                // Fire flatten every time we're past cooldown and still out of range.
                // Previously we only fired once; if the market order partially filled,
                // we were stuck forever. Now we retry until inventory is back in range.
                if !self.inventory_stop_active {
                    self.total_taker_stops += 1;
                }
                self.inventory_stop_active = true;
                self.inventory_stop_cooldown_until = ts + self.inventory_stop_cooldown_sec;

                let total_base_equiv = port.base_balance + port.quote_balance / mid;
                let target_base = total_base_equiv * dec!(0.5);
                let (side, amount) = if stop_long {
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
                return intents;
            } else {
                self.inventory_stop_active = false;
            }
        }

        // -----------------------------------------------------------------------
        // 2. Book imbalance filter
        // -----------------------------------------------------------------------
        let imbalance = self.book_imbalance(ob);
        if imbalance.abs() > self.book_imbalance_threshold {
            self.total_imbalance_pauses += 1;
            self.imbalance_blocks += 1;
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            return intents;
        }

        // -----------------------------------------------------------------------
        // 3. Spread widening filter
        //    If current spread is >3x the typical spread, book is stressed.
        //    Pull quotes — wide spread means someone is pulling liquidity fast.
        // -----------------------------------------------------------------------
        if let (Some(current_bps), Some(typical_bps)) =
            (Self::current_spread_bps(ob), self.typical_spread())
        {
            if current_bps > self.spread_widening_factor * typical_bps {
                self.total_spread_pauses += 1;
                self.spread_blocks += 1;
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));
                return intents;
            }
        }

        // -----------------------------------------------------------------------
        // 4. Volatility filter — pull quotes when rolling vol spikes
        // -----------------------------------------------------------------------
        if self.vol_enabled && self.return_history.len() >= self.vol_lookback {
            let mean: f64 = self
                .return_history
                .iter()
                .rev()
                .take(self.vol_lookback)
                .sum::<f64>()
                / self.vol_lookback as f64;
            let variance: f64 = self
                .return_history
                .iter()
                .rev()
                .take(self.vol_lookback)
                .map(|r| (r - mean).powi(2))
                .sum::<f64>()
                / self.vol_lookback as f64;
            let vol_bps = variance.sqrt() * 10000.0;
            if vol_bps > self.vol_threshold_bps {
                self.vol_blocks += 1;
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));
                return intents;
            }
        }

        // -----------------------------------------------------------------------
        // 5. Throttle refreshes
        // -----------------------------------------------------------------------
        if ts - self.last_refresh_ts < self.order_refresh_sec {
            self.refresh_throttled += 1;
            return intents;
        }
        self.last_refresh_ts = ts;

        // -----------------------------------------------------------------------
        // 6. Compute quotes — tick-based, at or one tick better than touch
        //    When spread is exactly 1 tick, price-improving would cross the book
        //    (best_bid+tick = best_ask, best_ask-tick = best_bid). Match touch instead.
        // -----------------------------------------------------------------------
        let spread = best_ask - best_bid;
        let (our_bid, our_ask) = if self.price_improve && spread > self.tick_size {
            // Price-improving: new best bid/ask → queue_position = 50% of touch level
            (best_bid + self.tick_size, best_ask - self.tick_size)
        } else {
            // Match touch when spread is minimum (1 tick) or price_improve disabled
            (best_bid, best_ask)
        };

        // Sanity: don't cross the book
        if our_bid >= our_ask {
            self.crossed_blocks += 1;
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            return intents;
        }

        // -----------------------------------------------------------------------
        // 7. Microprice & momentum suppression (augment inventory skew)
        // -----------------------------------------------------------------------
        let mut microprice_suppress_bid = false;
        let mut microprice_suppress_ask = false;
        if self.microprice_enabled {
            if let (Some(mp), Some(m)) = (Self::microprice(ob), mid.to_f64()) {
                if m > 0.0 {
                    let diff_bps = (mp - m) / m * 10000.0;
                    if diff_bps < -self.microprice_threshold_bps {
                        microprice_suppress_bid = true; // microprice below mid = cautious on bid
                        self.microprice_blocks += 1;
                    } else if diff_bps > self.microprice_threshold_bps {
                        microprice_suppress_ask = true; // microprice above mid = cautious on ask
                        self.microprice_blocks += 1;
                    }
                }
            }
        }

        let mut momentum_suppress_bid = false;
        let mut momentum_suppress_ask = false;
        if self.momentum_enabled && self.mid_history.len() > self.momentum_lookback {
            let mid_now = self.mid_history.back().copied().unwrap_or(0.0);
            let mid_old = self
                .mid_history
                .get(self.mid_history.len() - 1 - self.momentum_lookback)
                .copied()
                .unwrap_or(0.0);
            if mid_old > 0.0 {
                let mom_bps = (mid_now - mid_old) / mid_old * 10000.0;
                if mom_bps < -self.momentum_threshold_bps {
                    momentum_suppress_bid = true; // price dropped, don't bid
                    self.momentum_blocks += 1;
                } else if mom_bps > self.momentum_threshold_bps {
                    momentum_suppress_ask = true; // price rose, don't ask
                    self.momentum_blocks += 1;
                }
            }
        }

        // -----------------------------------------------------------------------
        // 8. Inventory-based side suppression
        //    inv > 0.5 (long): suppress bid at 0.60 — don't add to long
        //    inv < 0.5 (short): suppress ask only when we have no base to sell (inv ≈ 0).
        //    At inv=0.40 we were blocking asks — but with 56 buys inv only reached ~18%.
        //    We need to post asks as soon as we have base to sell (inv > 0.01).
        // -----------------------------------------------------------------------
        let deviation = (inv - 0.5).abs();
        let _suppression =
            (deviation / (self.inventory_stop_pct - 0.5) * self.skew_sensitivity).clamp(0.0, 1.0);

        let mut bid_allowed = if inv > 0.5 { inv < 0.60 } else { true };
        let mut ask_allowed = if inv < 0.5 { inv > 0.01 } else { true };
        bid_allowed = bid_allowed && !microprice_suppress_bid && !momentum_suppress_bid;
        ask_allowed = ask_allowed && !microprice_suppress_ask && !momentum_suppress_ask;

        if bid_allowed {
            self.quotes_placed += 1;
            intents.push(OrderIntent {
                side: OrderSide::Buy,
                price: our_bid,
                amount: self.order_amount,
                order_type: OrderType::Limit,
                layer: 1,
            });
        } else {
            self.bid_suppressed += 1;
            intents.push(Self::cancel(OrderSide::Buy, 1));
        }

        if ask_allowed {
            self.quotes_placed += 1;
            intents.push(OrderIntent {
                side: OrderSide::Sell,
                price: our_ask,
                amount: self.order_amount,
                order_type: OrderType::Limit,
                layer: 1,
            });
        } else {
            self.ask_suppressed += 1;
            intents.push(Self::cancel(OrderSide::Sell, 1));
        }

        intents
    }

    fn on_fill(&mut self, fill: &Fill, _port: &mut Portfolio, _ts: f64) {
        if fill.layer == 0 {
            return;
        }
        self.total_maker_fills += 1;
    }

    fn validate_config(&self) -> Result<(), StrategyError> {
        if self.order_amount <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "order_amount must be > 0".into(),
            ));
        }
        if self.tick_size <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig("tick_size must be > 0".into()));
        }
        if !(0.5 < self.inventory_stop_pct && self.inventory_stop_pct < 1.0) {
            return Err(StrategyError::InvalidConfig(
                "inventory_stop_pct must be in (0.5, 1.0)".into(),
            ));
        }
        if !(0.0 < self.book_imbalance_threshold && self.book_imbalance_threshold <= 1.0) {
            return Err(StrategyError::InvalidConfig(
                "book_imbalance_threshold must be in (0.0, 1.0]".into(),
            ));
        }
        if self.spread_widening_factor < 1.0 {
            return Err(StrategyError::InvalidConfig(
                "spread_widening_factor must be >= 1.0".into(),
            ));
        }
        Ok(())
    }

    fn gate_diagnostics(&self) -> Option<String> {
        Some(format!(
            "[GATES] imbalance={} spread={} microprice={} momentum={} vol={} crossed={} throttled={} bid_suppressed={} ask_suppressed={} quotes_placed={}",
            self.imbalance_blocks,
            self.spread_blocks,
            self.microprice_blocks,
            self.momentum_blocks,
            self.vol_blocks,
            self.crossed_blocks,
            self.refresh_throttled,
            self.bid_suppressed,
            self.ask_suppressed,
            self.quotes_placed,
        ))
    }
}
