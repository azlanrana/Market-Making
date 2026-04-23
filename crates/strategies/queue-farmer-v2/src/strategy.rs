//! QueueFarmer v2.0 — Crypto.com ETH/USDT, Japan Colo
//!
//! Simplified from v1. Core insight: pump/dumps were the primary PnL killer.
//! Solution: aggressive one-sided flow detection that pulls quotes *before*
//! inventory runs away, not after. Everything else stripped to reduce
//! parameter surface and interaction complexity.
//!
//! What's here:
//!   1. Micro-price + inventory skew  (passive rebalancing, zero taker cost)
//!   2. Flow imbalance filter         (THE pump/dump defense — see below)
//!   3. Hard inventory stop           (last resort, tightened to 65/35)
//!
//! What's gone vs v1:
//!   - Vol-based spread widening  (lagging, reacts after damage done)
//!   - Fill delays                (noise)
//!   - Daily loss circuit breaker (symptom, not cause)
//!   - Impulse filter on mid-price (too slow — price has already moved)
//!
//! Pump/dump defense — flow imbalance filter:
//!   Tracks buy_volume vs sell_volume at the top of book over a short window.
//!   When one side dominates (e.g. 80%+ of flow is buys), that's aggressive
//!   directional order flow — the signature of a pump. We pull *both* quotes
//!   immediately and sit out for `flow_cooldown_sec`. This fires *before* the
//!   price move completes, unlike a mid-price impulse filter which fires after.
//!
//!   The key difference: we're watching WHO is aggressing, not where price went.
//!
//! Parameters to tune first:
//!   spread_bps           — live: 2.0, backtest: 3.5 (controls fill rate)
//!   flow_window_sec      — lookback for imbalance calc (default: 8s)
//!   flow_imbalance_threshold — 0.0 = balanced, 1.0 = fully one-sided (default: 0.75)
//!   flow_cooldown_sec    — how long to stay flat after detecting pump (default: 15s)
//!   inventory_stop_pct   — hard flatten threshold (default: 0.65)
//!   skew_sensitivity     — how aggressively to lean quotes on inventory (default: 6.0)

use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::strategy::{Fill, OrderIntent, OrderType, Strategy, StrategyError};
use mm_core::Portfolio;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::VecDeque;

// ---------------------------------------------------------------------------
// Flow event: recorded each time we see the book update
// ---------------------------------------------------------------------------
#[derive(Debug)]
struct FlowEvent {
    ts: f64,
    buy_vol: f64,  // aggressive buy volume at top of book
    sell_vol: f64, // aggressive sell volume at top of book
}

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------
pub struct QueueFarmerV2 {
    // --- Config ---
    order_amount: Decimal,
    spread_bps: f64,
    backtest_spread_bps: f64,
    is_backtest: bool,

    skew_sensitivity: f64, // bps of quote shift per 1% inventory imbalance
    max_skew_bps: f64,     // cap on total skew

    inventory_stop_pct: f64, // flatten to 50% if inv exceeds this
    inventory_stop_cooldown_sec: f64,
    inventory_stop_min_fills: u64, // require this many maker fills before stop_short (avoid early taker)
    inventory_stop_min_sell_fills: u64, // require sells before stop_short (not just one-sided buys)

    // Flow imbalance (pump/dump) filter
    flow_window_sec: f64,
    flow_imbalance_threshold: f64, // 0.75 = pull quotes when 75%+ flow is one-sided
    flow_cooldown_sec: f64,        // stay flat this long after detecting imbalance

    order_refresh_sec: f64,

    // --- State ---
    last_refresh_ts: f64,
    flow_history: VecDeque<FlowEvent>,
    prev_best_bid: Option<Decimal>,
    prev_best_ask: Option<Decimal>,

    flow_cooldown_until: f64,
    inventory_stop_active: bool,
    inventory_stop_cooldown_until: f64,

    // --- Diagnostics ---
    total_maker_fills: u64,
    total_buy_fills: u64,
    total_sell_fills: u64,
    total_taker_stops: u64,
    total_flow_pauses: u64,
}

impl QueueFarmerV2 {
    pub fn new(order_amount: Decimal) -> Self {
        Self {
            order_amount,
            spread_bps: 2.0,
            backtest_spread_bps: 3.5,
            is_backtest: false,

            skew_sensitivity: 6.0,
            max_skew_bps: 15.0,

            inventory_stop_pct: 0.65,
            inventory_stop_cooldown_sec: 60.0,
            inventory_stop_min_fills: 10,
            inventory_stop_min_sell_fills: 5,

            flow_window_sec: 8.0,
            flow_imbalance_threshold: 0.75,
            flow_cooldown_sec: 15.0,

            order_refresh_sec: 0.5,

            last_refresh_ts: 0.0,
            flow_history: VecDeque::new(),
            prev_best_bid: None,
            prev_best_ask: None,

            flow_cooldown_until: 0.0,
            inventory_stop_active: false,
            inventory_stop_cooldown_until: 0.0,

            total_maker_fills: 0,
            total_buy_fills: 0,
            total_sell_fills: 0,
            total_taker_stops: 0,
            total_flow_pauses: 0,
        }
    }

    pub fn with_backtest_mode(mut self) -> Self {
        self.is_backtest = true;
        self
    }

    // Builder for tuning key params without touching internals
    pub fn with_flow_params(mut self, window_sec: f64, threshold: f64, cooldown_sec: f64) -> Self {
        self.flow_window_sec = window_sec;
        self.flow_imbalance_threshold = threshold;
        self.flow_cooldown_sec = cooldown_sec;
        self
    }

    pub fn with_spread(mut self, live_bps: f64, backtest_bps: f64) -> Self {
        self.spread_bps = live_bps;
        self.backtest_spread_bps = backtest_bps;
        self
    }

    pub fn with_inventory_stop(mut self, stop_pct: f64) -> Self {
        self.inventory_stop_pct = stop_pct;
        self
    }

    /// Minimum maker fills before firing stop_short (avoids huge taker buy during early build).
    pub fn with_inventory_stop_min_fills(mut self, n: u64) -> Self {
        self.inventory_stop_min_fills = n;
        self
    }

    /// Minimum sell fills before stop_short — ensures we've actually drifted short from selling.
    pub fn with_inventory_stop_min_sell_fills(mut self, n: u64) -> Self {
        self.inventory_stop_min_sell_fills = n;
        self
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn active_spread_bps(&self) -> f64 {
        if self.is_backtest {
            self.backtest_spread_bps
        } else {
            self.spread_bps
        }
    }

    fn micro_price(ob: &OrderBook) -> Option<Decimal> {
        let (best_bid, bid_qty) = ob.bids.first().map(|(p, q)| (*p, *q))?;
        let (best_ask, ask_qty) = ob.asks.first().map(|(p, q)| (*p, *q))?;
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return Some((best_bid + best_ask) / dec!(2));
        }
        // Micro-price: weight mid toward the thinner side
        // If ask qty is small, more aggressive buys → price wants to go up
        Some((best_bid * ask_qty + best_ask * bid_qty) / total)
    }

    fn inv_pct(port: &Portfolio, mid: Decimal) -> f64 {
        let base_val = port.base_balance * mid;
        let total = base_val + port.quote_balance;
        if total <= Decimal::ZERO {
            return 0.5;
        }
        (base_val / total).to_f64().unwrap_or(0.5).clamp(0.0, 1.0)
    }

    /// Infer aggressive flow from book changes.
    /// If best bid lifted (disappeared) → aggressive buy.
    /// If best ask hit (disappeared) → aggressive sell.
    /// This is an approximation from L2; good enough for pump detection.
    fn infer_flow(&self, ob: &OrderBook) -> (f64, f64) {
        let cur_bid = ob.bids.first().map(|(p, _)| *p);
        let cur_ask = ob.asks.first().map(|(p, _)| *p);

        let mut buy_vol = 0.0f64;
        let mut sell_vol = 0.0f64;

        // Ask side got hit: best ask price moved up → aggressive buys
        if let (Some(prev_ask), Some(cur_ask)) = (self.prev_best_ask, cur_ask) {
            if cur_ask > prev_ask {
                buy_vol = (cur_ask - prev_ask).to_f64().unwrap_or(0.0);
            }
        }
        // Bid side got lifted: best bid price moved down → aggressive sells
        if let (Some(prev_bid), Some(cur_bid)) = (self.prev_best_bid, cur_bid) {
            if cur_bid < prev_bid {
                sell_vol = (prev_bid - cur_bid).to_f64().unwrap_or(0.0);
            }
        }

        (buy_vol, sell_vol)
    }

    /// Flow imbalance over the window: +1.0 = all buys, -1.0 = all sells, 0.0 = balanced.
    fn flow_imbalance(&self) -> f64 {
        let total_buy: f64 = self.flow_history.iter().map(|e| e.buy_vol).sum();
        let total_sell: f64 = self.flow_history.iter().map(|e| e.sell_vol).sum();
        let total = total_buy + total_sell;
        if total < 1e-9 {
            return 0.0;
        }
        (total_buy - total_sell) / total
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

// ---------------------------------------------------------------------------
// Strategy impl
// ---------------------------------------------------------------------------
impl Strategy for QueueFarmerV2 {
    fn name(&self) -> &str {
        "queue_farmer_v2"
    }

    fn on_orderbook_update(
        &mut self,
        ob: &OrderBook,
        port: &Portfolio,
        ts: f64,
    ) -> Vec<OrderIntent> {
        let mut intents = Vec::new();

        let micro = match Self::micro_price(ob) {
            Some(m) => m,
            None => return intents,
        };
        let mid = ob.mid_price().unwrap_or(micro);

        // --- Update flow history ---
        let (buy_vol, sell_vol) = self.infer_flow(ob);
        self.flow_history.push_back(FlowEvent {
            ts,
            buy_vol,
            sell_vol,
        });
        while self
            .flow_history
            .front()
            .map_or(false, |e| e.ts < ts - self.flow_window_sec)
        {
            self.flow_history.pop_front();
        }
        self.prev_best_bid = ob.bids.first().map(|(p, _)| *p);
        self.prev_best_ask = ob.asks.first().map(|(p, _)| *p);

        let inv = Self::inv_pct(port, mid);

        // -----------------------------------------------------------------------
        // 1. Hard inventory stop (last resort)
        // -----------------------------------------------------------------------
        // Skip stop_short until we've drifted short from selling — not just low inv from one-sided buys.
        let stop_long = inv > self.inventory_stop_pct;
        let stop_short = inv < (1.0 - self.inventory_stop_pct)
            && self.total_maker_fills >= self.inventory_stop_min_fills
            && self.total_sell_fills >= self.inventory_stop_min_sell_fills;

        if stop_long || stop_short {
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));

            let in_cooldown = ts < self.inventory_stop_cooldown_until;
            if in_cooldown {
                return intents;
            }

            if !self.inventory_stop_active {
                self.inventory_stop_active = true;
                self.inventory_stop_cooldown_until = ts + self.inventory_stop_cooldown_sec;
                self.total_taker_stops += 1;

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
                    eprintln!(
                        "[STOP] ts={:.0} inv={:.1}% side={:?} amount={} n={}",
                        ts,
                        inv * 100.0,
                        side,
                        amount,
                        self.total_taker_stops
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
            self.inventory_stop_active = false;
        }

        // -----------------------------------------------------------------------
        // 2. Flow imbalance filter — pull quotes on directional aggression
        //    This is what stops you getting run over in pump/dumps.
        //    Fires *during* the move, not after (unlike a mid-price impulse filter).
        // -----------------------------------------------------------------------
        let imbalance = self.flow_imbalance();

        if imbalance.abs() > self.flow_imbalance_threshold && ts >= self.flow_cooldown_until {
            self.flow_cooldown_until = ts + self.flow_cooldown_sec;
            self.total_flow_pauses += 1;
            eprintln!(
                "[FLOW] ts={:.0} imbalance={:.2} cooling {:.0}s n={}",
                ts, imbalance, self.flow_cooldown_sec, self.total_flow_pauses
            );
        }

        if ts < self.flow_cooldown_until {
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            return intents;
        }

        // -----------------------------------------------------------------------
        // 3. Throttle — only refresh quotes at order_refresh_sec intervals
        // -----------------------------------------------------------------------
        if ts - self.last_refresh_ts < self.order_refresh_sec {
            return intents;
        }
        self.last_refresh_ts = ts;

        // -----------------------------------------------------------------------
        // 4. Compute quotes: skewed around micro-price
        // -----------------------------------------------------------------------
        let spread_bps = self.active_spread_bps();
        let spread_dec = Decimal::from_f64_retain(spread_bps / 10000.0).unwrap_or(dec!(0.0002));

        // Skew: shift effective mid away from current inventory lean
        // Long (inv > 0.5): shift DOWN → our ask gets closer to market, more likely to sell
        // Short (inv < 0.5): shift UP → our bid gets closer to market, more likely to buy
        let skew_bps = ((inv - 0.5) * self.skew_sensitivity * 100.0)
            .clamp(-self.max_skew_bps, self.max_skew_bps);
        let skew_dec = Decimal::from_f64_retain(skew_bps / 10000.0).unwrap_or(Decimal::ZERO);
        let effective_mid = micro * (dec!(1) - skew_dec);

        // Soft gates: don't add to already-skewed inventory passively
        let bid_allowed = inv < 0.60;
        let ask_allowed = inv > 0.40;

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
        if fill.layer == 0 {
            return; // taker stop, not a maker fill
        }
        self.total_maker_fills += 1;
        match fill.side {
            OrderSide::Buy => self.total_buy_fills += 1,
            OrderSide::Sell => self.total_sell_fills += 1,
        }
        eprintln!(
            "[FILL] ts={:.0} side={:?} price={} amt={} n={}",
            ts, fill.side, fill.price, fill.amount, self.total_maker_fills
        );
    }

    fn validate_config(&self) -> Result<(), StrategyError> {
        if self.order_amount <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "order_amount must be > 0".into(),
            ));
        }
        if self.spread_bps <= 0.0 || self.backtest_spread_bps <= 0.0 {
            return Err(StrategyError::InvalidConfig(
                "spread_bps must be > 0".into(),
            ));
        }
        if !(0.5 < self.inventory_stop_pct && self.inventory_stop_pct < 1.0) {
            return Err(StrategyError::InvalidConfig(
                "inventory_stop_pct must be in (0.5, 1.0)".into(),
            ));
        }
        if !(0.0 < self.flow_imbalance_threshold && self.flow_imbalance_threshold < 1.0) {
            return Err(StrategyError::InvalidConfig(
                "flow_imbalance_threshold must be in (0.0, 1.0)".into(),
            ));
        }
        Ok(())
    }
}
