//! QueueFarmer v3.0 — Crypto.com ETH/USDT, Japan Colo
//!
//! Major change vs v2: flow filter removed entirely.
//! It was measuring price deltas between snapshots and calling that "volume" —
//! producing ±1.0 imbalance constantly on normal quote noise, keeping us flat
//! ~100% of the time. Snapshot data cannot support flow inference.
//!
//! What we CAN compute reliably within a single snapshot:
//!   - Mid price / micro-price
//!   - Spread (bid-ask width)
//!   - Order book imbalance (qty on bid vs ask at top N levels)
//!   - Our own inventory
//!
//! Pump/dump defense without flow data:
//!   Book imbalance at the top of the book is a useful snapshot-safe signal.
//!   When 80%+ of qty at the top 3 levels sits on one side, that's a sign
//!   someone is spoofing or the book is heavily skewed pre-move. Pull quotes.
//!   This isn't as good as trade tape, but it's honest about what we have.
//!
//! Strategy logic:
//!   1. Compute micro-price from top-of-book quantities
//!   2. Check book imbalance (top N levels) — pause if extreme
//!   3. Skew quotes toward reducing inventory imbalance
//!   4. Hard inventory stop at 65/35 as last resort
//!
//! Parameters to tune:
//!   spread_bps              — 2.0 live, 3.5 backtest
//!   book_imbalance_threshold — 0.80 = pause when 80%+ qty is one-sided
//!   book_imbalance_levels    — how many levels to look at (3 is enough)
//!   inventory_stop_pct       — 0.65
//!   skew_sensitivity         — 6.0 (bps per 1% inventory deviation)

use mm_core::strategy::{Fill, OrderIntent, OrderType, Strategy, StrategyError};
use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::Portfolio;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;

pub struct QueueFarmerV3 {
    // --- Config ---
    order_amount: Decimal,
    spread_bps: f64,
    backtest_spread_bps: f64,
    is_backtest: bool,

    skew_sensitivity: f64,
    max_skew_bps: f64,

    inventory_stop_pct: f64,
    inventory_stop_cooldown_sec: f64,

    // Book imbalance filter (snapshot-safe pump defense)
    book_imbalance_threshold: f64, // 0.80 = pause when top levels 80%+ one-sided
    book_imbalance_levels: usize,  // how many price levels to include

    order_refresh_sec: f64,
    warmup_sec: f64,

    // --- State ---
    start_ts: Option<f64>,
    last_refresh_ts: f64,
    inventory_stop_active: bool,
    inventory_stop_cooldown_until: f64,

    // --- Diagnostics ---
    total_maker_fills: u64,
    total_taker_stops: u64,
    total_imbalance_pauses: u64,
}

impl QueueFarmerV3 {
    pub fn new(order_amount: Decimal) -> Self {
        Self {
            order_amount,
            spread_bps: 1.0,          // Price-improving vs 0.03 bps real spread → queue_pos = 50% of touch
            backtest_spread_bps: 1.0, // Same in backtest — we want to simulate price-improving fills
            is_backtest: false,

            skew_sensitivity: 6.0,
            max_skew_bps: 10.0,       // Tighter cap: at 1 bps spread, 10 bps skew is already 10x the spread

            inventory_stop_pct: 0.65,
            inventory_stop_cooldown_sec: 60.0,

            book_imbalance_threshold: 0.85, // Slightly more lenient — at tight spreads we want more fill opportunities
            book_imbalance_levels: 3,

            order_refresh_sec: 0.5,
            warmup_sec: 30.0,

            start_ts: None,
            last_refresh_ts: 0.0,
            inventory_stop_active: false,
            inventory_stop_cooldown_until: 0.0,

            total_maker_fills: 0,
            total_taker_stops: 0,
            total_imbalance_pauses: 0,
        }
    }

    pub fn with_backtest_mode(mut self) -> Self {
        self.is_backtest = true;
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

    pub fn with_book_imbalance(mut self, threshold: f64, levels: usize) -> Self {
        self.book_imbalance_threshold = threshold;
        self.book_imbalance_levels = levels;
        self
    }

    pub fn with_warmup(mut self, warmup_sec: f64) -> Self {
        self.warmup_sec = warmup_sec;
        self
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn active_spread_bps(&self) -> f64 {
        if self.is_backtest { self.backtest_spread_bps } else { self.spread_bps }
    }

    fn micro_price(ob: &OrderBook) -> Option<Decimal> {
        let (best_bid, bid_qty) = ob.bids.first().map(|(p, q)| (*p, *q))?;
        let (best_ask, ask_qty) = ob.asks.first().map(|(p, q)| (*p, *q))?;
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO {
            return Some((best_bid + best_ask) / dec!(2));
        }
        Some((best_bid * ask_qty + best_ask * bid_qty) / total)
    }

    fn inv_pct(port: &Portfolio, mid: Decimal) -> f64 {
        let base_val = port.base_balance * mid;
        let total = base_val + port.quote_balance;
        if total <= Decimal::ZERO { return 0.5; }
        (base_val / total).to_f64().unwrap_or(0.5).clamp(0.0, 1.0)
    }

    /// Book imbalance from top N levels: +1.0 = all qty on bid, -1.0 = all qty on ask.
    /// This is computable from a single snapshot and reflects structural book skew,
    /// not flow. Less reactive than trade data, but honest about what we have.
    fn book_imbalance(&self, ob: &OrderBook) -> f64 {
        let bid_qty: Decimal = ob.bids.iter()
            .take(self.book_imbalance_levels)
            .map(|(_, q)| *q)
            .sum();
        let ask_qty: Decimal = ob.asks.iter()
            .take(self.book_imbalance_levels)
            .map(|(_, q)| *q)
            .sum();
        let total = bid_qty + ask_qty;
        if total <= Decimal::ZERO { return 0.0; }
        ((bid_qty - ask_qty) / total).to_f64().unwrap_or(0.0)
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

impl Strategy for QueueFarmerV3 {
    fn name(&self) -> &str { "queue_farmer_v3" }

    fn on_orderbook_update(
        &mut self,
        ob: &OrderBook,
        port: &Portfolio,
        ts: f64,
    ) -> Vec<OrderIntent> {
        let mut intents = Vec::new();

        let start_ts = *self.start_ts.get_or_insert(ts);
        let warmed_up = (ts - start_ts) >= self.warmup_sec;

        let micro = match Self::micro_price(ob) {
            Some(m) => m,
            None => return intents,
        };
        let mid = ob.mid_price().unwrap_or(micro);
        let inv = Self::inv_pct(port, mid);

        // -----------------------------------------------------------------------
        // 1. Hard inventory stop — only after warm-up
        // -----------------------------------------------------------------------
        if warmed_up {
            let stop_long  = inv > self.inventory_stop_pct;
            let stop_short = inv < (1.0 - self.inventory_stop_pct);

            if stop_long || stop_short {
                intents.push(Self::cancel(OrderSide::Buy, 1));
                intents.push(Self::cancel(OrderSide::Sell, 1));

                if ts < self.inventory_stop_cooldown_until {
                    return intents;
                }

                if !self.inventory_stop_active {
                    self.inventory_stop_active = true;
                    self.inventory_stop_cooldown_until = ts + self.inventory_stop_cooldown_sec;
                    self.total_taker_stops += 1;

                    let total_base_equiv = port.base_balance + port.quote_balance / mid;
                    let target_base = total_base_equiv * dec!(0.5);
                    let (side, amount) = if stop_long {
                        (OrderSide::Sell, (port.base_balance - target_base).max(Decimal::ZERO))
                    } else {
                        (OrderSide::Buy, (target_base - port.base_balance).max(Decimal::ZERO))
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
                return intents;
            } else {
                self.inventory_stop_active = false;
            }
        }

        // -----------------------------------------------------------------------
        // 2. Book imbalance filter (snapshot-safe)
        //    Heavy one-sided qty at top of book = structural skew, not mean-reverting.
        //    Don't quote into it — we'll be on the wrong side of the next print.
        //    Unlike the flow filter, this fires for one tick only (no cooldown needed
        //    — it re-evaluates fresh each snapshot).
        // -----------------------------------------------------------------------
        let imbalance = self.book_imbalance(ob);
        if imbalance.abs() > self.book_imbalance_threshold {
            self.total_imbalance_pauses += 1;
            intents.push(Self::cancel(OrderSide::Buy, 1));
            intents.push(Self::cancel(OrderSide::Sell, 1));
            return intents;
        }

        // -----------------------------------------------------------------------
        // 3. Throttle refreshes
        // -----------------------------------------------------------------------
        if ts - self.last_refresh_ts < self.order_refresh_sec {
            return intents;
        }
        self.last_refresh_ts = ts;

        // -----------------------------------------------------------------------
        // 4. Compute and place quotes
        // -----------------------------------------------------------------------
        let spread_bps = self.active_spread_bps();
        let spread_dec = Decimal::from_f64_retain(spread_bps / 10000.0).unwrap_or(dec!(0.0002));

        // Inventory skew: shift effective mid to lean against our position.
        // Long (inv > 0.5): shift DOWN → ask closer to market, more sell fills.
        // Short (inv < 0.5): shift UP → bid closer to market, more buy fills.
        let skew_bps = ((inv - 0.5) * self.skew_sensitivity * 100.0)
            .clamp(-self.max_skew_bps, self.max_skew_bps);
        let skew_dec = Decimal::from_f64_retain(skew_bps / 10000.0).unwrap_or(Decimal::ZERO);
        let effective_mid = micro * (dec!(1) - skew_dec);

        // Soft gates: don't add to an already-skewed position passively
        let bid_allowed = inv < 0.60;
        let ask_allowed = inv > 0.40;

        if bid_allowed {
            intents.push(OrderIntent {
                side:       OrderSide::Buy,
                price:      effective_mid * (dec!(1) - spread_dec),
                amount:     self.order_amount,
                order_type: OrderType::Limit,
                layer:      1,
            });
        } else {
            intents.push(Self::cancel(OrderSide::Buy, 1));
        }

        if ask_allowed {
            intents.push(OrderIntent {
                side:       OrderSide::Sell,
                price:      effective_mid * (dec!(1) + spread_dec),
                amount:     self.order_amount,
                order_type: OrderType::Limit,
                layer:      1,
            });
        } else {
            intents.push(Self::cancel(OrderSide::Sell, 1));
        }

        intents
    }

    fn on_fill(&mut self, fill: &Fill, _port: &mut Portfolio, _ts: f64) {
        if fill.layer == 0 { return; }
        self.total_maker_fills += 1;
    }

    fn validate_config(&self) -> Result<(), StrategyError> {
        if self.order_amount <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig("order_amount must be > 0".into()));
        }
        if self.spread_bps <= 0.0 || self.backtest_spread_bps <= 0.0 {
            return Err(StrategyError::InvalidConfig("spread_bps must be > 0".into()));
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
        if self.book_imbalance_levels == 0 {
            return Err(StrategyError::InvalidConfig(
                "book_imbalance_levels must be >= 1".into(),
            ));
        }
        Ok(())
    }
}
