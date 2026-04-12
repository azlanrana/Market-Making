use mm_core_types::{Fill, FillReason, Order, OrderBookSnapshot, OrderStatus, OrderType, Side};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{BTreeMap, HashMap};

use crate::SimulatedOrder;

const EPSILON: Decimal = dec!(0.0000000001);

#[derive(Debug, Clone, Copy, Default)]
pub struct QueueChurnStats {
    pub cancel_ahead_advance_events: u64,
    pub cancel_ahead_advance_total: Decimal,
}

/// Queue model configuration for realistic fill simulation.
#[derive(Debug, Clone)]
pub struct QueueModelConfig {
    /// When price-improving (no L2 level at our price), assume we're behind this fraction of best level size.
    pub price_improving_queue_pct: f64,
    /// When matching the touch (our price = existing L2 level), assume this fraction of level is ahead of us.
    /// 1.0 = back of queue (default). 0.2 = colo: only 20% ahead, we fill much more often.
    pub touch_queue_pct: f64,
    pub queue_decay_enabled: bool,
    /// Enable fills from inferred queue depletion when the touch price is unchanged.
    pub queue_depletion_enabled: bool,
    /// Enable conservative queue advancement from inferred cancel/repost churn at unchanged touch.
    pub queue_churn_enabled: bool,
    /// Fraction of observed size decrease to treat as traded volume instead of cancels.
    pub delta_trade_fraction: f64,
    /// Ignore tiny size deltas to avoid noisy depletion fills.
    pub min_delta_for_fill: f64,
    /// Heuristic external queue turnover rate used to infer latent churn from elapsed time.
    pub queue_turnover_rate_per_sec: f64,
    /// Fraction of latent turnover assumed to happen ahead of our queue position.
    pub cancel_ahead_fraction: f64,
    /// Fraction of visible crossed-book liquidity assumed to survive long enough to execute.
    pub crossed_book_survival_rate: f64,
    /// Enable fills from inferred crossed-book depth removal. Disable for tape-validation diagnostics.
    pub crossed_book_fill_enabled: bool,
    /// For buy limits **inside the spread** with no bid ladder level at our exact price, scale inferred
    /// best-bid depletion by this factor (0 = off). Lets passive `best_ask - tick` quotes receive maker fills.
    pub price_improving_bid_depletion_blend: f64,
    /// Symmetric for sell limits inside the spread (no exact ask level), scaled from best-ask depletion.
    pub price_improving_ask_depletion_blend: f64,
}

impl Default for QueueModelConfig {
    fn default() -> Self {
        Self {
            price_improving_queue_pct: 0.5,
            touch_queue_pct: 1.0,
            queue_decay_enabled: false,
            queue_depletion_enabled: false,
            queue_churn_enabled: false,
            delta_trade_fraction: 0.5,
            min_delta_for_fill: 0.001,
            queue_turnover_rate_per_sec: 0.0,
            cancel_ahead_fraction: 1.0,
            crossed_book_survival_rate: 1.0,
            crossed_book_fill_enabled: true,
            price_improving_bid_depletion_blend: 0.35,
            price_improving_ask_depletion_blend: 0.35,
        }
    }
}

/// Order matching engine for backtesting.
/// Tracks order lifecycle, fills at maker limit price, walks book for market orders.
pub struct MatchingEngine {
    orders: HashMap<String, SimulatedOrder>,
    bids_by_price: BTreeMap<Decimal, Vec<String>>,
    asks_by_price: BTreeMap<Decimal, Vec<String>>,
    order_prices: HashMap<String, Decimal>,
    queue_config: QueueModelConfig,
    prev_snapshot: Option<OrderBookSnapshot>,
    queue_churn_stats: QueueChurnStats,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            bids_by_price: BTreeMap::new(),
            asks_by_price: BTreeMap::new(),
            order_prices: HashMap::new(),
            queue_config: QueueModelConfig::default(),
            prev_snapshot: None,
            queue_churn_stats: QueueChurnStats::default(),
        }
    }

    pub fn with_queue_config(mut self, config: QueueModelConfig) -> Self {
        self.queue_config = config;
        self
    }

    /// Submit an order. Returns immediate fills for Market orders.
    pub fn submit(&mut self, order: Order, book: &OrderBookSnapshot) -> Vec<Fill> {
        match order.order_type {
            OrderType::Market => self.fill_market(order, book),
            OrderType::Limit => {
                let queue = self.market_queue_at_price(book, order.price, order.side);
                let sim_order = SimulatedOrder::new(order.clone(), queue);
                self.add_limit_order(sim_order);
                Vec::new()
            }
            OrderType::Cancel => {
                self.cancel(&order.id);
                Vec::new()
            }
        }
    }

    /// Process book update against resting limit orders. Returns fills (at most one per order per call).
    ///
    /// **Maker fill semantics:**
    ///
    /// - **Queue depletion** walks **each** resting bid/ask price (not only touch): L2 size drops at that price
    ///   (or, for inside-spread quotes with no ladder level, a **blend** of best-bid / best-ask depletion via
    ///   `price_improving_*_depletion_blend`).
    /// - **Crossed-book** unchanged (ask removal `<=` bid for buys, etc.).
    pub fn process_book_update(&mut self, book: &OrderBookSnapshot) -> Vec<Fill> {
        let mut fills = Vec::new();
        if self.queue_config.queue_depletion_enabled {
            fills.extend(self.process_queue_depletion_fills(book));
        }
        if self.queue_config.crossed_book_fill_enabled {
            fills.extend(self.process_buy_fills(book));
            fills.extend(self.process_sell_fills(book));
        }
        self.prev_snapshot = Some(book.clone());
        fills
    }

    pub fn cancel(&mut self, order_id: &str) -> Vec<Fill> {
        if let Some((price, side)) = self.remove_order(order_id) {
            self.update_queue_positions(price, side);
        }
        Vec::new()
    }

    pub fn get_order(&self, id: &str) -> Option<&SimulatedOrder> {
        self.orders.get(id)
    }

    pub fn queue_churn_stats(&self) -> QueueChurnStats {
        self.queue_churn_stats
    }

    fn market_queue_at_price(&self, book: &OrderBookSnapshot, price: Decimal, side: Side) -> Decimal {
        let levels = match side {
            Side::Buy => &book.bids,
            Side::Sell => &book.asks,
        };
        match levels.iter().find(|(p, _)| (*p - price).abs() < dec!(0.0001)) {
            Some((_, qty)) => {
                let pct =
                    Decimal::from_f64_retain(self.queue_config.touch_queue_pct).unwrap_or(dec!(1));
                *qty * pct
            }
            None => {
                if self.is_inside_spread_price_improving(book, price, side) {
                    return Decimal::ZERO;
                }
                let best_level_size = levels.first().map(|(_, q)| *q).unwrap_or(Decimal::ZERO);
                best_level_size
                    * Decimal::from_f64_retain(self.queue_config.price_improving_queue_pct)
                        .unwrap_or(dec!(0.5))
            }
        }
    }

    fn is_inside_spread_price_improving(
        &self,
        book: &OrderBookSnapshot,
        price: Decimal,
        side: Side,
    ) -> bool {
        let (best_bid, best_ask) = match (book.bids.first(), book.asks.first()) {
            (Some((best_bid, _)), Some((best_ask, _))) => (*best_bid, *best_ask),
            _ => return false,
        };

        match side {
            Side::Buy => price > best_bid && price < best_ask,
            Side::Sell => price < best_ask && price > best_bid,
        }
    }

    fn add_limit_order(&mut self, sim_order: SimulatedOrder) {
        let order_id = sim_order.order.id.clone();
        let price = sim_order.order.price;
        let side = sim_order.order.side;
        let ts = sim_order.order.created_ts;

        self.orders.insert(order_id.clone(), sim_order);
        self.order_prices.insert(order_id.clone(), price);

        match side {
            Side::Buy => {
                let list = self.bids_by_price.entry(price).or_insert_with(Vec::new);
                if let Some(pos) = list.iter().position(|id| {
                    self.orders
                        .get(id)
                        .map(|o| o.order.created_ts > ts)
                        .unwrap_or(false)
                }) {
                    list.insert(pos, order_id);
                } else {
                    list.push(order_id);
                }
            }
            Side::Sell => {
                let list = self.asks_by_price.entry(price).or_insert_with(Vec::new);
                if let Some(pos) = list.iter().position(|id| {
                    self.orders
                        .get(id)
                        .map(|o| o.order.created_ts > ts)
                        .unwrap_or(false)
                }) {
                    list.insert(pos, order_id);
                } else {
                    list.push(order_id);
                }
            }
        }

        self.update_queue_positions(price, side);
    }

    fn remove_order(&mut self, order_id: &str) -> Option<(Decimal, Side)> {
        let (price, side) = {
            let order = self.orders.get(order_id)?;
            let price = *self.order_prices.get(order_id)?;
            (price, order.order.side)
        };

        self.orders.remove(order_id);
        self.order_prices.remove(order_id);

        match side {
            Side::Buy => {
                if let Some(list) = self.bids_by_price.get_mut(&price) {
                    list.retain(|id| id != order_id);
                    if list.is_empty() {
                        self.bids_by_price.remove(&price);
                    }
                }
            }
            Side::Sell => {
                if let Some(list) = self.asks_by_price.get_mut(&price) {
                    list.retain(|id| id != order_id);
                    if list.is_empty() {
                        self.asks_by_price.remove(&price);
                    }
                }
            }
        }
        Some((price, side))
    }

    fn fill_market(&mut self, order: Order, book: &OrderBookSnapshot) -> Vec<Fill> {
        let mut fills = Vec::new();
        let mut remaining = order.amount;

        match order.side {
            Side::Buy => {
                for (price, level_qty) in &book.asks {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    let fill_qty = remaining.min(*level_qty);
                    if fill_qty > Decimal::ZERO {
                        fills.push(Fill {
                            order_id: order.id.clone(),
                            side: Side::Buy,
                            price: *price,
                            amount: fill_qty,
                            remaining: remaining - fill_qty,
                            is_taker: true,
                            fill_reason: None,
                            timestamp: book.timestamp,
                            layer: order.layer,
                        });
                        remaining -= fill_qty;
                    }
                }
            }
            Side::Sell => {
                for (price, level_qty) in &book.bids {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    let fill_qty = remaining.min(*level_qty);
                    if fill_qty > Decimal::ZERO {
                        fills.push(Fill {
                            order_id: order.id.clone(),
                            side: Side::Sell,
                            price: *price,
                            amount: fill_qty,
                            remaining: remaining - fill_qty,
                            is_taker: true,
                            fill_reason: None,
                            timestamp: book.timestamp,
                            layer: order.layer,
                        });
                        remaining -= fill_qty;
                    }
                }
            }
        }
        fills
    }

    fn process_queue_depletion_fills(&mut self, book: &OrderBookSnapshot) -> Vec<Fill> {
        let mut fills = Vec::new();
        let prev = match self.prev_snapshot.clone() {
            Some(prev) => prev,
            None => return fills,
        };
        fills.extend(self.process_buy_queue_depletion(&prev, book));
        fills.extend(self.process_sell_queue_depletion(&prev, book));
        fills
    }

    /// Inferred traded volume (post `delta_trade_fraction`) eligible for queue-depletion fills on a **buy** at `p`.
    fn bid_depletion_traded_volume(
        &self,
        prev: &OrderBookSnapshot,
        book: &OrderBookSnapshot,
        p: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        prev_best_bid: Decimal,
    ) -> Option<Decimal> {
        let min_d = Self::min_delta_decimal(&self.queue_config);
        let blend = Decimal::from_f64_retain(self.queue_config.price_improving_bid_depletion_blend)
            .unwrap_or(dec!(0.35))
            .clamp(Decimal::ZERO, Decimal::ONE);

        if p == best_bid {
            if prev_best_bid != best_bid {
                return None;
            }
            let t = self.inferred_trade_delta(&prev.bids, &book.bids, p);
            return Some(if t > min_d { t } else { Decimal::ZERO });
        }

        if p < best_bid {
            let prev_q = Self::level_quantity(&prev.bids, p);
            let curr_q = Self::level_quantity(&book.bids, p);
            if prev_q <= Decimal::ZERO && curr_q <= Decimal::ZERO {
                return None;
            }
            let t = self.inferred_trade_delta(&prev.bids, &book.bids, p);
            return Some(if t > min_d { t } else { Decimal::ZERO });
        }

        // Inside spread: best_bid < p < best_ask — require stable touch so a repriced best bid
        // does not look like tradable depletion at our (former) price level.
        if p <= best_bid || p >= best_ask {
            return None;
        }
        if prev_best_bid != best_bid {
            return None;
        }

        let prev_q = Self::level_quantity(&prev.bids, p);
        let curr_q = Self::level_quantity(&book.bids, p);
        if prev_q > Decimal::ZERO || curr_q > Decimal::ZERO {
            let t = self.inferred_trade_delta(&prev.bids, &book.bids, p);
            return Some(if t > min_d { t } else { Decimal::ZERO });
        }

        // Price-improving bid (no visible bid level at p): share of touch depletion
        let base = self.inferred_trade_delta(&prev.bids, &book.bids, best_bid);
        if base <= min_d {
            return Some(Decimal::ZERO);
        }
        let scaled = base * blend;
        Some(if scaled > min_d { scaled } else { Decimal::ZERO })
    }

    fn has_active_buy_at_price(&self, price: Decimal) -> bool {
        self.bids_by_price.get(&price).map_or(false, |ids| {
            ids.iter()
                .any(|id| self.orders.get(id).map(|s| s.order.is_active()).unwrap_or(false))
        })
    }

    fn has_active_sell_at_price(&self, price: Decimal) -> bool {
        self.asks_by_price.get(&price).map_or(false, |ids| {
            ids.iter()
                .any(|id| self.orders.get(id).map(|s| s.order.is_active()).unwrap_or(false))
        })
    }

    fn process_buy_queue_depletion(
        &mut self,
        prev: &OrderBookSnapshot,
        book: &OrderBookSnapshot,
    ) -> Vec<Fill> {
        let mut fills = Vec::new();
        let (best_bid, best_ask) = match (book.bids.first(), book.asks.first()) {
            (Some((bid, _)), Some((ask, _))) => (*bid, *ask),
            _ => return fills,
        };
        let prev_best_bid = match prev.bids.first() {
            Some((price, _)) => *price,
            None => return fills,
        };

        if best_bid >= best_ask {
            return fills;
        }

        let min_d = Self::min_delta_decimal(&self.queue_config);

        let price_levels: Vec<Decimal> = self
            .bids_by_price
            .iter()
            .rev()
            .filter_map(|(p, ids)| {
                let active = ids.iter().any(|id| {
                    self.orders
                        .get(id)
                        .map(|s| s.order.is_active())
                        .unwrap_or(false)
                });
                active.then_some(*p)
            })
            .collect();

        let mut to_remove = Vec::new();

        'price_loop: for p in price_levels {
            let traded_for_fill = match self.bid_depletion_traded_volume(
                prev,
                book,
                p,
                best_bid,
                best_ask,
                prev_best_bid,
            ) {
                Some(t) if t > min_d => t,
                _ => continue,
            };

            let order_ids = match self.bids_by_price.get(&p) {
                Some(ids) => ids.clone(),
                None => continue,
            };

            let mut states = Vec::new();
            for order_id in &order_ids {
                if let Some(sim) = self.orders.get(order_id) {
                    if sim.order.is_active() {
                        states.push((
                            order_id.clone(),
                            sim.market_queue_ahead,
                            sim.internal_queue_ahead,
                            sim.remaining_amount(),
                        ));
                    }
                }
            }

            if traded_for_fill > Decimal::ZERO {
                self.reduce_market_queue_at_price(p, Side::Buy, traded_for_fill);
            }

            if traded_for_fill > Decimal::ZERO {
                for (order_id, market_ahead_before, internal_ahead_before, remaining_before) in states {
                    if remaining_before <= Decimal::ZERO {
                        continue;
                    }

                    let queue_ahead_before = market_ahead_before + internal_ahead_before;
                    if traded_for_fill <= queue_ahead_before {
                        continue;
                    }

                    let fill_amt = (traded_for_fill - queue_ahead_before).min(remaining_before);
                    if fill_amt <= EPSILON {
                        continue;
                    }

                    if let Some(fill) = self.apply_fill(
                        &order_id,
                        fill_amt,
                        p,
                        book.timestamp,
                        FillReason::QueueDepletion,
                    ) {
                        if let Some(sim) = self.orders.get(&order_id) {
                            if sim.order.status == OrderStatus::Filled {
                                to_remove.push(order_id.clone());
                            }
                        }
                        fills.push(fill);
                        self.update_queue_positions(p, Side::Buy);
                        break 'price_loop;
                    }
                }
            }
        }

        for id in to_remove {
            self.remove_order(&id);
        }

        if self.has_active_buy_at_price(best_bid) {
            let cancel_ahead = self.inferred_cancel_ahead_churn(
                &prev.bids,
                &book.bids,
                best_bid,
                prev.timestamp,
                book.timestamp,
            );
            self.apply_cancel_ahead_advance(best_bid, Side::Buy, cancel_ahead);
        }

        fills
    }

    fn ask_depletion_traded_volume(
        &self,
        prev: &OrderBookSnapshot,
        book: &OrderBookSnapshot,
        p: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        prev_best_ask: Decimal,
    ) -> Option<Decimal> {
        let min_d = Self::min_delta_decimal(&self.queue_config);
        let blend = Decimal::from_f64_retain(self.queue_config.price_improving_ask_depletion_blend)
            .unwrap_or(dec!(0.35))
            .clamp(Decimal::ZERO, Decimal::ONE);

        if p == best_ask {
            if prev_best_ask != best_ask {
                return None;
            }
            let t = self.inferred_trade_delta(&prev.asks, &book.asks, p);
            return Some(if t > min_d { t } else { Decimal::ZERO });
        }

        if p > best_ask {
            let prev_q = Self::level_quantity(&prev.asks, p);
            let curr_q = Self::level_quantity(&book.asks, p);
            if prev_q <= Decimal::ZERO && curr_q <= Decimal::ZERO {
                return None;
            }
            let t = self.inferred_trade_delta(&prev.asks, &book.asks, p);
            return Some(if t > min_d { t } else { Decimal::ZERO });
        }

        // Inside spread: best_bid < p < best_ask (stable best ask)
        if p <= best_bid || p >= best_ask {
            return None;
        }
        if prev_best_ask != best_ask {
            return None;
        }

        let prev_q = Self::level_quantity(&prev.asks, p);
        let curr_q = Self::level_quantity(&book.asks, p);
        if prev_q > Decimal::ZERO || curr_q > Decimal::ZERO {
            let t = self.inferred_trade_delta(&prev.asks, &book.asks, p);
            return Some(if t > min_d { t } else { Decimal::ZERO });
        }
        let base = self.inferred_trade_delta(&prev.asks, &book.asks, best_ask);
        if base <= min_d {
            return Some(Decimal::ZERO);
        }
        let scaled = base * blend;
        Some(if scaled > min_d { scaled } else { Decimal::ZERO })
    }

    fn process_sell_queue_depletion(
        &mut self,
        prev: &OrderBookSnapshot,
        book: &OrderBookSnapshot,
    ) -> Vec<Fill> {
        let mut fills = Vec::new();
        let (best_bid, best_ask) = match (book.bids.first(), book.asks.first()) {
            (Some((bid, _)), Some((ask, _))) => (*bid, *ask),
            _ => return fills,
        };
        let prev_best_ask = match prev.asks.first() {
            Some((price, _)) => *price,
            None => return fills,
        };

        if best_bid >= best_ask {
            return fills;
        }

        let min_d = Self::min_delta_decimal(&self.queue_config);

        let price_levels: Vec<Decimal> = self
            .asks_by_price
            .iter()
            .filter_map(|(p, ids)| {
                let active = ids.iter().any(|id| {
                    self.orders
                        .get(id)
                        .map(|s| s.order.is_active())
                        .unwrap_or(false)
                });
                active.then_some(*p)
            })
            .collect();

        let mut to_remove = Vec::new();

        'ask_loop: for p in price_levels {
            let traded_for_fill = match self.ask_depletion_traded_volume(
                prev,
                book,
                p,
                best_bid,
                best_ask,
                prev_best_ask,
            ) {
                Some(t) if t > min_d => t,
                _ => continue,
            };

            let order_ids = match self.asks_by_price.get(&p) {
                Some(ids) => ids.clone(),
                None => continue,
            };

            let mut states = Vec::new();
            for order_id in &order_ids {
                if let Some(sim) = self.orders.get(order_id) {
                    if sim.order.is_active() {
                        states.push((
                            order_id.clone(),
                            sim.market_queue_ahead,
                            sim.internal_queue_ahead,
                            sim.remaining_amount(),
                        ));
                    }
                }
            }

            if traded_for_fill > Decimal::ZERO {
                self.reduce_market_queue_at_price(p, Side::Sell, traded_for_fill);
            }

            if traded_for_fill > Decimal::ZERO {
                for (order_id, market_ahead_before, internal_ahead_before, remaining_before) in states {
                    if remaining_before <= Decimal::ZERO {
                        continue;
                    }

                    let queue_ahead_before = market_ahead_before + internal_ahead_before;
                    if traded_for_fill <= queue_ahead_before {
                        continue;
                    }

                    let fill_amt = (traded_for_fill - queue_ahead_before).min(remaining_before);
                    if fill_amt <= EPSILON {
                        continue;
                    }

                    if let Some(fill) = self.apply_fill(
                        &order_id,
                        fill_amt,
                        p,
                        book.timestamp,
                        FillReason::QueueDepletion,
                    ) {
                        if let Some(sim) = self.orders.get(&order_id) {
                            if sim.order.status == OrderStatus::Filled {
                                to_remove.push(order_id.clone());
                            }
                        }
                        fills.push(fill);
                        self.update_queue_positions(p, Side::Sell);
                        break 'ask_loop;
                    }
                }
            }
        }

        for id in to_remove {
            self.remove_order(&id);
        }

        if self.has_active_sell_at_price(best_ask) {
            let cancel_ahead = self.inferred_cancel_ahead_churn(
                &prev.asks,
                &book.asks,
                best_ask,
                prev.timestamp,
                book.timestamp,
            );
            self.apply_cancel_ahead_advance(best_ask, Side::Sell, cancel_ahead);
        }

        fills
    }

    fn process_buy_fills(&mut self, book: &OrderBookSnapshot) -> Vec<Fill> {
        let mut fills = Vec::new();
        let prev = match self.prev_snapshot.clone() {
            Some(prev) => prev,
            None => return fills,
        };

        let bid_levels: Vec<(Decimal, Vec<String>)> = self
            .bids_by_price
            .iter()
            .rev()
            .map(|(p, ids)| (*p, ids.clone()))
            .collect();

        let mut consumed = Decimal::ZERO;
        let mut to_remove: Vec<String> = Vec::new();

        for (bid_price, order_ids) in bid_levels {
            let traded_total = self.inferred_crossed_buy_trade_delta(&prev.asks, &book.asks, bid_price);
            let available = if traded_total > Self::min_delta_decimal(&self.queue_config) {
                traded_total.saturating_sub(consumed)
            } else {
                Decimal::ZERO
            };
            if available <= Decimal::ZERO {
                continue;
            }

            let mut states = Vec::new();
            for order_id in &order_ids {
                if let Some(sim) = self.orders.get(order_id) {
                    if sim.order.is_active() {
                        states.push((
                            order_id.clone(),
                            sim.market_queue_ahead,
                            sim.internal_queue_ahead,
                            sim.remaining_amount(),
                        ));
                    }
                }
            }
            if states.is_empty() {
                consumed += available;
                continue;
            }

            self.reduce_market_queue_at_price(bid_price, Side::Buy, available);
            let mut consumed_at_level = available;

            for (order_id, market_ahead_before, internal_ahead_before, remaining_before) in states {
                if remaining_before <= Decimal::ZERO {
                    continue;
                }

                let queue_ahead_before = market_ahead_before + internal_ahead_before;
                if queue_ahead_before >= available {
                    break;
                }

                let fill_amt = (available - queue_ahead_before).min(remaining_before);
                if fill_amt <= EPSILON {
                    continue;
                }

                if let Some(fill) =
                    self.apply_fill(&order_id, fill_amt, bid_price, book.timestamp, FillReason::CrossedBook)
                {
                    consumed_at_level = (queue_ahead_before + fill.amount).min(available);
                    if let Some(sim) = self.orders.get(&order_id) {
                        if sim.order.status == OrderStatus::Filled {
                            to_remove.push(order_id.clone());
                        }
                    }
                    fills.push(fill);
                    self.update_queue_positions(bid_price, Side::Buy);
                    break;
                }
            }

            consumed += consumed_at_level;
        }

        for id in to_remove {
            self.remove_order(&id);
        }
        fills
    }

    fn process_sell_fills(&mut self, book: &OrderBookSnapshot) -> Vec<Fill> {
        let mut fills = Vec::new();
        let prev = match self.prev_snapshot.clone() {
            Some(prev) => prev,
            None => return fills,
        };

        let ask_levels: Vec<(Decimal, Vec<String>)> = self
            .asks_by_price
            .iter()
            .map(|(p, ids)| (*p, ids.clone()))
            .collect();

        let mut consumed = Decimal::ZERO;
        let mut to_remove: Vec<String> = Vec::new();

        for (ask_price, order_ids) in ask_levels {
            let traded_total = self.inferred_crossed_sell_trade_delta(&prev.bids, &book.bids, ask_price);
            let available = if traded_total > Self::min_delta_decimal(&self.queue_config) {
                traded_total.saturating_sub(consumed)
            } else {
                Decimal::ZERO
            };
            if available <= Decimal::ZERO {
                continue;
            }

            let mut states = Vec::new();
            for order_id in &order_ids {
                if let Some(sim) = self.orders.get(order_id) {
                    if sim.order.is_active() {
                        states.push((
                            order_id.clone(),
                            sim.market_queue_ahead,
                            sim.internal_queue_ahead,
                            sim.remaining_amount(),
                        ));
                    }
                }
            }
            if states.is_empty() {
                consumed += available;
                continue;
            }

            self.reduce_market_queue_at_price(ask_price, Side::Sell, available);
            let mut consumed_at_level = available;

            for (order_id, market_ahead_before, internal_ahead_before, remaining_before) in states {
                if remaining_before <= Decimal::ZERO {
                    continue;
                }

                let queue_ahead_before = market_ahead_before + internal_ahead_before;
                if queue_ahead_before >= available {
                    break;
                }

                let fill_amt = (available - queue_ahead_before).min(remaining_before);
                if fill_amt <= EPSILON {
                    continue;
                }

                if let Some(fill) =
                    self.apply_fill(&order_id, fill_amt, ask_price, book.timestamp, FillReason::CrossedBook)
                {
                    consumed_at_level = (queue_ahead_before + fill.amount).min(available);
                    if let Some(sim) = self.orders.get(&order_id) {
                        if sim.order.status == OrderStatus::Filled {
                            to_remove.push(order_id.clone());
                        }
                    }
                    fills.push(fill);
                    self.update_queue_positions(ask_price, Side::Sell);
                    break;
                }
            }

            consumed += consumed_at_level;
        }

        for id in to_remove {
            self.remove_order(&id);
        }
        fills
    }

    fn apply_fill(
        &mut self,
        order_id: &str,
        fill_amt: Decimal,
        fill_price: Decimal,
        timestamp: f64,
        fill_reason: FillReason,
    ) -> Option<Fill> {
        let sim = self.orders.get_mut(order_id)?;
        if !sim.order.is_active() || fill_amt <= EPSILON {
            return None;
        }

        let remaining_before = sim.remaining_amount();
        let actual_fill = fill_amt.min(remaining_before);
        sim.order.filled_amount += actual_fill;

        let fill = Fill {
            order_id: sim.order.id.clone(),
            side: sim.order.side,
            price: fill_price,
            amount: actual_fill,
            remaining: remaining_before - actual_fill,
            is_taker: false,
            fill_reason: Some(fill_reason),
            timestamp,
            layer: sim.order.layer,
        };

        sim.fills.push(fill.clone());
        sim.order.status = if sim.remaining_amount() <= EPSILON {
            OrderStatus::Filled
        } else {
            OrderStatus::PartiallyFilled
        };

        Some(fill)
    }

    fn inferred_trade_delta(
        &self,
        prev_levels: &[(Decimal, Decimal)],
        curr_levels: &[(Decimal, Decimal)],
        touch_price: Decimal,
    ) -> Decimal {
        let prev_qty = Self::level_quantity(prev_levels, touch_price);
        let curr_qty = Self::level_quantity(curr_levels, touch_price);
        if prev_qty <= curr_qty {
            return Decimal::ZERO;
        }

        let delta = prev_qty - curr_qty;
        delta * self.delta_trade_fraction_decimal()
    }

    fn raw_touch_shrink(
        &self,
        prev_levels: &[(Decimal, Decimal)],
        curr_levels: &[(Decimal, Decimal)],
        touch_price: Decimal,
    ) -> Decimal {
        let prev_qty = Self::level_quantity(prev_levels, touch_price);
        let curr_qty = Self::level_quantity(curr_levels, touch_price);
        prev_qty.saturating_sub(curr_qty)
    }

    fn interval_seconds(&self, prev_ts: f64, curr_ts: f64) -> Decimal {
        Decimal::from_f64_retain((curr_ts - prev_ts).max(0.0)).unwrap_or(Decimal::ZERO)
    }

    fn delta_trade_fraction_decimal(&self) -> Decimal {
        Decimal::from_f64_retain(self.queue_config.delta_trade_fraction).unwrap_or(dec!(0.5))
    }

    fn crossed_book_survival_rate_decimal(&self) -> Decimal {
        Decimal::from_f64_retain(self.queue_config.crossed_book_survival_rate).unwrap_or(dec!(1.0))
    }

    fn inferred_crossed_buy_trade_delta(
        &self,
        prev_asks: &[(Decimal, Decimal)],
        curr_asks: &[(Decimal, Decimal)],
        bid_price: Decimal,
    ) -> Decimal {
        let removed = prev_asks
            .iter()
            .filter(|(price, _)| *price <= bid_price)
            .fold(Decimal::ZERO, |acc, (price, prev_qty)| {
                let curr_qty = Self::level_quantity(curr_asks, *price);
                acc + prev_qty.saturating_sub(curr_qty)
            });
        removed * self.delta_trade_fraction_decimal() * self.crossed_book_survival_rate_decimal()
    }

    fn inferred_crossed_sell_trade_delta(
        &self,
        prev_bids: &[(Decimal, Decimal)],
        curr_bids: &[(Decimal, Decimal)],
        ask_price: Decimal,
    ) -> Decimal {
        let removed = prev_bids
            .iter()
            .filter(|(price, _)| *price >= ask_price)
            .fold(Decimal::ZERO, |acc, (price, prev_qty)| {
                let curr_qty = Self::level_quantity(curr_bids, *price);
                acc + prev_qty.saturating_sub(curr_qty)
            });
        removed * self.delta_trade_fraction_decimal() * self.crossed_book_survival_rate_decimal()
    }

    fn inferred_cancel_ahead_churn(
        &self,
        prev_levels: &[(Decimal, Decimal)],
        curr_levels: &[(Decimal, Decimal)],
        touch_price: Decimal,
        prev_ts: f64,
        curr_ts: f64,
    ) -> Decimal {
        if !self.queue_config.queue_churn_enabled {
            return Decimal::ZERO;
        }

        let dt = self.interval_seconds(prev_ts, curr_ts);
        if dt <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        let raw_shrink = self.raw_touch_shrink(prev_levels, curr_levels, touch_price);
        let trade_delta = self.inferred_trade_delta(prev_levels, curr_levels, touch_price);
        let visible_cancel = raw_shrink.saturating_sub(trade_delta);
        let visible_touch_size = Self::level_quantity(curr_levels, touch_price);
        let turnover_rate = Decimal::from_f64_retain(self.queue_config.queue_turnover_rate_per_sec)
            .unwrap_or(Decimal::ZERO);
        let cancel_ahead_fraction =
            Decimal::from_f64_retain(self.queue_config.cancel_ahead_fraction)
                .unwrap_or(Decimal::ZERO);
        let latent_turnover = visible_touch_size * turnover_rate * dt;
        // Use observed non-trade touch shrink as the minimum queue advancement signal,
        // and fall back to the time-based turnover heuristic when the touch does not visibly shrink.
        let latent_cancel_ahead = visible_cancel.max(latent_turnover.saturating_sub(trade_delta))
            * cancel_ahead_fraction;
        if latent_cancel_ahead <= EPSILON {
            Decimal::ZERO
        } else {
            latent_cancel_ahead
        }
    }

    fn apply_cancel_ahead_advance(&mut self, price: Decimal, side: Side, advance: Decimal) {
        if advance <= Decimal::ZERO {
            return;
        }

        self.reduce_market_queue_at_price(price, side, advance);
        self.queue_churn_stats.cancel_ahead_advance_events += 1;
        self.queue_churn_stats.cancel_ahead_advance_total += advance;
    }

    fn reduce_market_queue_at_price(&mut self, price: Decimal, side: Side, reduction: Decimal) {
        if reduction <= Decimal::ZERO {
            return;
        }
        let ids = match side {
            Side::Buy => self.bids_by_price.get(&price).cloned(),
            Side::Sell => self.asks_by_price.get(&price).cloned(),
        };
        let Some(ids) = ids else {
            return;
        };

        for order_id in ids {
            if let Some(sim) = self.orders.get_mut(&order_id) {
                if sim.order.is_active() {
                    sim.market_queue_ahead = if reduction >= sim.market_queue_ahead {
                        Decimal::ZERO
                    } else {
                        sim.market_queue_ahead - reduction
                    };
                }
            }
        }
    }

    fn level_quantity(levels: &[(Decimal, Decimal)], price: Decimal) -> Decimal {
        levels
            .iter()
            .find(|(level_price, _)| *level_price == price)
            .map(|(_, qty)| *qty)
            .unwrap_or(Decimal::ZERO)
    }

    fn min_delta_decimal(config: &QueueModelConfig) -> Decimal {
        Decimal::from_f64_retain(config.min_delta_for_fill).unwrap_or(Decimal::ZERO)
    }

    fn update_queue_positions(&mut self, price: Decimal, side: Side) {
        let list = match side {
            Side::Buy => self.bids_by_price.get(&price),
            Side::Sell => self.asks_by_price.get(&price),
        };
        let Some(list) = list else {
            return;
        };

        let mut cum = Decimal::ZERO;
        let order_ids = list.clone();
        for order_id in order_ids {
            if let Some(sim) = self.orders.get_mut(&order_id) {
                if sim.order.is_active() {
                    sim.internal_queue_ahead = cum;
                    cum += sim.remaining_amount();
                }
            }
        }
    }
}

impl Default for MatchingEngine {
    fn default() -> Self {
        Self::new()
    }
}
