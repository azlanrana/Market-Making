use mm_core::strategy::{OrderType, Strategy};
use mm_core::market_data::OrderBook as CoreOrderBook;
use mm_core::market_data::OrderSide as CoreOrderSide;
use mm_core::Portfolio as CorePortfolio;
use mm_core_types::{FeeModel, Fill as CoreFill, Order, OrderBookSnapshot, OrderType as CoreOrderType, Side};
use mm_simulator::{MatchingEngine, QueueModelConfig};
use mm_portfolio::Portfolio;
use mm_metrics::{MetricsCollector, MMDashboardSummary};
use orderbook::snapshot::OrderBookSnapshot as ObSnapshot;
use data_loader::DataLoader;
use rust_decimal::Decimal;
use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;

use crate::{SimpleFeeModel, LatencyModel};

/// Pending cancel: (order_id, effective_timestamp when cancel takes effect)
type PendingCancel = (String, f64);
/// Pending order activation: (order, effective_timestamp when order becomes active)
type PendingOrder = (Order, f64);
use crate::round_trip::RoundTripTracker;

fn to_core_snapshot(ob: &ObSnapshot) -> OrderBookSnapshot {
    OrderBookSnapshot::new(ob.timestamp, ob.bids.clone(), ob.asks.clone())
}

fn to_core_orderbook(ob: &ObSnapshot) -> CoreOrderBook {
    CoreOrderBook {
        bids: ob.bids.clone(),
        asks: ob.asks.clone(),
        timestamp: ob.timestamp,
    }
}

fn to_core_portfolio(port: &Portfolio) -> CorePortfolio {
    let cost_basis = if port.base_balance > Decimal::ZERO {
        port.base_balance * port.avg_cost
    } else {
        Decimal::ZERO
    };
    CorePortfolio {
        base_balance: port.base_balance,
        quote_balance: port.quote_balance,
        realized_pnl: port.realized_pnl,
        cost_basis,
    }
}

fn to_core_fill(f: &CoreFill) -> mm_core::strategy::Fill {
    mm_core::strategy::Fill {
        order_id: f.order_id.clone(),
        side: match f.side {
            Side::Buy => CoreOrderSide::Buy,
            Side::Sell => CoreOrderSide::Sell,
        },
        price: f.price,
        amount: f.amount,
        timestamp: f.timestamp,
        layer: f.layer,
    }
}

/// Spread distribution: ticks -> snapshot count (from observed L2 data).
pub type SpreadDistribution = HashMap<u32, u64>;

pub struct BacktestResults {
    pub stats: mm_metrics::BacktestStats,
    /// First snapshot timestamp (unix sec)
    pub first_ts: f64,
    /// Last snapshot timestamp (unix sec)
    pub last_ts: f64,
    pub snapshot_count: u64,
    /// Spread in ticks (1=1 tick, 2=2 ticks) -> number of snapshots
    pub spread_distribution: SpreadDistribution,
    /// Optional gate/filter diagnostics from strategy (e.g. "[GATES] imbalance=...")
    pub gate_diagnostics: Option<String>,
    /// MM monitoring dashboard (fill rate, markout, etc.)
    pub dashboard: Option<MMDashboardSummary>,
    /// Portfolio value over time (timestamp, value) for equity curve / visualization
    pub portfolio_snapshots: Vec<(f64, f64)>,
    /// Per-fill 1s markout / adverse (matches dashboard toxic classifier)
    pub resolved_1s_markouts: Vec<mm_metrics::Markout1sRecord>,
}

#[derive(Debug, Clone)]
pub struct LivePaperSummary {
    pub timestamp: f64,
    pub mid_price: Decimal,
    pub spread_bps: f64,
    pub portfolio_value: Decimal,
    pub open_order_count: usize,
    pub dashboard: MMDashboardSummary,
}

/// Optional callback invoked on each simulated fill (for tape validation).
pub type FillCallback = Box<dyn FnMut(&CoreFill) + Send>;

pub struct BacktestEngine<S: Strategy> {
    strategy: S,
    matcher: MatchingEngine,
    portfolio: Portfolio,
    metrics: MetricsCollector,
    fee_model: SimpleFeeModel,
    tick_size: Decimal,
    initial_capital: Decimal,
    active_orders_by_layer: HashMap<(Side, u32), Vec<String>>,
    order_placement_ts: HashMap<String, f64>,
    latency: Option<LatencyModel>,
    pending_cancels: Vec<PendingCancel>,
    pending_orders: Vec<PendingOrder>,
    round_trip_tracker: RoundTripTracker,
    fill_callback: Option<FillCallback>,
    first_ts: Option<f64>,
    last_ts: f64,
    last_mid: Decimal,
    snapshot_count: u64,
    spread_distribution: SpreadDistribution,
}

impl<S: Strategy> BacktestEngine<S> {
    pub fn new(
        strategy: S,
        initial_capital: Decimal,
        _initial_price: Decimal,
        fee_model: SimpleFeeModel,
        tick_size: Decimal,
    ) -> Self {
        Self {
            strategy,
            matcher: MatchingEngine::new(),
            portfolio: Portfolio::new(Decimal::ZERO, initial_capital),
            metrics: MetricsCollector::new(),
            fee_model,
            tick_size,
            initial_capital,
            active_orders_by_layer: HashMap::new(),
            order_placement_ts: HashMap::new(),
            latency: None,
            pending_cancels: Vec::new(),
            pending_orders: Vec::new(),
            round_trip_tracker: RoundTripTracker::new(),
            fill_callback: None,
            first_ts: None,
            last_ts: 0.0,
            last_mid: Decimal::ZERO,
            snapshot_count: 0,
            spread_distribution: HashMap::new(),
        }
    }

    /// Register a callback invoked on each simulated fill. Used for tape validation.
    pub fn with_fill_callback<F>(mut self, f: F) -> Self
    where
        F: FnMut(&CoreFill) + Send + 'static,
    {
        self.fill_callback = Some(Box::new(f));
        self
    }

    pub fn with_latency(mut self, model: LatencyModel) -> Self {
        self.latency = Some(model);
        self
    }

    /// Use custom queue model (e.g. touch_queue_pct for colo simulation).
    pub fn with_queue_config(mut self, config: QueueModelConfig) -> Self {
        self.matcher = MatchingEngine::new().with_queue_config(config);
        self
    }

    pub fn with_markout_enabled(mut self, enabled: bool) -> Self {
        self.metrics = std::mem::take(&mut self.metrics).with_markout_enabled(enabled);
        self
    }

    fn order_submission_delay_secs(&self) -> Option<f64> {
        let delay_us = self.latency.as_ref()?.order_submission_us;
        if delay_us == 0 {
            None
        } else {
            Some(delay_us as f64 / 1_000_000.0)
        }
    }

    fn cancel_delay_secs(&self) -> Option<f64> {
        let delay_us = self.latency.as_ref()?.cancel_us;
        if delay_us == 0 {
            None
        } else {
            Some(delay_us as f64 / 1_000_000.0)
        }
    }

    fn activate_limit_order(&mut self, order: Order, core_snap: &OrderBookSnapshot) {
        let order_id = order.id.clone();
        let side = order.side;
        let layer = order.layer;
        let limit_fills = self.matcher.submit(order, core_snap);
        debug_assert!(
            limit_fills.is_empty(),
            "limit order activation unexpectedly produced immediate fills"
        );
        if limit_fills.is_empty() {
            self.active_orders_by_layer
                .entry((side, layer))
                .or_default()
                .push(order_id);
        }
    }

    fn schedule_pending_order(&mut self, order: Order, effective_ts: f64) {
        self.order_placement_ts
            .insert(order.id.clone(), order.created_ts);
        self.pending_orders.push((order, effective_ts));
    }

    fn cancel_pending_orders_for_layer(&mut self, side: Side, layer: u32) {
        self.pending_orders.retain(|(order, _)| {
            let matches_layer = order.side == side && order.layer == layer;
            if matches_layer {
                self.order_placement_ts.remove(&order.id);
            }
            !matches_layer
        });
    }

    fn apply_pending_orders(&mut self, timestamp: f64, core_snap: &OrderBookSnapshot) {
        if self.pending_orders.is_empty() {
            return;
        }

        let mut remaining = Vec::with_capacity(self.pending_orders.len());
        let pending = std::mem::take(&mut self.pending_orders);
        for (order, effective_ts) in pending {
            if effective_ts <= timestamp {
                self.activate_limit_order(order, core_snap);
            } else {
                remaining.push((order, effective_ts));
            }
        }
        self.pending_orders = remaining;
    }

    fn apply_pending_cancels(&mut self, timestamp: f64) {
        self.pending_cancels.retain(|(order_id, effective_ts)| {
            if *effective_ts <= timestamp {
                self.matcher.cancel(order_id);
                self.order_placement_ts.remove(order_id);
                false
            } else {
                true
            }
        });
    }

    fn cancel_orders_for_layer(&mut self, side: Side, layer: u32, timestamp: f64) {
        let key = (side, layer);
        if let Some(ids) = self.active_orders_by_layer.remove(&key) {
            if let Some(delay_secs) = self.cancel_delay_secs() {
                let effective_ts = timestamp + delay_secs;
                for id in ids {
                    self.pending_cancels.push((id, effective_ts));
                }
            } else {
                for id in &ids {
                    self.matcher.cancel(id);
                    self.order_placement_ts.remove(id);
                }
            }
        }
        self.cancel_pending_orders_for_layer(side, layer);
    }

    fn has_matching_active_limit_order(
        &self,
        side: Side,
        layer: u32,
        price: Decimal,
        amount: Decimal,
    ) -> bool {
        let Some(ids) = self.active_orders_by_layer.get(&(side, layer)) else {
            return false;
        };
        if ids.len() != 1 {
            return false;
        }
        let Some(sim_order) = self.matcher.get_order(&ids[0]) else {
            return false;
        };
        sim_order.order.price == price && sim_order.remaining_amount() == amount
    }

    fn has_matching_pending_limit_order(
        &self,
        side: Side,
        layer: u32,
        price: Decimal,
        amount: Decimal,
    ) -> bool {
        let mut matches = self.pending_orders.iter().filter(|(order, _)| {
            order.side == side
                && order.layer == layer
                && order.price == price
                && order.amount == amount
        });
        matches.next().is_some() && matches.next().is_none()
    }

    fn should_keep_existing_limit_order(
        &self,
        side: Side,
        layer: u32,
        price: Decimal,
        amount: Decimal,
    ) -> bool {
        self.has_matching_active_limit_order(side, layer, price, amount)
            || self.has_matching_pending_limit_order(side, layer, price, amount)
    }

    fn process_fill(&mut self, fill: &CoreFill, timestamp: f64, mid_at_fill: Decimal) {
        if let Some(ref mut cb) = self.fill_callback {
            cb(fill);
        }
        if let Some(&placement_ts) = self.order_placement_ts.get(&fill.order_id) {
            self.metrics
                .record_quote_lifetime((timestamp - placement_ts).max(0.0));
            self.order_placement_ts.remove(&fill.order_id);
        }

        let fee_bps = self.fee_model.fee_bps(fill);
        self.portfolio.apply_fill(fill, fee_bps);
        self.metrics.record_fill(fill, fee_bps);
        self.metrics.record_fill_for_markout(fill, mid_at_fill);

        let round_trips = self.round_trip_tracker.process_fill(
            fill,
            fee_bps,
            self.fee_model.maker_bps,
        );
        for rt in round_trips {
            self.metrics.record_round_trip(rt);
        }

        let mut core_port = to_core_portfolio(&self.portfolio);
        let fill_event = to_core_fill(fill);
        self.strategy.on_fill(&fill_event, &mut core_port, timestamp);
    }

    fn track_snapshot_stats(&mut self, core_snap: &OrderBookSnapshot, timestamp: f64) {
        if self.snapshot_count == 0 {
            self.first_ts = Some(timestamp);
        }
        self.last_ts = timestamp;
        self.last_mid = core_snap.mid_price().unwrap_or(self.last_mid);

        if let (Some((best_bid, _)), Some((best_ask, _))) = (core_snap.bids.first(), core_snap.asks.first()) {
            let spread = *best_ask - *best_bid;
            if spread > Decimal::ZERO && self.tick_size > Decimal::ZERO {
                let ticks_f64 = (spread / self.tick_size).to_f64().unwrap_or(0.0);
                let ticks = ticks_f64.floor().max(0.0) as u32;
                *self.spread_distribution.entry(ticks).or_insert(0) += 1;
            }
        }
    }

    fn process_snapshot_inner(&mut self, ob_snap: &ObSnapshot) -> Decimal {
        let timestamp = ob_snap.timestamp;
        let core_snap = to_core_snapshot(ob_snap);
        let mid = core_snap.mid_price().unwrap_or(ob_snap.mid_price);
        self.track_snapshot_stats(&core_snap, timestamp);

        self.apply_pending_cancels(timestamp);
        self.apply_pending_orders(timestamp, &core_snap);
        if self.metrics.markout_enabled() {
            self.metrics.process_markout_snapshot(timestamp, mid);
        }

        let fills = self.matcher.process_book_update(&core_snap);
        for fill in &fills {
            self.process_fill(fill, timestamp, mid);
        }

        let core_port = to_core_portfolio(&self.portfolio);
        let orderbook = to_core_orderbook(ob_snap);
        let intents = self.strategy.on_orderbook_update(&orderbook, &core_port, timestamp);

        let mut intents_to_execute = Vec::with_capacity(intents.len());
        for intent in intents {
            let side = match intent.side {
                CoreOrderSide::Buy => Side::Buy,
                CoreOrderSide::Sell => Side::Sell,
            };
            if intent.order_type == OrderType::Limit
                && self.should_keep_existing_limit_order(side, intent.layer, intent.price, intent.amount)
            {
                continue;
            }
            self.cancel_orders_for_layer(side, intent.layer, timestamp);
            intents_to_execute.push(intent);
        }

        let limit_intent_count = intents_to_execute
            .iter()
            .filter(|i| i.order_type == OrderType::Limit)
            .count();
        self.metrics.record_order_placed(timestamp, limit_intent_count as u64);

        for (idx, intent) in intents_to_execute.into_iter().enumerate() {
            if intent.order_type == OrderType::Cancel {
                continue;
            }

            if intent.order_type == OrderType::Market {
                let order = Order::new(
                    format!("mkt_{}_{}", timestamp, idx),
                    match intent.side {
                        CoreOrderSide::Buy => Side::Buy,
                        CoreOrderSide::Sell => Side::Sell,
                    },
                    Decimal::ZERO,
                    intent.amount,
                    CoreOrderType::Market,
                    intent.layer,
                    timestamp,
                );
                let mkt_fills = self.matcher.submit(order, &core_snap);
                for fill in &mkt_fills {
                    self.process_fill(fill, timestamp, mid);
                }
                continue;
            }

            let side = match intent.side {
                CoreOrderSide::Buy => Side::Buy,
                CoreOrderSide::Sell => Side::Sell,
            };
            let order = Order::new(
                format!("ord_{}_{}_{}", timestamp, intent.layer, idx),
                side,
                intent.price,
                intent.amount,
                CoreOrderType::Limit,
                intent.layer,
                timestamp,
            );
            let order_id = order.id.clone();
            if let Some(delay_secs) = self.order_submission_delay_secs() {
                self.schedule_pending_order(order, timestamp + delay_secs);
            } else {
                let limit_fills = self.matcher.submit(order, &core_snap);
                let limit_order_added = limit_fills.is_empty();
                for fill in &limit_fills {
                    self.process_fill(fill, timestamp, mid);
                }

                if limit_order_added {
                    self.order_placement_ts.insert(order_id.clone(), timestamp);
                    self.active_orders_by_layer
                        .entry((side, intent.layer))
                        .or_default()
                        .push(order_id);
                }
            }
        }

        self.metrics.record_inventory_snapshot(
            timestamp,
            self.portfolio.base_balance,
            self.portfolio.quote_balance,
            mid,
            self.portfolio.avg_cost,
        );
        let pv = self.portfolio.mark_to_market(mid);
        self.metrics.record_equity_sample(timestamp, pv);
        self.snapshot_count += 1;
        mid
    }

    pub fn process_snapshot(&mut self, ob_snap: ObSnapshot) -> LivePaperSummary {
        let mid = self.process_snapshot_inner(&ob_snap);
        LivePaperSummary {
            timestamp: ob_snap.timestamp,
            mid_price: mid,
            spread_bps: ob_snap.spread_bps,
            portfolio_value: self.portfolio.mark_to_market(mid),
            open_order_count: self.order_placement_ts.len(),
            dashboard: self.current_dashboard_summary(),
        }
    }

    pub fn record_live_snapshot(&mut self) {
        if self.last_ts > 0.0 {
            let pv = self.portfolio.mark_to_market(self.last_mid);
            self.metrics.record_equity_sample(self.last_ts, pv);
            self.metrics.record_snapshot(self.last_ts, pv);
        }
    }

    pub fn current_dashboard_summary(&self) -> MMDashboardSummary {
        let maker_rebate_bps = self.fee_model.maker_bps.to_f64().unwrap_or(0.0).abs();
        let churn_stats = self.matcher.queue_churn_stats();
        self.metrics.get_dashboard_summary(
            &self.portfolio,
            self.initial_capital,
            maker_rebate_bps,
            churn_stats.cancel_ahead_advance_events,
            churn_stats.cancel_ahead_advance_total,
        )
    }

    pub fn current_portfolio_value(&self) -> Option<Decimal> {
        if self.last_ts > 0.0 {
            Some(self.portfolio.mark_to_market(self.last_mid))
        } else {
            None
        }
    }

    pub fn snapshot_count(&self) -> u64 {
        self.snapshot_count
    }

    pub async fn run<D: DataLoader>(&mut self, data_loader: D) -> Result<BacktestResults> {
        let snapshots_iter = data_loader.load_snapshots()?;

        for snapshot_res in snapshots_iter {
            let ob_snap = snapshot_res?;
            let mid = self.process_snapshot_inner(&ob_snap);
            if self.snapshot_count % 500 == 0 {
                let pv = self.portfolio.mark_to_market(mid);
                self.metrics.record_snapshot(ob_snap.timestamp, pv);
            }
        }

        let final_pv = self.portfolio.mark_to_market(self.last_mid);
        self.metrics.record_snapshot(self.last_ts, final_pv);

        let stats = self.metrics.get_stats(&self.portfolio);
        let gate_diagnostics = self.strategy.gate_diagnostics();
        let dashboard = Some(self.current_dashboard_summary());
        Ok(BacktestResults {
            stats,
            first_ts: self.first_ts.unwrap_or_default(),
            last_ts: self.last_ts,
            snapshot_count: self.snapshot_count,
            spread_distribution: self.spread_distribution.clone(),
            gate_diagnostics,
            dashboard,
            portfolio_snapshots: self.metrics.portfolio_snapshots(),
            resolved_1s_markouts: self.metrics.resolved_1s_markouts(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mm_core::strategy::{OrderIntent, OrderType as StrategyOrderType};
    use orderbook::snapshot::OrderBookSnapshot as ObSnapshot;
    use rust_decimal_macros::dec;
    use std::collections::VecDeque;

    #[derive(Debug)]
    struct ScriptedStrategy {
        intents: VecDeque<Vec<OrderIntent>>,
        fills_seen: Vec<String>,
    }

    impl ScriptedStrategy {
        fn new(intents: Vec<Vec<OrderIntent>>) -> Self {
            Self {
                intents: intents.into(),
                fills_seen: Vec::new(),
            }
        }
    }

    impl Strategy for ScriptedStrategy {
        fn name(&self) -> &str {
            "scripted"
        }

        fn on_orderbook_update(
            &mut self,
            _orderbook: &CoreOrderBook,
            _portfolio: &CorePortfolio,
            _timestamp: f64,
        ) -> Vec<OrderIntent> {
            self.intents.pop_front().unwrap_or_default()
        }

        fn on_fill(
            &mut self,
            fill: &mm_core::strategy::Fill,
            _portfolio: &mut CorePortfolio,
            _timestamp: f64,
        ) {
            self.fills_seen.push(fill.order_id.clone());
        }

        fn validate_config(&self) -> std::result::Result<(), mm_core::strategy::StrategyError> {
            Ok(())
        }
    }

    fn limit_intent(side: CoreOrderSide, price: Decimal, amount: Decimal, layer: u32) -> OrderIntent {
        OrderIntent {
            side,
            price,
            amount,
            order_type: StrategyOrderType::Limit,
            layer,
        }
    }

    fn snapshot(ts: f64, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> ObSnapshot {
        let best_bid = Decimal::from_f64_retain(bids[0].0).unwrap();
        let best_ask = Decimal::from_f64_retain(asks[0].0).unwrap();
        let mid_price = (best_bid + best_ask) / dec!(2);
        ObSnapshot::from_price_levels(
            ts,
            mid_price,
            best_bid,
            best_ask,
            bids.into_iter().map(|(p, q)| vec![p, q]).collect(),
            asks.into_iter().map(|(p, q)| vec![p, q]).collect(),
        )
        .unwrap()
    }

    fn queue_config() -> QueueModelConfig {
        QueueModelConfig {
            price_improving_queue_pct: 0.0,
            touch_queue_pct: 0.0,
            queue_depletion_enabled: false,
            queue_churn_enabled: false,
            delta_trade_fraction: 1.0,
            ..QueueModelConfig::default()
        }
    }

    fn latency(order_submission_us: u64, cancel_us: u64) -> LatencyModel {
        LatencyModel {
            order_submission_us,
            fill_notification_us: 0,
            book_update_us: 0,
            cancel_us,
        }
    }

    #[test]
    fn test_order_submission_latency_delays_activation_until_effective_ts() {
        let strategy = ScriptedStrategy::new(vec![
            vec![limit_intent(CoreOrderSide::Buy, dec!(101), dec!(1), 0)],
            vec![],
            vec![],
        ]);
        let mut engine = BacktestEngine::new(
            strategy,
            dec!(1000000),
            dec!(100),
            SimpleFeeModel::new(dec!(0), dec!(0)),
            dec!(0.01),
        )
        .with_queue_config(queue_config())
        .with_latency(latency(100_000, 0));

        let snap1 = snapshot(1.0, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        let summary1 = engine.process_snapshot(snap1);
        assert_eq!(summary1.dashboard.fill_count, 0);
        assert_eq!(engine.pending_orders.len(), 1);
        assert!(engine.active_orders_by_layer.is_empty());

        let snap2 = snapshot(1.05, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        let summary2 = engine.process_snapshot(snap2);
        assert_eq!(summary2.dashboard.fill_count, 0);
        assert_eq!(engine.pending_orders.len(), 1);
        assert!(engine.active_orders_by_layer.is_empty());

        let snap3 = snapshot(1.15, vec![(100.0, 5.0)], vec![(99.0, 1.0)]);
        let summary3 = engine.process_snapshot(snap3);
        assert_eq!(summary3.dashboard.fill_count, 1);
        assert!(engine.pending_orders.is_empty());
    }

    #[test]
    fn test_cancel_latency_keeps_old_order_fillable_before_effective_ts() {
        let strategy = ScriptedStrategy::new(vec![
            vec![limit_intent(CoreOrderSide::Buy, dec!(101), dec!(1), 0)],
            vec![limit_intent(CoreOrderSide::Buy, dec!(100), dec!(1), 0)],
            vec![],
        ]);
        let mut engine = BacktestEngine::new(
            strategy,
            dec!(1000000),
            dec!(100),
            SimpleFeeModel::new(dec!(0), dec!(0)),
            dec!(0.01),
        )
        .with_queue_config(queue_config())
        .with_latency(latency(0, 100_000));

        let snap1 = snapshot(1.0, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        engine.process_snapshot(snap1);

        let snap2 = snapshot(2.0, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        engine.process_snapshot(snap2);
        assert_eq!(engine.pending_cancels.len(), 1);
        assert!(engine.order_placement_ts.contains_key("ord_1_0_0"));

        let snap3 = snapshot(2.05, vec![(100.0, 5.0)], vec![(99.0, 1.0)]);
        let summary3 = engine.process_snapshot(snap3);
        assert_eq!(summary3.dashboard.fill_count, 1);
    }

    #[test]
    fn test_cancel_latency_removes_old_order_after_effective_ts() {
        let strategy = ScriptedStrategy::new(vec![
            vec![limit_intent(CoreOrderSide::Buy, dec!(101), dec!(1), 0)],
            vec![limit_intent(CoreOrderSide::Buy, dec!(100), dec!(1), 0)],
            vec![],
        ]);
        let mut engine = BacktestEngine::new(
            strategy,
            dec!(1000000),
            dec!(100),
            SimpleFeeModel::new(dec!(0), dec!(0)),
            dec!(0.01),
        )
        .with_queue_config(queue_config())
        .with_latency(latency(0, 100_000));

        let snap1 = snapshot(1.0, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        engine.process_snapshot(snap1);

        let snap2 = snapshot(2.0, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        engine.process_snapshot(snap2);

        let snap3 = snapshot(2.2, vec![(100.0, 5.0)], vec![(99.0, 1.0)]);
        let summary3 = engine.process_snapshot(snap3);
        assert_eq!(summary3.dashboard.fill_count, 0);
        assert!(!engine.order_placement_ts.contains_key("ord_1_0_0"));
    }

    #[test]
    fn test_zero_latency_keeps_immediate_limit_activation() {
        let strategy = ScriptedStrategy::new(vec![vec![limit_intent(
            CoreOrderSide::Buy,
            dec!(101),
            dec!(1),
            0,
        )]]);
        let mut engine = BacktestEngine::new(
            strategy,
            dec!(1000000),
            dec!(100),
            SimpleFeeModel::new(dec!(0), dec!(0)),
            dec!(0.01),
        )
        .with_queue_config(queue_config())
        .with_latency(LatencyModel::disabled());

        let snap1 = snapshot(1.0, vec![(100.0, 5.0)], vec![(101.0, 2.0)]);
        let summary1 = engine.process_snapshot(snap1);
        assert_eq!(summary1.dashboard.fill_count, 0);
        assert!(engine.pending_orders.is_empty());
        assert_eq!(
            engine.active_orders_by_layer.get(&(Side::Buy, 0)).map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn test_same_limit_intent_keeps_existing_active_order() {
        let strategy = ScriptedStrategy::new(vec![
            vec![limit_intent(CoreOrderSide::Buy, dec!(100), dec!(1), 0)],
            vec![limit_intent(CoreOrderSide::Buy, dec!(100), dec!(1), 0)],
        ]);
        let mut engine = BacktestEngine::new(
            strategy,
            dec!(1000000),
            dec!(100),
            SimpleFeeModel::new(dec!(0), dec!(0)),
            dec!(0.01),
        )
        .with_queue_config(queue_config())
        .with_latency(LatencyModel::disabled());

        let snap1 = snapshot(1.0, vec![(100.0, 5.0)], vec![(101.0, 5.0)]);
        engine.process_snapshot(snap1);
        let first_id = engine
            .active_orders_by_layer
            .get(&(Side::Buy, 0))
            .and_then(|ids| ids.first())
            .cloned()
            .unwrap();
        let first_ts = *engine.order_placement_ts.get(&first_id).unwrap();

        let snap2 = snapshot(2.0, vec![(100.0, 5.0)], vec![(101.0, 5.0)]);
        engine.process_snapshot(snap2);
        let ids = engine.active_orders_by_layer.get(&(Side::Buy, 0)).unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0], first_id);
        assert_eq!(engine.pending_cancels.len(), 0);
        assert_eq!(engine.pending_orders.len(), 0);
        assert_eq!(engine.order_placement_ts.get(&first_id), Some(&first_ts));
    }

    #[test]
    fn test_same_limit_intent_keeps_pending_order_without_rescheduling() {
        let strategy = ScriptedStrategy::new(vec![
            vec![limit_intent(CoreOrderSide::Buy, dec!(100), dec!(1), 0)],
            vec![limit_intent(CoreOrderSide::Buy, dec!(100), dec!(1), 0)],
        ]);
        let mut engine = BacktestEngine::new(
            strategy,
            dec!(1000000),
            dec!(100),
            SimpleFeeModel::new(dec!(0), dec!(0)),
            dec!(0.01),
        )
        .with_queue_config(queue_config())
        .with_latency(latency(200_000, 0));

        let snap1 = snapshot(1.0, vec![(100.0, 5.0)], vec![(101.0, 5.0)]);
        engine.process_snapshot(snap1);
        assert_eq!(engine.pending_orders.len(), 1);
        let first_pending_id = engine.pending_orders[0].0.id.clone();
        let first_effective_ts = engine.pending_orders[0].1;

        let snap2 = snapshot(1.1, vec![(100.0, 5.0)], vec![(101.0, 5.0)]);
        engine.process_snapshot(snap2);
        assert_eq!(engine.pending_orders.len(), 1);
        assert_eq!(engine.pending_orders[0].0.id, first_pending_id);
        assert_eq!(engine.pending_orders[0].1, first_effective_ts);
    }
}
