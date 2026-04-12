use crate::simulator::OrderBookSimulator;
use crate::portfolio::BacktestPortfolio;
use crate::latency::{LatencySimulator, DEFAULT_LATENCY_SEED};
use crate::metrics::MetricsCollector;
use data_loader::DataLoader;
use mm_core::strategy::Strategy;
use mm_core::strategy::OrderType;
use orderbook::order::{Order, OrderSide, OrderStatus};
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use anyhow::Result;
use std::collections::HashMap;

/// Backtest Runner - processes CSV snapshots chronologically
pub struct BacktestRunner<S: Strategy> {
    strategy: S,
    simulator: OrderBookSimulator,
    portfolio: BacktestPortfolio,
    latency: Option<LatencySimulator>,
    metrics: MetricsCollector,
    maker_fee_bps: Decimal,
    tick_size: Decimal,

    // Track active orders by (side, layer) - cancel only the layer we replace
    active_orders_by_layer: HashMap<(OrderSide, u32), Vec<String>>,

    // Pending cancels: (order_id, effective_timestamp when cancel takes effect)
    pending_cancels: Vec<(String, f64)>,

    // Accumulate trade PnL by order_id; flush as one round-trip when order fully filled
    trade_pnl_by_order: HashMap<String, Decimal>,
}

impl<S: Strategy> BacktestRunner<S> {
    pub fn new(
        strategy: S,
        initial_capital: Decimal,
        initial_price: Decimal,
        maker_fee_bps: Decimal,
        use_latency: bool,
        tick_size: Decimal,
    ) -> Self {
        let portfolio = BacktestPortfolio::new(
            Decimal::ZERO,
            initial_capital,
            initial_price,
        );

        let latency = if use_latency {
            Some(LatencySimulator::new_with_seed(
                Default::default(),
                DEFAULT_LATENCY_SEED,
            ))
        } else {
            None
        };

        Self {
            strategy,
            simulator: OrderBookSimulator::new(),
            portfolio,
            latency,
            metrics: MetricsCollector::new(),
            maker_fee_bps,
            tick_size,
            active_orders_by_layer: HashMap::new(),
            pending_cancels: Vec::new(),
            trade_pnl_by_order: HashMap::new(),
        }
    }

    fn apply_pending_cancels(&mut self, timestamp: f64) {
        self.pending_cancels.retain(|(order_id, effective_ts)| {
            if *effective_ts <= timestamp {
                self.simulator.cancel_order(order_id, timestamp);
                false // Remove from pending
            } else {
                true // Keep in pending
            }
        });
    }

    fn cancel_orders_for_layer(&mut self, side: OrderSide, layer: u32, timestamp: f64) {
        let key = (side, layer);
        if let Some(order_ids) = self.active_orders_by_layer.get(&key) {
            let order_ids_clone = order_ids.clone();
            for order_id in order_ids_clone {
                if let Some(ref mut latency) = self.latency {
                    let effective_ts = latency.apply_cancel_latency(timestamp);
                    self.pending_cancels.push((order_id, effective_ts));
                } else {
                    self.simulator.cancel_order(&order_id, timestamp);
                }
            }
            self.active_orders_by_layer.remove(&key);
        }
    }
    
    fn track_order(&mut self, order_id: String, side: OrderSide, layer: u32) {
        self.active_orders_by_layer
            .entry((side, layer))
            .or_insert_with(Vec::new)
            .push(order_id);
    }

    /// Walk the book for market orders; returns (price, qty) partial fills with slippage.
    fn fill_market_with_slippage(
        snapshot: &OrderBookSnapshot,
        side: OrderSide,
        amount: Decimal,
    ) -> Vec<(Decimal, Decimal)> {
        let mut fills = Vec::new();
        let mut remaining = amount;

        match side {
            OrderSide::Buy => {
                for (price, level_qty) in &snapshot.asks {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    let fill_qty = remaining.min(*level_qty);
                    if fill_qty > Decimal::ZERO {
                        fills.push((*price, fill_qty));
                        remaining -= fill_qty;
                    }
                }
            }
            OrderSide::Sell => {
                for (price, level_qty) in &snapshot.bids {
                    if remaining <= Decimal::ZERO {
                        break;
                    }
                    let fill_qty = remaining.min(*level_qty);
                    if fill_qty > Decimal::ZERO {
                        fills.push((*price, fill_qty));
                        remaining -= fill_qty;
                    }
                }
            }
        }

        fills
    }

    /// Market depth at given price on same side (exchange book). Our order joins the queue behind this.
    /// Matches within tick_size (exact equality fails due to computed prices vs tick-quantized L2).
    fn market_queue_at_price(
        snapshot: &OrderBookSnapshot,
        price: Decimal,
        side: OrderSide,
        tick_size: Decimal,
    ) -> Decimal {
        let levels: &[(Decimal, Decimal)] = match side {
            OrderSide::Buy => &snapshot.bids,
            OrderSide::Sell => &snapshot.asks,
        };
        // Find nearest level within one tick
        let matching = levels
            .iter()
            .filter(|(p, _)| (*p - price).abs() < tick_size)
            .min_by(|a, b| {
                let da = (a.0 - price).abs();
                let db = (b.0 - price).abs();
                da.cmp(&db)
            });
        match matching {
            Some((_, qty)) => *qty,
            None => Decimal::ZERO, // No level within tick — we're price-improving, first in queue
        }
    }

    pub async fn run<D: DataLoader>(&mut self, data_loader: D) -> Result<BacktestResults> {
        let snapshots_iter = data_loader.load_snapshots()?;
        
        let mut pending_orders: Vec<(Order, f64)> = Vec::new(); // (order, effective_timestamp)
        let mut last_mid_price = Decimal::ZERO;
        let mut last_timestamp = 0.0;
        let mut snapshot_count: u64 = 0;

        for snapshot_res in snapshots_iter {
            let snapshot = snapshot_res?;
            snapshot_count += 1;
            last_mid_price = snapshot.mid_price;
            last_timestamp = snapshot.timestamp;
            let timestamp = snapshot.timestamp;

            // Apply latency to market data if enabled
            let effective_timestamp = if let Some(ref mut latency) = self.latency {
                latency.apply_market_update_latency(timestamp)
            } else {
                timestamp
            };

            // Apply pending cancels whose latency has elapsed
            self.apply_pending_cancels(timestamp);

            // Process pending orders that should now be active
            pending_orders.retain(|(order, effective_ts)| {
                if *effective_ts <= timestamp {
                    let active_order = order.clone();
                    let order_id = active_order.order_id.clone();
                    let side = active_order.side;
                    let layer = active_order.layer;
                    let market_queue = Self::market_queue_at_price(&snapshot, active_order.price, side, self.tick_size);
                    self.simulator.add_order_with_market_queue(active_order, market_queue);
                    self.track_order(order_id, side, layer);
                    false // Remove from pending
                } else {
                    true // Keep in pending
                }
            });

            // Process fills BEFORE cancel/replace: this snapshot shows market state AFTER time passed.
            // Our existing orders (from previous snapshots) may have been hit - check against NEW state.
            let fills = self.simulator.process_market_snapshot(&snapshot);

            // Process fills
            let mut portfolio_for_strategy = mm_core::Portfolio::new(
                self.portfolio.get_base_balance(),
                self.portfolio.get_quote_balance(),
            );
            portfolio_for_strategy.realized_pnl = self.portfolio.get_realized_pnl();

            for fill in fills {
                let order = self.simulator.get_order(&fill.order_id).cloned();
                if let Some(order) = order {
                    let fees = fill.fill_price * fill.filled_amount * self.maker_fee_bps / dec!(10000);
                    let prev_realized = self.portfolio.get_realized_pnl();

                    self.portfolio.add_trade(
                        fill.timestamp,
                        order.side,
                        fill.fill_price,
                        fill.filled_amount,
                        fees,
                    );

                    let layer = order.layer;
                    // Don't remove from active_orders on partial fill — we need to cancel partially
                    // filled orders. When fully filled, remove to avoid stale refs.

                    let fill_event = mm_core::strategy::Fill {
                        order_id: fill.order_id.clone(),
                        side: match order.side {
                            OrderSide::Buy => mm_core::market_data::OrderSide::Buy,
                            OrderSide::Sell => mm_core::market_data::OrderSide::Sell,
                        },
                        price: fill.fill_price,
                        amount: fill.filled_amount,
                        timestamp: fill.timestamp,
                        layer: order.layer,
                    };

                    self.strategy.on_fill(
                        &fill_event,
                        &mut portfolio_for_strategy,
                        fill.timestamp,
                    );

                    self.metrics.record_fill_with_ts(order.side, order.layer, fill.filled_amount, fill.fill_price, fees, fill.timestamp);
                    self.metrics.record_fill_forensic(fill.fill_price, snapshot.mid_price, order.side, fill.timestamp);

                    let new_realized = self.portfolio.get_realized_pnl();
                    let trade_pnl = new_realized - prev_realized;

                    // Accumulate PnL by order; only count win/loss when order fully filled (round-trip)
                    *self.trade_pnl_by_order
                        .entry(fill.order_id.clone())
                        .or_insert(Decimal::ZERO) += trade_pnl;
                    self.metrics.record_trade_pnl_delta(trade_pnl, order.layer, fill.timestamp);

                    if order.status == OrderStatus::Filled {
                        let round_trip_pnl = self.trade_pnl_by_order
                            .remove(&fill.order_id)
                            .unwrap_or(Decimal::ZERO);
                        self.metrics.record_round_trip_pnl(round_trip_pnl, fill.timestamp);
                        // Remove filled order from tracking so we don't try to cancel it later
                        if let Some(active_orders) = self.active_orders_by_layer.get_mut(&(order.side, layer)) {
                            active_orders.retain(|id: &String| id != &fill.order_id);
                        }
                    }
                }
            }

            let orderbook = mm_core::market_data::OrderBook {
                bids: snapshot.bids.clone(),
                asks: snapshot.asks.clone(),
                timestamp: effective_timestamp,
            };

            portfolio_for_strategy.base_balance = self.portfolio.get_base_balance();
            portfolio_for_strategy.quote_balance = self.portfolio.get_quote_balance();
            portfolio_for_strategy.realized_pnl = self.portfolio.get_realized_pnl();

            let order_intents = self.strategy.on_orderbook_update(
                &orderbook,
                &portfolio_for_strategy,
                effective_timestamp,
            );

            // Cancel by (side, layer) - only cancel layers we're replacing
            for intent in &order_intents {
                let side = match intent.side {
                    mm_core::market_data::OrderSide::Buy => OrderSide::Buy,
                    mm_core::market_data::OrderSide::Sell => OrderSide::Sell,
                };
                self.cancel_orders_for_layer(side, intent.layer, timestamp);
            }

            // Taker fee for Market orders (1.5 bps)
            const TAKER_FEE_BPS: Decimal = dec!(1.5);

            // Process intents: place Limit, execute Market immediately, skip Cancel
            for (intent_idx, intent) in order_intents.into_iter().enumerate() {
                if intent.order_type == OrderType::Cancel {
                    continue; // Already cancelled above
                }

                // Market order: walk the book with slippage (taker)
                if intent.order_type == OrderType::Market {
                    let order_side = match intent.side {
                        mm_core::market_data::OrderSide::Buy => OrderSide::Buy,
                        mm_core::market_data::OrderSide::Sell => OrderSide::Sell,
                    };
                    let partial_fills = Self::fill_market_with_slippage(
                        &snapshot,
                        order_side,
                        intent.amount,
                    );

                    let mut total_fees = Decimal::ZERO;
                    let prev_realized = self.portfolio.get_realized_pnl();

                    for (fill_price, fill_amount) in partial_fills {
                        if fill_amount <= Decimal::ZERO {
                            continue;
                        }
                        let fees = fill_price * fill_amount * TAKER_FEE_BPS / dec!(10000);
                        total_fees += fees;

                        self.portfolio.add_trade(
                            timestamp,
                            order_side,
                            fill_price,
                            fill_amount,
                            fees,
                        );

                        let fill_event = mm_core::strategy::Fill {
                            order_id: format!("market_{}_{}", timestamp, intent.layer),
                            side: intent.side,
                            price: fill_price,
                            amount: fill_amount,
                            timestamp,
                            layer: intent.layer,
                        };

                        let mut portfolio_for_strategy = mm_core::Portfolio::new(
                            self.portfolio.get_base_balance(),
                            self.portfolio.get_quote_balance(),
                        );
                        portfolio_for_strategy.realized_pnl = self.portfolio.get_realized_pnl();
                        self.strategy.on_fill(&fill_event, &mut portfolio_for_strategy, timestamp);

                        self.metrics.record_fill_with_ts(
                            order_side,
                            intent.layer,
                            fill_amount,
                            fill_price,
                            fees,
                            timestamp,
                        );
                    }

                    let trade_pnl = self.portfolio.get_realized_pnl() - prev_realized;
                    self.metrics.record_trade_pnl_by_day(trade_pnl, intent.layer, timestamp);
                    continue;
                }

                let order_side = match intent.side {
                    mm_core::market_data::OrderSide::Buy => OrderSide::Buy,
                    mm_core::market_data::OrderSide::Sell => OrderSide::Sell,
                };
                let order = Order::new(
                    format!("order_{}_{}_{}", timestamp, intent.layer, intent_idx),
                    order_side,
                    intent.price,
                    intent.amount,
                    effective_timestamp,
                    intent.layer,
                );

                let order_id = order.order_id.clone();
                let side = order.side;
                let layer = order.layer;

                // Apply placement latency
                let placement_timestamp = if let Some(ref mut latency) = self.latency {
                    latency.apply_placement_latency(effective_timestamp)
                } else {
                    effective_timestamp
                };

                if placement_timestamp <= timestamp {
                    let market_queue = Self::market_queue_at_price(&snapshot, order.price, order.side, self.tick_size);
                    self.simulator.add_order_with_market_queue(order, market_queue);
                    self.track_order(order_id, side, layer);
                } else {
                    pending_orders.push((order, placement_timestamp));
                }
            }

            // Record portfolio snapshot periodically (every 5000 snapshots to reduce memory and improve performance)
            if snapshot_count % 5000 == 0 {
                let portfolio_snapshot = self.portfolio.mark_to_market(timestamp, snapshot.mid_price);
                self.metrics.record_snapshot(portfolio_snapshot);
            }
        }

        // Record final snapshot
        if snapshot_count > 0 {
            let portfolio_snapshot = self.portfolio.mark_to_market(last_timestamp, last_mid_price);
            self.metrics.record_snapshot(portfolio_snapshot);
        }


        let stats = self.metrics.get_final_stats(&self.portfolio);
        let simulator_stats = self.simulator.get_statistics();

        Ok(BacktestResults {
            stats,
            simulator_stats,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BacktestResults {
    pub stats: crate::metrics::BacktestStats,
    pub simulator_stats: crate::simulator::SimulatorStats,
}
