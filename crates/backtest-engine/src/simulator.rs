use orderbook::order::{Order, OrderSide, OrderStatus};
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{BTreeMap, HashMap};
use tracing::warn;

/// Order Book Simulator for realistic backtesting
///
/// This simulator maintains a virtual order book and tracks queue positions
/// for accurate fill simulation based on L2 order book data.
///
/// Key features:
/// - Maintains virtual order book with all orders
/// - Tracks queue position for each order
/// - Simulates order consumption chronologically
/// - Handles partial fills based on depth
pub struct OrderBookSimulator {
    /// Track all orders by ID
    orders: HashMap<String, Order>,

    /// Track orders by price level (for queue position calculation)
    /// bids: price -> list of order IDs (sorted by timestamp)
    /// asks: price -> list of order IDs (sorted by timestamp)
    bids_by_price: BTreeMap<Decimal, Vec<String>>,
    asks_by_price: BTreeMap<Decimal, Vec<String>>,

    /// Track order IDs by price (for fast lookup)
    order_prices: HashMap<String, Decimal>,

    /// Statistics
    total_fills: u64,
    total_partial_fills: u64,
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: String,
    pub filled_amount: Decimal,
    pub fill_price: Decimal,
    pub timestamp: f64,
}

impl OrderBookSimulator {
    pub fn new() -> Self {
        Self {
            orders: HashMap::new(),
            bids_by_price: BTreeMap::new(),
            asks_by_price: BTreeMap::new(),
            order_prices: HashMap::new(),
            total_fills: 0,
            total_partial_fills: 0,
        }
    }

    /// Add an order to the book with optional market queue (size ahead at same price from exchange book)
    pub fn add_order_with_market_queue(
        &mut self,
        mut order: Order,
        market_queue_ahead: Decimal,
    ) -> bool {
        order.queue_position = market_queue_ahead;
        self.add_order_inner(order)
    }

    /// Add an order to the book (queue_position left as-is, typically 0)
    pub fn add_order(&mut self, order: Order) -> bool {
        self.add_order_inner(order)
    }

    fn add_order_inner(&mut self, mut order: Order) -> bool {
        if self.orders.contains_key(&order.order_id) {
            warn!("Order {} already exists", order.order_id);
            return false;
        }

        if order.amount <= Decimal::ZERO {
            warn!("Invalid order amount: {}", order.amount);
            return false;
        }

        if order.price <= Decimal::ZERO {
            warn!("Invalid order price: {}", order.price);
            return false;
        }

        // Add order
        order.status = OrderStatus::Active;
        let order_id = order.order_id.clone();
        let price = order.price;
        let side = order.side;
        let timestamp = order.timestamp;

        self.orders.insert(order_id.clone(), order);
        self.order_prices.insert(order_id.clone(), price);

        // Add to price level tracking
        if side == OrderSide::Buy {
            self.insert_order_at_price_buy(price, order_id.clone(), timestamp);
        } else {
            self.insert_order_at_price_sell(price, order_id.clone(), timestamp);
        }

        // Update queue positions for all orders at this price level
        self.update_queue_positions_at_price(price, side);

        true
    }

    fn insert_order_at_price_buy(&mut self, price: Decimal, order_id: String, timestamp: f64) {
        let order_list = self.bids_by_price.entry(price).or_insert_with(Vec::new);

        // Insert in chronological order (FIFO at same price)
        let mut inserted = false;
        for (i, existing_id) in order_list.iter().enumerate() {
            if let Some(existing_order) = self.orders.get(existing_id) {
                if existing_order.timestamp > timestamp {
                    order_list.insert(i, order_id.clone());
                    inserted = true;
                    break;
                }
            }
        }

        if !inserted {
            order_list.push(order_id);
        }
    }

    fn insert_order_at_price_sell(&mut self, price: Decimal, order_id: String, timestamp: f64) {
        let order_list = self.asks_by_price.entry(price).or_insert_with(Vec::new);

        // Insert in chronological order (FIFO at same price)
        let mut inserted = false;
        for (i, existing_id) in order_list.iter().enumerate() {
            if let Some(existing_order) = self.orders.get(existing_id) {
                if existing_order.timestamp > timestamp {
                    order_list.insert(i, order_id.clone());
                    inserted = true;
                    break;
                }
            }
        }

        if !inserted {
            order_list.push(order_id);
        }
    }

    /// Cancel an order
    pub fn cancel_order(&mut self, order_id: &str, cancel_timestamp: f64) -> bool {
        let order = match self.orders.get_mut(order_id) {
            Some(o) => o,
            None => return false,
        };

        if order.is_filled() {
            return false; // Can't cancel filled order
        }

        order.cancel(cancel_timestamp);
        let price = self.order_prices.get(order_id).copied();
        let side = order.side;

        // Remove from price level tracking
        if let Some(price) = price {
            if side == OrderSide::Buy {
                if let Some(order_list) = self.bids_by_price.get_mut(&price) {
                    order_list.retain(|id| id != order_id);
                    if order_list.is_empty() {
                        self.bids_by_price.remove(&price);
                    }
                }
            } else {
                if let Some(order_list) = self.asks_by_price.get_mut(&price) {
                    order_list.retain(|id| id != order_id);
                    if order_list.is_empty() {
                        self.asks_by_price.remove(&price);
                    }
                }
            }

            // Update queue positions
            self.update_queue_positions_at_price(price, side);
        }

        true
    }

    /// Process a market snapshot and check for fills
    ///
    /// Returns list of fills that occurred
    pub fn process_market_snapshot(&mut self, snapshot: &OrderBookSnapshot) -> Vec<Fill> {
        let mut fills = Vec::new();

        // Process fills for buy orders (our bids)
        // Our bid fills when market asks cross down to our price
        fills.extend(self.process_buy_order_fills(&snapshot.asks, snapshot.timestamp));

        // Process fills for sell orders (our asks)
        // Our ask fills when market bids cross up to our price
        fills.extend(self.process_sell_order_fills(&snapshot.bids, snapshot.timestamp));

        fills
    }

    fn process_buy_order_fills(
        &mut self,
        market_asks: &[(Decimal, Decimal)],
        timestamp: f64,
    ) -> Vec<Fill> {
        let mut fills = Vec::new();
        let mut filled_or_cancelled_orders = Vec::new();

        let bid_levels: Vec<(Decimal, Vec<String>)> = self
            .bids_by_price
            .iter()
            .rev()
            .map(|(p, ids)| (*p, ids.clone()))
            .collect();

        if bid_levels.is_empty() {
            return fills;
        }

        // Best ask is first (market_asks sorted ascending)
        let best_ask = match market_asks.first() {
            Some((p, _)) => *p,
            None => return fills,
        };

        // Cumulative market liquidity: (price, running_sum) for asks from best upward
        let mut cumulative_ask_liquidity: Vec<(Decimal, Decimal)> = Vec::new();
        let mut running = Decimal::ZERO;
        for (price, qty) in market_asks {
            running += qty;
            cumulative_ask_liquidity.push((*price, running));
        }

        let mut consumed_liquidity = Decimal::ZERO;

        for (bid_price, order_ids) in &bid_levels {
            if *bid_price < best_ask {
                break;
            }

            let available_total: Decimal = cumulative_ask_liquidity
                .iter()
                .filter(|(ask_price, _)| *ask_price <= *bid_price)
                .map(|(_, cum)| *cum)
                .last()
                .unwrap_or(Decimal::ZERO);

            let mut available_at_our_price = if available_total > consumed_liquidity {
                available_total - consumed_liquidity
            } else {
                Decimal::ZERO
            };

            if available_at_our_price <= Decimal::ZERO {
                continue;
            }

            for order_id in order_ids {
                let order = match self.orders.get_mut(order_id) {
                    Some(o) => o,
                    None => continue,
                };

                if order.is_filled() || order.is_cancelled() {
                    filled_or_cancelled_orders.push((order_id.clone(), OrderSide::Buy, *bid_price));
                    continue;
                }

                let size_ahead = order.queue_position;
                if size_ahead >= available_at_our_price {
                    break;
                }

                let liquidity_for_us = available_at_our_price - size_ahead;
                let fill_amount = liquidity_for_us.min(order.remaining_amount);

                if fill_amount > dec!(0.0000000001) {
                    // Maker gets their limit price (bid_price), not best_ask
                    let fill_price = *bid_price;
                    let actual_fill = order.fill(fill_amount, fill_price, timestamp);
                    if actual_fill > Decimal::ZERO {
                        fills.push(Fill {
                            order_id: order.order_id.clone(),
                            filled_amount: actual_fill,
                            fill_price,
                            timestamp,
                        });
                        consumed_liquidity += actual_fill;
                        available_at_our_price -= actual_fill;
                        self.total_fills += 1;
                        if order.status == OrderStatus::PartiallyFilled {
                            self.total_partial_fills += 1;
                        } else if order.status == OrderStatus::Filled {
                            filled_or_cancelled_orders.push((
                                order_id.to_string(),
                                OrderSide::Buy,
                                *bid_price,
                            ));
                        }
                        self.update_queue_positions_at_price(*bid_price, OrderSide::Buy);
                    }
                }
            }
        }

        for (order_id, _side, price) in filled_or_cancelled_orders {
            if let Some(list) = self.bids_by_price.get_mut(&price) {
                list.retain(|id| id != &order_id);
                if list.is_empty() {
                    self.bids_by_price.remove(&price);
                }
            }
        }

        fills
    }

    fn process_sell_order_fills(
        &mut self,
        market_bids: &[(Decimal, Decimal)],
        timestamp: f64,
    ) -> Vec<Fill> {
        let mut fills = Vec::new();
        let mut filled_or_cancelled_orders = Vec::new();

        let ask_levels: Vec<(Decimal, Vec<String>)> = self
            .asks_by_price
            .iter()
            .map(|(p, ids)| (*p, ids.clone()))
            .collect();

        if ask_levels.is_empty() {
            return fills;
        }

        // Best bid is first (market_bids sorted descending, best bid first)
        let best_bid = match market_bids.first() {
            Some((p, _)) => *p,
            None => return fills,
        };

        // Cumulative market bid liquidity: (price, running_sum) from best bid downward
        let mut cumulative_bid_liquidity: Vec<(Decimal, Decimal)> = Vec::new();
        let mut running = Decimal::ZERO;
        for (price, qty) in market_bids {
            running += qty;
            cumulative_bid_liquidity.push((*price, running));
        }

        let mut consumed_liquidity = Decimal::ZERO;

        for (ask_price, order_ids) in &ask_levels {
            if *ask_price > best_bid {
                break;
            }

            let available_total: Decimal = cumulative_bid_liquidity
                .iter()
                .filter(|(bid_price, _)| *bid_price >= *ask_price)
                .map(|(_, cum)| *cum)
                .last()
                .unwrap_or(Decimal::ZERO);

            let mut available_at_our_price = if available_total > consumed_liquidity {
                available_total - consumed_liquidity
            } else {
                Decimal::ZERO
            };

            if available_at_our_price <= Decimal::ZERO {
                continue;
            }

            for order_id in order_ids {
                let order = match self.orders.get_mut(order_id) {
                    Some(o) => o,
                    None => continue,
                };

                if order.is_filled() || order.is_cancelled() {
                    filled_or_cancelled_orders.push((
                        order_id.clone(),
                        OrderSide::Sell,
                        *ask_price,
                    ));
                    continue;
                }

                let size_ahead = order.queue_position;
                if size_ahead >= available_at_our_price {
                    break;
                }

                let liquidity_for_us = available_at_our_price - size_ahead;
                let fill_amount = liquidity_for_us.min(order.remaining_amount);

                if fill_amount > dec!(0.0000000001) {
                    // Maker gets their limit price (ask_price), not best_bid
                    let fill_price = *ask_price;
                    let actual_fill = order.fill(fill_amount, fill_price, timestamp);
                    if actual_fill > Decimal::ZERO {
                        fills.push(Fill {
                            order_id: order.order_id.clone(),
                            filled_amount: actual_fill,
                            fill_price,
                            timestamp,
                        });
                        consumed_liquidity += actual_fill;
                        available_at_our_price -= actual_fill;
                        self.total_fills += 1;
                        if order.status == OrderStatus::PartiallyFilled {
                            self.total_partial_fills += 1;
                        } else if order.status == OrderStatus::Filled {
                            filled_or_cancelled_orders.push((
                                order_id.to_string(),
                                OrderSide::Sell,
                                *ask_price,
                            ));
                        }
                        self.update_queue_positions_at_price(*ask_price, OrderSide::Sell);
                    }
                }
            }
        }

        for (order_id, _side, price) in filled_or_cancelled_orders {
            if let Some(list) = self.asks_by_price.get_mut(&price) {
                list.retain(|id| id != &order_id);
                if list.is_empty() {
                    self.asks_by_price.remove(&price);
                }
            }
        }

        fills
    }

    /// Update queue positions for all orders at a specific price level
    ///
    /// Queue position = sum of sizes of orders ahead of us at the same price
    /// This matches Hummingbot's _update_queue_positions_at_price() logic exactly
    fn update_queue_positions_at_price(&mut self, price: Decimal, side: OrderSide) {
        let order_list = match side {
            OrderSide::Buy => self.bids_by_price.get(&price),
            OrderSide::Sell => self.asks_by_price.get(&price),
        };

        let order_list = match order_list {
            Some(list) => list,
            None => return,
        };

        // Calculate cumulative size ahead of each order
        let mut cumulative_size_ahead = Decimal::ZERO;
        let total_size_at_price: Decimal = order_list
            .iter()
            .filter_map(|oid| self.orders.get(oid))
            .filter(|o| !o.is_filled() && !o.is_cancelled())
            .map(|o| o.remaining_amount)
            .sum();

        for order_id in order_list {
            let order = match self.orders.get_mut(order_id) {
                Some(o) => o,
                None => continue,
            };

            if order.is_filled() || order.is_cancelled() {
                order.update_queue_position(Decimal::ZERO, total_size_at_price);
                continue;
            }

            // Set queue position (size ahead of this order)
            order.update_queue_position(cumulative_size_ahead, total_size_at_price);

            // Add this order's size to cumulative for next orders
            cumulative_size_ahead += order.remaining_amount;
        }
    }

    pub fn get_active_orders(&self) -> Vec<&Order> {
        self.orders.values().filter(|o| o.is_active()).collect()
    }

    pub fn get_order(&self, order_id: &str) -> Option<&Order> {
        self.orders.get(order_id)
    }

    pub fn get_order_mut(&mut self, order_id: &str) -> Option<&mut Order> {
        self.orders.get_mut(order_id)
    }

    pub fn get_statistics(&self) -> SimulatorStats {
        let active_orders = self.get_active_orders().len();
        let filled_orders = self.orders.values().filter(|o| o.is_filled()).count();
        let partially_filled = self
            .orders
            .values()
            .filter(|o| matches!(o.status, OrderStatus::PartiallyFilled))
            .count();

        SimulatorStats {
            total_orders: self.orders.len(),
            active_orders,
            filled_orders,
            partially_filled_orders: partially_filled,
            total_fills: self.total_fills,
            total_partial_fills: self.total_partial_fills,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SimulatorStats {
    pub total_orders: usize,
    pub active_orders: usize,
    pub filled_orders: usize,
    pub partially_filled_orders: usize,
    pub total_fills: u64,
    pub total_partial_fills: u64,
}
