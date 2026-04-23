use mm_core::market_data::{OrderBook, OrderSide};
use mm_core::strategy::{Fill, OrderIntent, Strategy, StrategyError};
use mm_core::Portfolio;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::collections::VecDeque;

/// Multi-layer market making strategy with inventory control
/// Places 3 layers of orders at 1, 2, and 3 bps from mid-price
/// Automatically rebalances inventory to prevent accumulation
/// Supports dynamic spreads that widen during high volatility
pub struct BalancedMMStrategy {
    order_amount: Decimal,

    // Multi-layer configuration (base spreads)
    base_layer_spreads_bps: Vec<Decimal>, // [1, 2, 3] bps for layers 1, 2, 3

    // Order lifecycle management per layer
    active_bid_orders: HashMap<u32, Option<String>>,
    active_ask_orders: HashMap<u32, Option<String>>,
    last_bid_fill_times: HashMap<u32, Option<f64>>,
    last_ask_fill_times: HashMap<u32, Option<f64>>,

    last_refresh_time: f64,
    order_refresh_time: f64,
    filled_order_delay: f64,

    // Inventory control
    target_inventory_pct: f64,  // Target inventory (50% = balanced)
    soft_limit_pct: f64,        // Soft limit (60% = start adjusting)
    hard_limit_pct: f64,        // Hard limit (80% = stop one side)
    max_skew_bps: f64,          // Maximum skew in basis points (e.g., 50.0)
    inventory_sensitivity: f64, // How aggressively to skew (skew = deviation * sensitivity)

    // Volatility-aware dynamic spreads
    price_history: VecDeque<(f64, f64)>, // (timestamp, mid_price)
    volatility_multiplier: f64, // Current spread multiplier (1.0 = base, 2.0 = double spreads)
    vol_lookback_secs: f64,     // How far back to look for volatility (e.g., 300 = 5 min)
    vol_threshold_bps: f64,     // Baseline volatility threshold (e.g., 2.0 = 2 bps)
    max_spread_multiplier: f64, // Max multiplier in high vol (e.g., 4.0 = 4x spreads)
    volatility_enabled: bool,   // Enable/disable dynamic spreads

    // 1. Asymmetric spread stretching - widen spread on risky side
    spread_stretch_enabled: bool,
    spread_stretch_multiplier: f64, // e.g. 5.0 = 5x spread on side we don't want fills

    // 2. Dynamic order sizing - lean into reducing side, sip on risky side
    dynamic_sizing_enabled: bool,
    risky_side_size_pct: f64, // e.g. 0.25 = 25% on risky side near hard limit
    reducing_side_size_pct: f64, // e.g. 1.50 = 150% on reducing side near hard limit

    // 3. Order book shadowing - place one tick in front of walls
    shadowing_enabled: bool,
    wall_threshold_mult: f64, // Wall = level with size > average * this (e.g. 5.0)
    shadow_tick_bps: f64,     // Minimum tick in bps to place in front (e.g. 0.5)

    // 4. Ping-pong scalping - immediate scratch order after fill
    pingpong_enabled: bool,
    pingpong_spread_bps: f64, // Scratch order spread (e.g. 0.5 = mid + 0.5 bps for ask)
    maker_fee_bps: f64,       // For scratch pricing (e.g. 0.25; negative = rebate)
    pending_scratches: VecDeque<(OrderSide, Decimal, Decimal, f64)>, // Queue multiple scratches
    micro_price_enabled: bool, // Use imbalance-weighted micro-price instead of simple mid
}

impl BalancedMMStrategy {
    pub fn new(_bid_spread_bps: f64, _ask_spread_bps: f64, order_amount: Decimal) -> Self {
        Self::new_with_config(
            order_amount,
            10.0, // refresh_time
            3.0,  // filled_order_delay
        )
    }

    /// Create strategy with custom configuration for optimization
    pub fn new_with_config(
        order_amount: Decimal,
        order_refresh_time: f64,
        filled_order_delay: f64,
    ) -> Self {
        // Multi-layer: 3 layers at 1, 2, 3 bps from mid-price (base)
        let base_layer_spreads_bps = vec![
            Decimal::from(1), // Layer 1: 1 bps
            Decimal::from(2), // Layer 2: 2 bps
            Decimal::from(3), // Layer 3: 3 bps
        ];

        let mut active_bid_orders = HashMap::new();
        let mut active_ask_orders = HashMap::new();
        let mut last_bid_fill_times = HashMap::new();
        let mut last_ask_fill_times = HashMap::new();

        // Initialize tracking for all 3 layers
        for layer in 1..=3 {
            active_bid_orders.insert(layer, None);
            active_ask_orders.insert(layer, None);
            last_bid_fill_times.insert(layer, None);
            last_ask_fill_times.insert(layer, None);
        }

        Self {
            order_amount,
            base_layer_spreads_bps,
            active_bid_orders,
            active_ask_orders,
            last_bid_fill_times,
            last_ask_fill_times,
            last_refresh_time: 0.0,
            order_refresh_time,
            filled_order_delay,
            target_inventory_pct: 0.5,
            soft_limit_pct: 0.6,        // Still used to stop quoting if extreme
            hard_limit_pct: 0.8,        // Still used to stop quoting if extreme
            max_skew_bps: 50.0,         // Max 50 bps skew
            inventory_sensitivity: 5.0, // Skew factor
            price_history: VecDeque::new(),
            volatility_multiplier: 1.0,
            vol_lookback_secs: 300.0,   // 5 minutes
            vol_threshold_bps: 2.0,     // 2 bps baseline vol
            max_spread_multiplier: 4.0, // Up to 4x spreads in high vol
            volatility_enabled: true,
            spread_stretch_enabled: true,
            spread_stretch_multiplier: 5.0,
            dynamic_sizing_enabled: true,
            risky_side_size_pct: 0.25,
            reducing_side_size_pct: 1.50,
            shadowing_enabled: false, // Disabled by default - can cause aggressive placement
            wall_threshold_mult: 5.0,
            shadow_tick_bps: 0.5,
            pingpong_enabled: true,
            pingpong_spread_bps: 0.5,
            maker_fee_bps: 0.25,
            pending_scratches: VecDeque::new(),
            micro_price_enabled: true,
        }
    }

    /// Enable/disable and configure tactical boosts
    pub fn with_spread_stretch(mut self, enabled: bool, multiplier: f64) -> Self {
        self.spread_stretch_enabled = enabled;
        self.spread_stretch_multiplier = multiplier;
        self
    }

    pub fn with_dynamic_sizing(mut self, enabled: bool, risky_pct: f64, reducing_pct: f64) -> Self {
        self.dynamic_sizing_enabled = enabled;
        self.risky_side_size_pct = risky_pct;
        self.reducing_side_size_pct = reducing_pct;
        self
    }

    pub fn with_shadowing(mut self, enabled: bool, wall_mult: f64, tick_bps: f64) -> Self {
        self.shadowing_enabled = enabled;
        self.wall_threshold_mult = wall_mult;
        self.shadow_tick_bps = tick_bps;
        self
    }

    pub fn with_pingpong(mut self, enabled: bool, spread_bps: f64, maker_fee_bps: f64) -> Self {
        self.pingpong_enabled = enabled;
        self.pingpong_spread_bps = spread_bps;
        self.maker_fee_bps = maker_fee_bps;
        self
    }

    pub fn with_micro_price(mut self, enabled: bool) -> Self {
        self.micro_price_enabled = enabled;
        self
    }

    /// Set target inventory percentage (0.0 - 1.0)
    pub fn with_target_inventory_pct(mut self, target_pct: f64) -> Self {
        self.target_inventory_pct = target_pct;
        self
    }

    /// Set inventory limits (0.0 - 1.0)
    pub fn with_inventory_limits(mut self, soft_limit_pct: f64, hard_limit_pct: f64) -> Self {
        self.soft_limit_pct = soft_limit_pct;
        self.hard_limit_pct = hard_limit_pct;
        self
    }

    /// Set skew configuration
    pub fn with_skew_config(mut self, max_skew_bps: f64, sensitivity: f64) -> Self {
        self.max_skew_bps = max_skew_bps;
        self.inventory_sensitivity = sensitivity;
        self
    }

    /// Set base layer spreads in bps. E.g. [10.0, 20.0, 35.0] for wider harvest on volatile pairs.
    pub fn with_base_spreads_bps(mut self, spreads_bps: &[f64]) -> Self {
        self.base_layer_spreads_bps = spreads_bps
            .iter()
            .map(|&s| Decimal::from_f64_retain(s).unwrap_or(dec!(1)))
            .collect();
        self
    }

    /// Enable/disable volatility-aware dynamic spreads
    pub fn with_volatility_adjustment(mut self, enabled: bool) -> Self {
        self.volatility_enabled = enabled;
        self
    }

    /// Set volatility parameters for dynamic spread adjustment
    /// - lookback_secs: How far back to measure volatility (default: 300 = 5 min)
    /// - threshold_bps: Baseline volatility in bps - spreads scale when vol exceeds this
    /// - max_multiplier: Maximum spread multiplier in high vol (default: 4.0)
    pub fn with_volatility_config(
        mut self,
        lookback_secs: f64,
        threshold_bps: f64,
        max_multiplier: f64,
    ) -> Self {
        self.vol_lookback_secs = lookback_secs;
        self.vol_threshold_bps = threshold_bps;
        self.max_spread_multiplier = max_multiplier;
        self
    }

    /// Micro-price: imbalance-weighted mid. Shifts toward heavier side to reduce sniping.
    fn get_micro_price(&self, orderbook: &OrderBook) -> Option<Decimal> {
        if !self.micro_price_enabled {
            return orderbook.mid_price();
        }
        let (best_bid, bid_qty) = orderbook.bids.first().map(|(p, q)| (*p, *q))?;
        let (best_ask, ask_qty) = orderbook.asks.first().map(|(p, q)| (*p, *q))?;
        let total_qty = bid_qty + ask_qty;
        if total_qty <= Decimal::ZERO {
            return orderbook.mid_price();
        }
        // micro_price = (bid * ask_qty + ask * bid_qty) / (bid_qty + ask_qty)
        Some((best_bid * ask_qty + best_ask * bid_qty) / total_qty)
    }

    /// Calculate inventory percentage (base / total portfolio value)
    fn calculate_inventory_pct(&self, portfolio: &Portfolio, mid_price: Decimal) -> f64 {
        let base_value = portfolio.base_balance * mid_price;
        let total_value = base_value + portfolio.quote_balance;

        if total_value <= Decimal::ZERO {
            return 0.5; // Default to balanced if no value
        }

        (base_value / total_value).to_f64().unwrap_or(0.5)
    }

    /// Determine if we should place bid orders based on inventory
    fn should_place_bids(&self, inventory_pct: f64) -> bool {
        // Stop buying if inventory is too high (hard limit)
        inventory_pct < self.hard_limit_pct
    }

    /// Determine if we should place ask orders based on inventory
    fn should_place_asks(&self, inventory_pct: f64) -> bool {
        // Stop selling if inventory is too low (hard limit)
        inventory_pct > (1.0 - self.hard_limit_pct)
    }

    /// Update price history and calculate volatility
    fn update_volatility(&mut self, timestamp: f64, mid_price: f64) {
        self.price_history.push_back((timestamp, mid_price));

        // Remove old entries outside lookback window
        let cutoff = timestamp - self.vol_lookback_secs;
        while let Some(&(t, _)) = self.price_history.front() {
            if t < cutoff {
                self.price_history.pop_front();
            } else {
                break;
            }
        }

        // Calculate realized volatility (std dev of returns) if we have enough data
        if self.volatility_enabled && self.price_history.len() >= 10 {
            let prices: Vec<f64> = self.price_history.iter().map(|(_, p)| *p).collect();
            let mut returns = Vec::with_capacity(prices.len() - 1);
            for i in 1..prices.len() {
                if prices[i - 1] > 0.0 {
                    let ret = (prices[i] - prices[i - 1]) / prices[i - 1];
                    returns.push(ret);
                }
            }

            if returns.len() >= 5 {
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
                let variance =
                    returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
                let vol_bps = (variance.sqrt() * 10000.0).abs(); // Convert to bps

                // Scale multiplier: 1.0 when vol <= threshold, scales up when vol > threshold
                if vol_bps <= self.vol_threshold_bps {
                    self.volatility_multiplier = 1.0;
                } else {
                    let ratio = vol_bps / self.vol_threshold_bps;
                    self.volatility_multiplier =
                        (1.0 + (ratio - 1.0) * 0.5).clamp(1.0, self.max_spread_multiplier);
                }
            }
        }
    }

    /// Get effective spreads (base * volatility multiplier)
    fn get_effective_spreads(&self) -> Vec<Decimal> {
        let mult = Decimal::from_f64_retain(self.volatility_multiplier).unwrap_or(dec!(1));
        self.base_layer_spreads_bps
            .iter()
            .map(|s| (*s * mult).max(dec!(1)))
            .collect()
    }

    /// 1. Asymmetric spread stretch: return (bid_mult, ask_mult). Stretch the side we don't want.
    fn get_spread_stretch_multipliers(&self, inventory_pct: f64) -> (f64, f64) {
        if !self.spread_stretch_enabled {
            return (1.0, 1.0);
        }
        let stretch = self.spread_stretch_multiplier;
        if inventory_pct > self.target_inventory_pct {
            // Too long: stretch bid (don't want more buys), keep ask tight
            (stretch, 1.0)
        } else if inventory_pct < self.target_inventory_pct {
            // Too short: stretch ask (don't want more sells), keep bid tight
            (1.0, stretch)
        } else {
            (1.0, 1.0)
        }
    }

    /// 2. Dynamic sizing: return (bid_size_pct, ask_size_pct) based on inventory health.
    fn get_order_size_multipliers(&self, inventory_pct: f64) -> (f64, f64) {
        if !self.dynamic_sizing_enabled {
            return (1.0, 1.0);
        }
        let dist_to_hard = (self.hard_limit_pct - self.target_inventory_pct).max(0.01);
        let dist_from_target = (inventory_pct - self.target_inventory_pct).abs();
        // How "near" hard limit (0 = at target, 1 = at hard limit)
        let near_limit = (dist_from_target / dist_to_hard).min(1.0);

        if inventory_pct > self.target_inventory_pct {
            let bid_pct = 1.0 - near_limit * (1.0 - self.risky_side_size_pct);
            let ask_pct = 1.0 + near_limit * (self.reducing_side_size_pct - 1.0);
            (bid_pct, ask_pct)
        } else if inventory_pct < self.target_inventory_pct {
            let ask_pct = 1.0 - near_limit * (1.0 - self.risky_side_size_pct);
            let bid_pct = 1.0 + near_limit * (self.reducing_side_size_pct - 1.0);
            (bid_pct, ask_pct)
        } else {
            (1.0, 1.0)
        }
    }

    /// 3. Shadowing: place one tick in front of a wall. Never cross the spread.
    fn get_shadow_adjusted_price(
        &self,
        orderbook: &OrderBook,
        side: OrderSide,
        base_price: Decimal,
        mid_price: Decimal,
    ) -> Decimal {
        if !self.shadowing_enabled {
            return base_price;
        }
        let (best_bid, best_ask) = match (orderbook.best_bid(), orderbook.best_ask()) {
            (Some(b), Some(a)) => (b, a),
            _ => return base_price,
        };
        let levels = match side {
            OrderSide::Buy => &orderbook.bids,
            OrderSide::Sell => &orderbook.asks,
        };
        if levels.len() < 5 {
            return base_price;
        }
        let avg_size: Decimal = levels.iter().take(10).map(|(_, amt)| *amt).sum::<Decimal>()
            / Decimal::from(levels.len().min(10));
        if avg_size <= Decimal::ZERO {
            return base_price;
        }
        // Wall must be significant: avg*mult AND at least 2x our order size (avoid small players)
        let min_wall_size = self.order_amount * dec!(2);
        let threshold = (avg_size
            * Decimal::from_f64_retain(self.wall_threshold_mult).unwrap_or(dec!(5)))
        .max(min_wall_size);
        let tick = mid_price
            * Decimal::from_f64_retain(self.shadow_tick_bps / 10000.0).unwrap_or(dec!(0.0001));

        match side {
            OrderSide::Buy => {
                for (price, amount) in levels.iter().take(20) {
                    if *amount >= threshold && *price >= base_price {
                        let shadow_price = *price + tick;
                        return shadow_price.min(best_ask - tick); // Never cross: must stay < best_ask
                    }
                }
            }
            OrderSide::Sell => {
                for (price, amount) in levels.iter().take(20) {
                    if *amount >= threshold && *price <= base_price {
                        let shadow_price = *price - tick;
                        return shadow_price.max(best_bid + tick); // Never cross: must stay > best_bid
                    }
                }
            }
        }
        base_price
    }
}

impl Strategy for BalancedMMStrategy {
    fn name(&self) -> &str {
        "balanced_mm"
    }

    fn on_orderbook_update(
        &mut self,
        orderbook: &OrderBook,
        portfolio: &Portfolio,
        timestamp: f64,
    ) -> Vec<OrderIntent> {
        let mid_price = match self
            .get_micro_price(orderbook)
            .or_else(|| orderbook.mid_price())
        {
            Some(p) => p,
            None => return Vec::new(),
        };

        // 4. Ping-pong: add all pending scratch orders (multi-scratch support)
        let mut intents = Vec::new();
        while let Some((side, price, amount, _ts)) = self.pending_scratches.pop_front() {
            intents.push(OrderIntent {
                side,
                price,
                amount,
                order_type: mm_core::strategy::OrderType::Limit,
                layer: 0, // Scratch orders use layer 0
            });
        }

        // Update volatility and get effective spreads (dynamic based on volatility)
        let mid_f64 = mid_price.to_f64().unwrap_or(0.0);
        self.update_volatility(orderbook.timestamp, mid_f64);
        let effective_spreads = self.get_effective_spreads();

        // Calculate current inventory percentage
        let inventory_pct = self.calculate_inventory_pct(portfolio, mid_price);

        // Check inventory control limits
        let should_place_bids = self.should_place_bids(inventory_pct);
        let should_place_asks = self.should_place_asks(inventory_pct);

        // 1. Asymmetric spread stretch multipliers
        let (bid_stretch, ask_stretch) = self.get_spread_stretch_multipliers(inventory_pct);

        // 2. Dynamic order sizing
        let (bid_size_pct, ask_size_pct) = self.get_order_size_multipliers(inventory_pct);

        // Calculate Skew
        let deviation = inventory_pct - self.target_inventory_pct;
        let raw_skew_bps = deviation * self.inventory_sensitivity * 100.0;
        let skew_bps = raw_skew_bps.clamp(-self.max_skew_bps, self.max_skew_bps);
        let skew_decimal =
            Decimal::from_f64_retain(skew_bps).unwrap_or(Decimal::ZERO) / dec!(10000);

        let effective_mid = mid_price * (dec!(1) - skew_decimal);

        // Always place initial orders on first call
        let is_initial = self.last_refresh_time == 0.0;
        let time_since_refresh = timestamp - self.last_refresh_time;
        let needs_refresh = is_initial || time_since_refresh >= self.order_refresh_time;

        for (layer_idx, spread_bps) in effective_spreads.iter().enumerate() {
            let layer = (layer_idx + 1) as u32;

            let last_bid_fill = self.last_bid_fill_times.get(&layer).copied().flatten();
            let can_place_bid =
                last_bid_fill.map_or(true, |t| timestamp - t >= self.filled_order_delay);

            let last_ask_fill = self.last_ask_fill_times.get(&layer).copied().flatten();
            let can_place_ask =
                last_ask_fill.map_or(true, |t| timestamp - t >= self.filled_order_delay);

            // 1. Apply asymmetric stretch: bid/ask spread multipliers
            let bid_spread_bps =
                *spread_bps * Decimal::from_f64_retain(bid_stretch).unwrap_or(dec!(1));
            let ask_spread_bps =
                *spread_bps * Decimal::from_f64_retain(ask_stretch).unwrap_or(dec!(1));

            // 2. Dynamic order size
            let bid_amount =
                self.order_amount * Decimal::from_f64_retain(bid_size_pct).unwrap_or(dec!(1));
            let ask_amount =
                self.order_amount * Decimal::from_f64_retain(ask_size_pct).unwrap_or(dec!(1));

            // Place bid
            let active_bid = self.active_bid_orders.get(&layer).cloned().flatten();
            if (active_bid.is_none() || needs_refresh) && can_place_bid && should_place_bids {
                let mut bid_price = effective_mid * (dec!(1) - bid_spread_bps / dec!(10000));
                bid_price =
                    self.get_shadow_adjusted_price(orderbook, OrderSide::Buy, bid_price, mid_price);

                intents.push(OrderIntent {
                    side: OrderSide::Buy,
                    price: bid_price,
                    amount: bid_amount.max(dec!(0.001)), // Min size floor
                    order_type: mm_core::strategy::OrderType::Limit,
                    layer,
                });
                self.active_bid_orders.insert(
                    layer,
                    Some(format!("bid_L{}_", layer) + &timestamp.to_string()),
                );
            }

            // Place ask
            let active_ask = self.active_ask_orders.get(&layer).cloned().flatten();
            if (active_ask.is_none() || needs_refresh) && can_place_ask && should_place_asks {
                let mut ask_price = effective_mid * (dec!(1) + ask_spread_bps / dec!(10000));
                ask_price = self.get_shadow_adjusted_price(
                    orderbook,
                    OrderSide::Sell,
                    ask_price,
                    mid_price,
                );

                intents.push(OrderIntent {
                    side: OrderSide::Sell,
                    price: ask_price,
                    amount: ask_amount.max(dec!(0.001)),
                    order_type: mm_core::strategy::OrderType::Limit,
                    layer,
                });
                self.active_ask_orders.insert(
                    layer,
                    Some(format!("ask_L{}_", layer) + &timestamp.to_string()),
                );
            }
        }

        if !intents.is_empty() {
            self.last_refresh_time = timestamp;
        }

        intents
    }

    fn on_fill(&mut self, fill: &Fill, _portfolio: &mut Portfolio, timestamp: f64) {
        let layer = fill.layer;

        match fill.side {
            OrderSide::Buy => {
                self.last_bid_fill_times.insert(layer, Some(timestamp));
                self.active_bid_orders.insert(layer, None);

                // 4. Ping-pong: queue scratch ASK (sell what we just bought at small premium)
                if self.pingpong_enabled && layer > 0 {
                    // Use abs(fee) for rebates: -0.75 bps rebate still gives positive spread floor
                    let fee_contribution = self.maker_fee_bps.abs() * 2.0;
                    let scratch_spread_bps = (self.pingpong_spread_bps + fee_contribution).max(0.5);
                    let scratch_spread = Decimal::from_f64_retain(scratch_spread_bps)
                        .unwrap_or(dec!(1))
                        / dec!(10000);
                    let scratch_price = fill.price * (dec!(1) + scratch_spread);
                    self.pending_scratches.push_back((
                        OrderSide::Sell,
                        scratch_price,
                        fill.amount,
                        timestamp,
                    ));
                }
            }
            OrderSide::Sell => {
                self.last_ask_fill_times.insert(layer, Some(timestamp));
                self.active_ask_orders.insert(layer, None);

                if self.pingpong_enabled && layer > 0 {
                    let fee_contribution = self.maker_fee_bps.abs() * 2.0;
                    let scratch_spread_bps = (self.pingpong_spread_bps + fee_contribution).max(0.5);
                    let scratch_spread = Decimal::from_f64_retain(scratch_spread_bps)
                        .unwrap_or(dec!(1))
                        / dec!(10000);
                    let scratch_price = fill.price * (dec!(1) - scratch_spread);
                    self.pending_scratches.push_back((
                        OrderSide::Buy,
                        scratch_price,
                        fill.amount,
                        timestamp,
                    ));
                }
            }
        }
    }

    fn validate_config(&self) -> Result<(), StrategyError> {
        for spread_bps in &self.base_layer_spreads_bps {
            if *spread_bps <= Decimal::ZERO {
                return Err(StrategyError::InvalidConfig(
                    "Layer spreads must be positive".to_string(),
                ));
            }
        }
        if self.order_amount <= Decimal::ZERO {
            return Err(StrategyError::InvalidConfig(
                "Order amount must be positive".to_string(),
            ));
        }
        Ok(())
    }
}
