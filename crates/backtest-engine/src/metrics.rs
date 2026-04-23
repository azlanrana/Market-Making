use crate::portfolio::PortfolioSnapshot;
use chrono::Timelike;
use orderbook::order::OrderSide;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;

/// Metrics collector for backtesting
/// Uses running statistics to avoid storing all snapshots (O(1) memory instead of O(n))
pub struct MetricsCollector {
    // Only store initial and final snapshots
    initial_snapshot: Option<PortfolioSnapshot>,
    final_snapshot: Option<PortfolioSnapshot>,

    // Running statistics
    max_portfolio_value: Decimal,
    min_portfolio_value: Option<Decimal>,
    inventory_sum: f64,
    snapshot_count: u64,

    // Fill tracking
    fills_by_layer: HashMap<u32, u64>,
    total_volume: Decimal,
    total_fees: Decimal,

    // Directional analysis
    buy_volume: Decimal,
    sell_volume: Decimal,
    buy_fills: u64,
    sell_fills: u64,

    // Layer performance
    pnl_by_layer: HashMap<u32, Decimal>,
    volume_by_layer: HashMap<u32, Decimal>,
    pnl_by_day_by_layer: HashMap<String, HashMap<u32, Decimal>>,
    volume_by_day_by_layer: HashMap<String, HashMap<u32, Decimal>>,
    fills_by_day_by_layer: HashMap<String, HashMap<u32, u64>>,

    // Trade quality
    winning_trades: u64,
    losing_trades: u64,
    gross_profit: Decimal,
    gross_loss: Decimal,
    largest_win: Decimal,
    largest_loss: Decimal,

    // Temporal tracking (store snapshots for temporal analysis)
    snapshots: Vec<PortfolioSnapshot>,
    max_inventory: f64,
    min_inventory: f64,

    // Drawdown tracking
    drawdown_start: Option<f64>,
    max_drawdown_duration: f64,
    current_drawdown_duration: f64,
    in_drawdown: bool,

    // Forensic: fill price vs mid (decision DNA)
    /// Per-hour: (sum of fill_price_vs_mid_bps, count). Positive = filled above mid (sell) or below (buy).
    fill_gap_bps_by_hour: HashMap<u32, (f64, u64)>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            initial_snapshot: None,
            final_snapshot: None,
            max_portfolio_value: Decimal::ZERO,
            min_portfolio_value: None,
            inventory_sum: 0.0,
            snapshot_count: 0,
            fills_by_layer: HashMap::new(),
            total_volume: Decimal::ZERO,
            total_fees: Decimal::ZERO,
            buy_volume: Decimal::ZERO,
            sell_volume: Decimal::ZERO,
            buy_fills: 0,
            sell_fills: 0,
            pnl_by_layer: HashMap::new(),
            volume_by_layer: HashMap::new(),
            pnl_by_day_by_layer: HashMap::new(),
            volume_by_day_by_layer: HashMap::new(),
            fills_by_day_by_layer: HashMap::new(),
            winning_trades: 0,
            losing_trades: 0,
            gross_profit: Decimal::ZERO,
            gross_loss: Decimal::ZERO,
            largest_win: Decimal::ZERO,
            largest_loss: Decimal::ZERO,
            snapshots: Vec::new(),
            max_inventory: 0.0,
            min_inventory: 1.0,
            drawdown_start: None,
            max_drawdown_duration: 0.0,
            current_drawdown_duration: 0.0,
            in_drawdown: false,
            fill_gap_bps_by_hour: HashMap::new(),
        }
    }

    /// Forensic: record fill price vs mid gap in bps. Positive = advantageous fill.
    pub fn record_fill_forensic(
        &mut self,
        fill_price: Decimal,
        mid_price: Decimal,
        _side: OrderSide,
        timestamp: f64,
    ) {
        if mid_price <= Decimal::ZERO {
            return;
        }
        let gap_bps = ((fill_price - mid_price) / mid_price * Decimal::from(10000))
            .to_f64()
            .unwrap_or(0.0);
        // For buys: negative gap = we paid below mid (good). For sells: positive = we got above mid (good).
        let hour = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp as i64, 0)
            .map(|dt| dt.hour())
            .unwrap_or(0);
        let entry = self.fill_gap_bps_by_hour.entry(hour).or_insert((0.0, 0));
        entry.0 += gap_bps;
        entry.1 += 1;
    }

    pub fn record_snapshot(&mut self, snapshot: PortfolioSnapshot) {
        // Store first snapshot as initial
        if self.initial_snapshot.is_none() {
            self.initial_snapshot = Some(snapshot.clone());
            self.max_portfolio_value = snapshot.portfolio_value;
            self.min_portfolio_value = Some(snapshot.portfolio_value);
            self.max_inventory = snapshot.inventory_pct;
            self.min_inventory = snapshot.inventory_pct;
        } else {
            // Update running statistics
            if snapshot.portfolio_value > self.max_portfolio_value {
                self.max_portfolio_value = snapshot.portfolio_value;
            }
            if let Some(ref mut min_val) = self.min_portfolio_value {
                if snapshot.portfolio_value < *min_val {
                    *min_val = snapshot.portfolio_value;
                }
            }

            // Track inventory extremes
            if snapshot.inventory_pct > self.max_inventory {
                self.max_inventory = snapshot.inventory_pct;
            }
            if snapshot.inventory_pct < self.min_inventory {
                self.min_inventory = snapshot.inventory_pct;
            }

            // Track drawdown duration
            if snapshot.portfolio_value < self.max_portfolio_value {
                if !self.in_drawdown {
                    self.in_drawdown = true;
                    self.drawdown_start = Some(snapshot.timestamp);
                    self.current_drawdown_duration = 0.0;
                } else {
                    // Estimate duration (assuming snapshots are periodic)
                    if let Some(start) = self.drawdown_start {
                        self.current_drawdown_duration = snapshot.timestamp - start;
                        if self.current_drawdown_duration > self.max_drawdown_duration {
                            self.max_drawdown_duration = self.current_drawdown_duration;
                        }
                    }
                }
            } else {
                if self.in_drawdown {
                    self.in_drawdown = false;
                    self.drawdown_start = None;
                    self.current_drawdown_duration = 0.0;
                }
            }
        }

        // Always update final snapshot
        self.final_snapshot = Some(snapshot.clone());

        // Store snapshot for temporal analysis (limit to reasonable size)
        if self.snapshots.len() < 10000 {
            self.snapshots.push(snapshot.clone());
        }

        self.inventory_sum += snapshot.inventory_pct;
        self.snapshot_count += 1;
    }

    pub fn record_fill(
        &mut self,
        side: OrderSide,
        layer: u32,
        amount: Decimal,
        price: Decimal,
        fees: Decimal,
    ) {
        self.record_fill_with_ts(side, layer, amount, price, fees, 0.0);
    }

    pub fn record_fill_with_ts(
        &mut self,
        side: OrderSide,
        layer: u32,
        amount: Decimal,
        price: Decimal,
        fees: Decimal,
        timestamp: f64,
    ) {
        *self.fills_by_layer.entry(layer).or_insert(0) += 1;
        *self.volume_by_layer.entry(layer).or_insert(Decimal::ZERO) += amount * price;
        self.total_volume += amount * price;
        self.total_fees += fees;

        // Track directional metrics
        let fill_value = amount * price;
        match side {
            OrderSide::Buy => {
                self.buy_volume += fill_value;
                self.buy_fills += 1;
            }
            OrderSide::Sell => {
                self.sell_volume += fill_value;
                self.sell_fills += 1;
            }
        }
        if timestamp > 0.0 {
            if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp as i64, 0) {
                let day = dt.format("%Y-%m-%d").to_string();
                *self
                    .volume_by_day_by_layer
                    .entry(day.clone())
                    .or_default()
                    .entry(layer)
                    .or_insert(Decimal::ZERO) += fill_value;
                *self
                    .fills_by_day_by_layer
                    .entry(day)
                    .or_default()
                    .entry(layer)
                    .or_insert(0) += 1;
            }
        }
    }

    /// Record PnL delta (for running totals by layer/day). Called on every fill.
    pub fn record_trade_pnl_delta(&mut self, pnl: Decimal, layer: u32, timestamp: f64) {
        *self.pnl_by_layer.entry(layer).or_insert(Decimal::ZERO) += pnl;
        if timestamp > 0.0 {
            if let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp as i64, 0) {
                let day = dt.format("%Y-%m-%d").to_string();
                *self
                    .pnl_by_day_by_layer
                    .entry(day)
                    .or_default()
                    .entry(layer)
                    .or_insert(Decimal::ZERO) += pnl;
            }
        }
    }

    /// Record a complete round-trip PnL (win/loss). Only call when an order is fully filled.
    /// This prevents partial fills from inflating the loss count and deflating win rate.
    pub fn record_round_trip_pnl(&mut self, pnl: Decimal, _timestamp: f64) {
        if pnl > Decimal::ZERO {
            self.winning_trades += 1;
            self.gross_profit += pnl;
            if pnl > self.largest_win {
                self.largest_win = pnl;
            }
        } else if pnl < Decimal::ZERO {
            self.losing_trades += 1;
            self.gross_loss += pnl.abs();
            if pnl.abs() > self.largest_loss {
                self.largest_loss = pnl.abs();
            }
        }
    }

    pub fn record_trade_pnl_by_day(&mut self, pnl: Decimal, layer: u32, timestamp: f64) {
        self.record_trade_pnl_delta(pnl, layer, timestamp);
        self.record_round_trip_pnl(pnl, timestamp);
    }

    pub fn record_trade_pnl(&mut self, pnl: Decimal, layer: u32) {
        self.record_trade_pnl_by_day(pnl, layer, 0.0);
    }

    pub fn get_final_stats(
        &self,
        portfolio: &crate::portfolio::BacktestPortfolio,
    ) -> BacktestStats {
        let initial = match &self.initial_snapshot {
            Some(s) => s,
            None => return BacktestStats::default(),
        };

        let final_snapshot = match &self.final_snapshot {
            Some(s) => s,
            None => return BacktestStats::default(),
        };

        let min_value = self.min_portfolio_value.unwrap_or(Decimal::ZERO);
        let max_drawdown = if self.max_portfolio_value > Decimal::ZERO {
            ((self.max_portfolio_value - min_value) / self.max_portfolio_value)
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        let avg_inventory = if self.snapshot_count > 0 {
            self.inventory_sum / self.snapshot_count as f64
        } else {
            0.0
        };

        // Calculate directional P&L from trades
        let trades = portfolio.get_trades();
        let mut buy_pnl = Decimal::ZERO;
        let mut sell_pnl = Decimal::ZERO;

        // Calculate P&L by side (approximate - using realized P&L allocation)
        // This is a simplified calculation - in reality we'd need to track cost basis per side
        let total_trades = trades.len();
        if total_trades > 0 {
            let pnl_per_trade = final_snapshot.realized_pnl / Decimal::from(total_trades);
            for trade in trades {
                match trade.side {
                    OrderSide::Buy => {
                        buy_pnl += pnl_per_trade;
                    }
                    OrderSide::Sell => {
                        sell_pnl += pnl_per_trade;
                    }
                }
            }
        }

        // Calculate trade quality metrics
        let total_trades_count = self.winning_trades + self.losing_trades;
        let win_rate = if total_trades_count > 0 {
            self.winning_trades as f64 / total_trades_count as f64
        } else {
            0.0
        };

        let profit_factor = if self.gross_loss > Decimal::ZERO {
            (self.gross_profit / self.gross_loss)
                .to_f64()
                .unwrap_or(0.0)
        } else if self.gross_profit > Decimal::ZERO {
            f64::INFINITY
        } else {
            0.0
        };

        let avg_win = if self.winning_trades > 0 {
            (self.gross_profit / Decimal::from(self.winning_trades))
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        let avg_loss = if self.losing_trades > 0 {
            (self.gross_loss / Decimal::from(self.losing_trades))
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        // Calculate risk-adjusted metrics
        let return_pct = if initial.portfolio_value > Decimal::ZERO {
            (final_snapshot.total_pnl / initial.portfolio_value)
                .to_f64()
                .unwrap_or(0.0)
        } else {
            0.0
        };

        // Calculate volatility from snapshots
        let returns: Vec<f64> = self
            .snapshots
            .windows(2)
            .map(|w| {
                if w[0].portfolio_value > Decimal::ZERO {
                    ((w[1].portfolio_value - w[0].portfolio_value) / w[0].portfolio_value)
                        .to_f64()
                        .unwrap_or(0.0)
                } else {
                    0.0
                }
            })
            .collect();

        let volatility = if returns.len() > 1 {
            let mean = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance =
                returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
            variance.sqrt()
        } else {
            0.0
        };

        // Calculate downside volatility (only negative returns)
        let downside_returns: Vec<f64> = returns.iter().filter(|&&r| r < 0.0).copied().collect();

        let downside_volatility = if downside_returns.len() > 1 {
            let mean = downside_returns.iter().sum::<f64>() / downside_returns.len() as f64;
            let variance = downside_returns
                .iter()
                .map(|r| (r - mean).powi(2))
                .sum::<f64>()
                / downside_returns.len() as f64;
            variance.sqrt()
        } else {
            0.0
        };

        // Annualize return and volatility (assuming snapshots are ~5 seconds apart)
        let time_periods_per_year = if self.snapshots.len() > 1 {
            let duration = self.snapshots.last().unwrap().timestamp
                - self.snapshots.first().unwrap().timestamp;
            if duration > 0.0 {
                365.0 * 24.0 * 3600.0 / duration
            } else {
                1.0
            }
        } else {
            1.0
        };

        let annualized_return = return_pct * time_periods_per_year;
        let annualized_volatility = volatility * time_periods_per_year.sqrt();
        let annualized_downside_vol = downside_volatility * time_periods_per_year.sqrt();

        // Risk-adjusted ratios
        let sharpe_ratio = if annualized_volatility > 0.0 {
            annualized_return / annualized_volatility
        } else {
            0.0
        };

        let sortino_ratio = if annualized_downside_vol > 0.0 {
            annualized_return / annualized_downside_vol
        } else {
            0.0
        };

        let calmar_ratio = if max_drawdown > 0.0 {
            annualized_return / max_drawdown
        } else {
            0.0
        };

        // Temporal analysis - P&L by day/hour
        // pnl_by_day: portfolio value change (includes mark-to-market on inventory) — directional exposure
        let mut pnl_by_hour: HashMap<u32, Decimal> = HashMap::new();
        let mut pnl_by_day: HashMap<String, Decimal> = HashMap::new();

        if self.snapshots.len() > 1 {
            for i in 1..self.snapshots.len() {
                let value_change =
                    self.snapshots[i].portfolio_value - self.snapshots[i - 1].portfolio_value;
                let timestamp = self.snapshots[i].timestamp;
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp as i64, 0);
                if let Some(dt) = dt {
                    let hour = dt.hour();
                    *pnl_by_hour.entry(hour).or_insert(Decimal::ZERO) += value_change;
                    let day = dt.format("%Y-%m-%d").to_string();
                    *pnl_by_day.entry(day).or_insert(Decimal::ZERO) += value_change;
                }
            }
        }

        // Strategy P&L by day: realized only (trades + rebates - taker fees). Excludes mark-to-market.
        // This is what you want to measure — market-making edge, not ETH price exposure.
        let realized_pnl_by_day: HashMap<String, Decimal> = self
            .pnl_by_day_by_layer
            .iter()
            .map(|(day, layers)| {
                let day_total: Decimal = layers.values().copied().sum();
                (day.clone(), day_total)
            })
            .collect();

        BacktestStats {
            initial_value: initial.portfolio_value,
            final_value: final_snapshot.portfolio_value,
            total_return: final_snapshot.total_pnl,
            return_pct,
            realized_pnl: final_snapshot.realized_pnl,
            unrealized_pnl: final_snapshot.unrealized_pnl,
            max_drawdown,
            max_portfolio_value: self.max_portfolio_value,
            min_portfolio_value: min_value,
            final_inventory_pct: final_snapshot.inventory_pct,
            avg_inventory_pct: avg_inventory,
            total_volume: self.total_volume,
            total_fees: self.total_fees,
            fills_by_layer: self.fills_by_layer.clone(),

            // Directional analysis
            buy_volume: self.buy_volume,
            sell_volume: self.sell_volume,
            buy_pnl,
            sell_pnl,
            buy_fills: self.buy_fills,
            sell_fills: self.sell_fills,
            net_position_over_time: final_snapshot.net_position,

            // Trade quality
            win_rate,
            profit_factor,
            avg_win,
            avg_loss,
            largest_win: self.largest_win.to_f64().unwrap_or(0.0),
            largest_loss: self.largest_loss.to_f64().unwrap_or(0.0),
            total_trades: total_trades_count,

            // Layer performance
            pnl_by_layer: self.pnl_by_layer.clone(),
            volume_by_layer: self.volume_by_layer.clone(),
            pnl_by_day_by_layer: self.pnl_by_day_by_layer.clone(),
            volume_by_day_by_layer: self.volume_by_day_by_layer.clone(),
            fills_by_day_by_layer: self.fills_by_day_by_layer.clone(),

            // Inventory extremes
            max_inventory_reached: self.max_inventory,
            min_inventory_reached: self.min_inventory,

            // Temporal analysis
            pnl_by_hour,
            pnl_by_day,
            realized_pnl_by_day,
            max_drawdown_duration: self.max_drawdown_duration,

            // Risk-adjusted metrics
            sharpe_ratio,
            sortino_ratio,
            calmar_ratio,
            volatility: annualized_volatility,

            // Forensic
            fill_gap_bps_by_hour: self.fill_gap_bps_by_hour.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BacktestStats {
    pub initial_value: Decimal,
    pub final_value: Decimal,
    pub total_return: Decimal,
    pub return_pct: f64,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub max_drawdown: f64,
    pub max_portfolio_value: Decimal,
    pub min_portfolio_value: Decimal,
    pub final_inventory_pct: f64,
    pub avg_inventory_pct: f64,
    pub total_volume: Decimal,
    pub total_fees: Decimal,
    pub fills_by_layer: HashMap<u32, u64>,

    // Directional analysis
    pub buy_volume: Decimal,
    pub sell_volume: Decimal,
    pub buy_pnl: Decimal,
    pub sell_pnl: Decimal,
    pub buy_fills: u64,
    pub sell_fills: u64,
    pub net_position_over_time: Decimal,

    // Trade quality
    pub win_rate: f64,
    pub profit_factor: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub total_trades: u64,

    // Layer performance
    pub pnl_by_layer: HashMap<u32, Decimal>,
    pub volume_by_layer: HashMap<u32, Decimal>,
    pub pnl_by_day_by_layer: HashMap<String, HashMap<u32, Decimal>>,
    pub volume_by_day_by_layer: HashMap<String, HashMap<u32, Decimal>>,
    pub fills_by_day_by_layer: HashMap<String, HashMap<u32, u64>>,

    // Inventory extremes
    pub max_inventory_reached: f64,
    pub min_inventory_reached: f64,

    // Temporal analysis
    pub pnl_by_hour: HashMap<u32, Decimal>,
    pub pnl_by_day: HashMap<String, Decimal>,
    /// Strategy P&L by day (realized only: trades + rebates - taker fees). Excludes mark-to-market.
    pub realized_pnl_by_day: HashMap<String, Decimal>,
    pub max_drawdown_duration: f64,

    // Risk-adjusted metrics
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub volatility: f64,

    // Forensic: fill price vs mid by hour (avg bps). Positive = sold above mid / bought below mid.
    pub fill_gap_bps_by_hour: HashMap<u32, (f64, u64)>,
}
