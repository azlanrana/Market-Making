use crate::market_data::{MarketData, OrderBook};
use crate::portfolio::Portfolio;
use rust_decimal::Decimal;

#[derive(Debug, thiserror::Error)]
pub enum StrategyError {
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Strategy error: {0}")]
    Strategy(String),
}

#[derive(Debug, Clone)]
pub struct OrderIntent {
    pub side: crate::market_data::OrderSide,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub layer: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    /// Cancel orders for this (side, layer) without placing - used for kill-switch scratch
    Cancel,
}

pub trait Strategy: Send + Sync {
    /// Get the name of the strategy
    fn name(&self) -> &str;

    /// Called on each order book update
    fn on_orderbook_update(
        &mut self,
        orderbook: &OrderBook,
        portfolio: &Portfolio,
        timestamp: f64,
    ) -> Vec<OrderIntent>;

    /// Called when an order fills
    fn on_fill(
        &mut self,
        fill: &Fill,
        portfolio: &mut Portfolio,
        timestamp: f64,
    );

    /// Called periodically (for time-based logic)
    fn on_tick(
        &mut self,
        market_data: &MarketData,
        portfolio: &Portfolio,
        timestamp: f64,
    ) -> Vec<OrderIntent> {
        // Default implementation: delegate to on_orderbook_update
        self.on_orderbook_update(&market_data.orderbook, portfolio, timestamp)
    }

    /// Validate configuration
    fn validate_config(&self) -> Result<(), StrategyError>;

    /// Optional gate/filter diagnostics for debugging (e.g. "[GATES] imbalance=...")
    fn gate_diagnostics(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: String,
    pub side: crate::market_data::OrderSide,
    pub price: Decimal,
    pub amount: Decimal,
    pub timestamp: f64,
    pub layer: u32,
}
