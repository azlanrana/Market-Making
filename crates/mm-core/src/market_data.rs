use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    pub fn opposite(&self) -> Self {
        match self {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: Vec<(Decimal, Decimal)>, // (price, amount) sorted descending
    pub asks: Vec<(Decimal, Decimal)>, // (price, amount) sorted ascending
    pub timestamp: f64,
}

impl OrderBook {
    pub fn new(timestamp: f64) -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
            timestamp,
        }
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        let best_bid = self.bids.first()?.0;
        let best_ask = self.asks.first()?.0;
        Some((best_bid + best_ask) / Decimal::from(2))
    }

    pub fn spread(&self) -> Option<Decimal> {
        let best_bid = self.bids.first()?.0;
        let best_ask = self.asks.first()?.0;
        Some(best_ask - best_bid)
    }

    pub fn spread_bps(&self) -> Option<f64> {
        let spread = self.spread()?;
        let mid = self.mid_price()?;
        if mid.is_zero() {
            return None;
        }
        Some((spread / mid * Decimal::from(10000)).to_f64().unwrap_or(0.0))
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|(price, _)| *price)
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|(price, _)| *price)
    }
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub orderbook: OrderBook,
    pub timestamp: f64,
}
