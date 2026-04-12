use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub timestamp: f64,
    pub bids: Vec<(Decimal, Decimal)>, // (price, qty) sorted descending
    pub asks: Vec<(Decimal, Decimal)>, // (price, qty) sorted ascending
}

impl OrderBookSnapshot {
    pub fn new(timestamp: f64, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) -> Self {
        Self { timestamp, bids, asks }
    }

    pub fn mid_price(&self) -> Option<Decimal> {
        let best_bid = self.bids.first()?.0;
        let best_ask = self.asks.first()?.0;
        Some((best_bid + best_ask) / Decimal::from(2))
    }

    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.first().map(|(p, _)| *p)
    }

    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|(p, _)| *p)
    }

    pub fn spread_bps(&self) -> Option<f64> {
        let mid = self.mid_price()?;
        let spread = self.best_ask()? - self.best_bid()?;
        if mid.is_zero() {
            return None;
        }
        Some((spread / mid * Decimal::from(10000)).to_f64().unwrap_or(0.0))
    }
}
