use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub timestamp: f64,
    pub mid_price: Decimal,
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub spread: Decimal,
    pub spread_bps: f64,
    pub bids: Vec<(Decimal, Decimal)>, // (price, amount) sorted descending
    pub asks: Vec<(Decimal, Decimal)>, // (price, amount) sorted ascending
}

impl OrderBookSnapshot {
    pub fn from_csv_row(
        timestamp: f64,
        mid_price: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        bids_json: &str,
        asks_json: &str,
    ) -> Result<Self, String> {
        let mut bids = parse_price_levels(bids_json)?;
        let mut asks = parse_price_levels(asks_json)?;
        bids.sort_by(|a, b| b.0.cmp(&a.0)); // Descending
        asks.sort_by(|a, b| a.0.cmp(&b.0)); // Ascending
        
        let spread = best_ask - best_bid;
        let spread_bps = if mid_price > Decimal::ZERO {
            (spread / mid_price * Decimal::from(10000)).to_f64().unwrap_or(0.0)
        } else {
            0.0
        };

        Ok(Self {
            timestamp,
            mid_price,
            best_bid,
            best_ask,
            spread,
            spread_bps,
            bids,
            asks,
        })
    }
    
    /// Direct constructor from parsed price level vectors (avoids JSON serialization overhead)
    pub fn from_price_levels(
        timestamp: f64,
        mid_price: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        bids: Vec<Vec<f64>>,
        asks: Vec<Vec<f64>>,
    ) -> Result<Self, String> {
        let mut bids_parsed: Vec<(Decimal, Decimal)> = bids.iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    Some((
                        Decimal::from_f64_retain(level[0]).unwrap(),
                        Decimal::from_f64_retain(level[1]).unwrap(),
                    ))
                } else {
                    None
                }
            })
            .collect();
        bids_parsed.sort_by(|a, b| b.0.cmp(&a.0)); // Descending (best bid first)
        
        let mut asks_parsed: Vec<(Decimal, Decimal)> = asks.iter()
            .filter_map(|level| {
                if level.len() >= 2 {
                    Some((
                        Decimal::from_f64_retain(level[0]).unwrap(),
                        Decimal::from_f64_retain(level[1]).unwrap(),
                    ))
                } else {
                    None
                }
            })
            .collect();
        asks_parsed.sort_by(|a, b| a.0.cmp(&b.0)); // Ascending (best ask first)
        
        let spread = best_ask - best_bid;
        let spread_bps = if mid_price > Decimal::ZERO {
            (spread / mid_price * Decimal::from(10000)).to_f64().unwrap_or(0.0)
        } else {
            0.0
        };

        Ok(Self {
            timestamp,
            mid_price,
            best_bid,
            best_ask,
            spread,
            spread_bps,
            bids: bids_parsed,
            asks: asks_parsed,
        })
    }
}

fn parse_price_levels(json_str: &str) -> Result<Vec<(Decimal, Decimal)>, String> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| format!("Failed to parse JSON: {}", e))?;
    
    let array = value.as_array()
        .ok_or_else(|| "Expected array".to_string())?;
    
    let mut levels = Vec::new();
    for item in array {
        let level = item.as_array()
            .ok_or_else(|| "Expected array for price level".to_string())?;
        
        if level.len() != 2 {
            return Err(format!("Expected [price, amount], got {} elements", level.len()));
        }
        
        let price = level[0].as_f64()
            .ok_or_else(|| "Price must be a number".to_string())?;
        let amount = level[1].as_f64()
            .ok_or_else(|| "Amount must be a number".to_string())?;
        
        levels.push((Decimal::from_f64_retain(price).unwrap(), Decimal::from_f64_retain(amount).unwrap()));
    }
    
    Ok(levels)
}
