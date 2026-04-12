use mm_core_types::Side;
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct RoundTrip {
    pub open_ts: f64,
    pub close_ts: f64,
    pub open_price: Decimal,
    pub close_price: Decimal,
    pub amount: Decimal,
    pub pnl: Decimal,
    pub side: Side,
}
