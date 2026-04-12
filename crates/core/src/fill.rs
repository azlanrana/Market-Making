use rust_decimal::Decimal;

use crate::order::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillReason {
    QueueDepletion,
    CrossedBook,
}

#[derive(Debug, Clone)]
pub struct Fill {
    pub order_id: String,
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub remaining: Decimal,
    pub is_taker: bool,
    pub fill_reason: Option<FillReason>,
    pub timestamp: f64,
    pub layer: u32,
}
