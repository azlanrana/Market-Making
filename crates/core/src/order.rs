use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub id: String,
    pub side: Side,
    pub price: Decimal,
    pub amount: Decimal,
    pub order_type: OrderType,
    pub layer: u32,
    pub status: OrderStatus,
    pub filled_amount: Decimal,
    pub created_ts: f64,
}

impl Order {
    pub fn new(id: String, side: Side, price: Decimal, amount: Decimal, order_type: OrderType, layer: u32, created_ts: f64) -> Self {
        Self {
            id,
            side,
            price,
            amount,
            order_type,
            layer,
            status: OrderStatus::Open,
            filled_amount: Decimal::ZERO,
            created_ts,
        }
    }

    pub fn remaining_amount(&self) -> Decimal {
        self.amount - self.filled_amount
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, OrderStatus::Open | OrderStatus::PartiallyFilled)
    }
}
