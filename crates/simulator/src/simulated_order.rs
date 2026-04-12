use mm_core_types::{Fill, Order};
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct SimulatedOrder {
    pub order: Order,
    pub market_queue_ahead: Decimal,
    pub internal_queue_ahead: Decimal,
    pub fills: Vec<Fill>,
}

impl SimulatedOrder {
    pub fn new(order: Order, market_queue_ahead: Decimal) -> Self {
        Self {
            order,
            market_queue_ahead,
            internal_queue_ahead: Decimal::ZERO,
            fills: Vec::new(),
        }
    }

    pub fn remaining_amount(&self) -> Decimal {
        self.order.amount - self.order.filled_amount
    }

    pub fn effective_queue_ahead(&self) -> Decimal {
        self.market_queue_ahead + self.internal_queue_ahead
    }
}
