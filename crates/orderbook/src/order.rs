use mm_core::market_data::OrderSide as CoreOrderSide;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl From<CoreOrderSide> for OrderSide {
    fn from(side: CoreOrderSide) -> Self {
        match side {
            CoreOrderSide::Buy => OrderSide::Buy,
            CoreOrderSide::Sell => OrderSide::Sell,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Active,
    PartiallyFilled,
    Filled,
    Cancelled,
    Expired,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: String,
    pub side: OrderSide,
    pub price: Decimal,
    pub amount: Decimal,
    pub original_amount: Decimal,
    pub timestamp: f64,
    pub layer: u32,
    pub order_type: String,

    // Status tracking
    pub status: OrderStatus,
    pub filled_amount: Decimal,
    pub remaining_amount: Decimal,

    // Queue position tracking
    pub queue_position: Decimal, // Total size ahead of this order at same price
    pub total_size_at_price: Decimal, // Total size at this price level

    // Fill tracking
    pub fill_timestamps: Vec<(f64, Decimal)>,
    pub avg_fill_price: Decimal,

    // Cancellation
    pub cancel_timestamp: Option<f64>,
}

impl Order {
    pub fn new(
        order_id: String,
        side: OrderSide,
        price: Decimal,
        amount: Decimal,
        timestamp: f64,
        layer: u32,
    ) -> Self {
        Self {
            order_id,
            side,
            price,
            amount,
            original_amount: amount,
            timestamp,
            layer,
            order_type: "limit".to_string(),
            status: OrderStatus::Pending,
            filled_amount: Decimal::ZERO,
            remaining_amount: amount,
            queue_position: Decimal::ZERO,
            total_size_at_price: Decimal::ZERO,
            fill_timestamps: Vec::new(),
            avg_fill_price: Decimal::ZERO,
            cancel_timestamp: None,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::Active | OrderStatus::PartiallyFilled
        )
    }

    pub fn is_filled(&self) -> bool {
        self.status == OrderStatus::Filled
    }

    pub fn is_cancelled(&self) -> bool {
        self.status == OrderStatus::Cancelled
    }

    pub fn fill(&mut self, amount: Decimal, fill_price: Decimal, fill_timestamp: f64) -> Decimal {
        if self.is_filled() || self.is_cancelled() {
            return Decimal::ZERO;
        }

        let actual_fill = amount.min(self.remaining_amount);

        if actual_fill <= Decimal::ZERO {
            return Decimal::ZERO;
        }

        // Update amounts
        self.filled_amount += actual_fill;
        self.remaining_amount -= actual_fill;

        // Update average fill price
        let total_value =
            self.avg_fill_price * (self.filled_amount - actual_fill) + fill_price * actual_fill;
        self.avg_fill_price = if self.filled_amount > Decimal::ZERO {
            total_value / self.filled_amount
        } else {
            fill_price
        };

        // Track fill
        self.fill_timestamps.push((fill_timestamp, actual_fill));

        // Update status
        if self.remaining_amount <= dec!(0.0000000001) {
            self.status = OrderStatus::Filled;
        } else {
            self.status = OrderStatus::PartiallyFilled;
        }

        actual_fill
    }

    pub fn cancel(&mut self, cancel_timestamp: f64) {
        if !self.is_filled() {
            self.status = OrderStatus::Cancelled;
            self.cancel_timestamp = Some(cancel_timestamp);
        }
    }

    pub fn update_queue_position(&mut self, size_ahead: Decimal, total_at_price: Decimal) {
        self.queue_position = size_ahead;
        self.total_size_at_price = total_at_price;
    }

    pub fn get_fill_rate(&self) -> f64 {
        if self.original_amount <= Decimal::ZERO {
            return 0.0;
        }
        (self.filled_amount / self.original_amount)
            .to_f64()
            .unwrap_or(0.0)
    }
}
