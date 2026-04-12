use mm_core_types::Fill;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Portfolio with average cost accounting.
#[derive(Debug, Clone)]
pub struct Portfolio {
    pub base_balance: Decimal,
    pub quote_balance: Decimal,
    pub avg_cost: Decimal,
    pub realized_pnl: Decimal,
    pub total_fees: Decimal,
}

impl Portfolio {
    pub fn new(initial_base: Decimal, initial_quote: Decimal) -> Self {
        let avg_cost = if initial_base > Decimal::ZERO {
            Decimal::ZERO
        } else {
            Decimal::ZERO
        };
        Self {
            base_balance: initial_base,
            quote_balance: initial_quote,
            avg_cost,
            realized_pnl: Decimal::ZERO,
            total_fees: Decimal::ZERO,
        }
    }

    pub fn apply_fill(&mut self, fill: &Fill, fee_bps: Decimal) {
        let fee = fill.price * fill.amount * fee_bps / dec!(10000);
        self.total_fees += fee;

        match fill.side {
            mm_core_types::Side::Buy => {
                let old_value = self.base_balance * self.avg_cost;
                let new_value = fill.amount * fill.price + fee;
                self.base_balance += fill.amount;
                self.avg_cost = if self.base_balance > Decimal::ZERO {
                    (old_value + new_value) / self.base_balance
                } else {
                    Decimal::ZERO
                };
                self.quote_balance -= fill.price * fill.amount + fee;
            }
            mm_core_types::Side::Sell => {
                let pnl = (fill.price - self.avg_cost) * fill.amount - fee;
                self.realized_pnl += pnl;
                self.base_balance -= fill.amount;
                self.quote_balance += fill.price * fill.amount - fee;
                if self.base_balance <= Decimal::ZERO {
                    self.avg_cost = Decimal::ZERO;
                }
            }
        }
    }

    pub fn mark_to_market(&self, mid: Decimal) -> Decimal {
        self.base_balance * mid + self.quote_balance
    }

    pub fn unrealized_pnl(&self, mid: Decimal) -> Decimal {
        if self.base_balance <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        (mid - self.avg_cost) * self.base_balance
    }
}
