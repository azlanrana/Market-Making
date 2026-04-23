use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub struct Portfolio {
    pub base_balance: Decimal,
    pub quote_balance: Decimal,
    pub realized_pnl: Decimal,
    pub cost_basis: Decimal,
}

impl Portfolio {
    pub fn new(initial_base: Decimal, initial_quote: Decimal) -> Self {
        Self {
            base_balance: initial_base,
            quote_balance: initial_quote,
            realized_pnl: Decimal::ZERO,
            cost_basis: Decimal::ZERO,
        }
    }

    pub fn add_trade(
        &mut self,
        side: crate::market_data::OrderSide,
        price: Decimal,
        amount: Decimal,
        fees: Decimal,
    ) {
        match side {
            crate::market_data::OrderSide::Buy => {
                // If we were short, closing short position realizes P&L
                if self.base_balance < Decimal::ZERO {
                    let short_cost = if self.cost_basis != Decimal::ZERO {
                        self.cost_basis / self.base_balance.abs()
                    } else {
                        price
                    };

                    if self.base_balance.abs() <= amount {
                        // Closing entire short position
                        let realized = (short_cost - price) * self.base_balance.abs() - fees;
                        self.realized_pnl += realized;
                        self.cost_basis = Decimal::ZERO;
                    } else {
                        // Partial close
                        let realized = (short_cost - price) * amount - fees;
                        self.realized_pnl += realized;
                        self.cost_basis -= short_cost * amount;
                    }
                }

                // Update balances
                self.base_balance += amount;
                self.quote_balance -= price * amount + fees;

                // Update cost basis for long position
                if self.base_balance > Decimal::ZERO {
                    self.cost_basis += price * amount + fees;
                }
            }
            crate::market_data::OrderSide::Sell => {
                // If we have a long position, selling realizes P&L
                if self.base_balance > Decimal::ZERO && self.cost_basis > Decimal::ZERO {
                    let avg_cost = self.cost_basis / self.base_balance;
                    let realized = (price - avg_cost) * amount - fees;
                    self.realized_pnl += realized;
                    // Update cost basis (reduce by proportion sold)
                    self.cost_basis -= avg_cost * amount;
                }

                // Update balances
                self.base_balance -= amount;
                self.quote_balance += price * amount - fees;

                // If we go short, track short cost basis
                if self.base_balance < Decimal::ZERO {
                    if self.cost_basis == Decimal::ZERO {
                        self.cost_basis = price * amount; // Short cost basis
                    } else {
                        // Average short price
                        let prev_abs = (self.base_balance + amount).abs();
                        self.cost_basis =
                            (self.cost_basis * prev_abs + price * amount) / self.base_balance.abs();
                    }
                }
            }
        }
    }

    pub fn mark_to_market(&self, mid_price: Decimal) -> PortfolioSnapshot {
        // Calculate unrealized P&L
        let unrealized_pnl = if self.base_balance > Decimal::ZERO {
            let avg_cost = if self.base_balance > Decimal::ZERO {
                self.cost_basis / self.base_balance
            } else {
                mid_price
            };
            (mid_price - avg_cost) * self.base_balance
        } else if self.base_balance < Decimal::ZERO {
            // Short position: simplified
            (mid_price - mid_price) * self.base_balance.abs() // TODO: track short cost basis properly
        } else {
            Decimal::ZERO
        };

        let total_pnl = self.realized_pnl + unrealized_pnl;
        let portfolio_value = self.quote_balance + (self.base_balance * mid_price);

        // Inventory percentage
        let inventory_pct = if portfolio_value > Decimal::ZERO {
            let base_value = self.base_balance * mid_price;
            (base_value / portfolio_value).to_f64().unwrap_or(0.5)
        } else {
            // Fallback calculation
            let abs_base_value = (self.base_balance * mid_price).abs();
            let abs_quote_value = self.quote_balance.abs();
            let total_abs = abs_base_value + abs_quote_value;
            if total_abs > Decimal::ZERO {
                (abs_base_value / total_abs).to_f64().unwrap_or(0.5)
            } else {
                0.5
            }
        };

        PortfolioSnapshot {
            timestamp: 0.0, // Will be set by caller
            base_balance: self.base_balance,
            quote_balance: self.quote_balance,
            mid_price,
            realized_pnl: self.realized_pnl,
            unrealized_pnl,
            total_pnl,
            portfolio_value,
            inventory_pct: inventory_pct.max(0.0).min(1.0),
            net_position: self.base_balance,
        }
    }

    pub fn get_inventory_pct(&self, mid_price: Decimal) -> f64 {
        self.mark_to_market(mid_price).inventory_pct
    }
}

#[derive(Debug, Clone)]
pub struct PortfolioSnapshot {
    pub timestamp: f64,
    pub base_balance: Decimal,
    pub quote_balance: Decimal,
    pub mid_price: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub portfolio_value: Decimal,
    pub inventory_pct: f64,
    pub net_position: Decimal,
}
