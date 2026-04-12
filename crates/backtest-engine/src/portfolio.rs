use mm_core::portfolio::Portfolio as CorePortfolio;
use orderbook::order::OrderSide;
use rust_decimal::Decimal;

/// Portfolio tracking for backtesting
/// Matches Hummingbot's Portfolio.add_trade() logic exactly with FIFO cost basis
pub struct BacktestPortfolio {
    core: CorePortfolio,
    trades: Vec<Trade>,
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub timestamp: f64,
    pub side: OrderSide,
    pub price: Decimal,
    pub amount: Decimal,
    pub fees: Decimal,
}

impl BacktestPortfolio {
    pub fn new(initial_base: Decimal, initial_quote: Decimal, _initial_price: Decimal) -> Self {
        Self {
            core: CorePortfolio::new(initial_base, initial_quote),
            trades: Vec::new(),
        }
    }

    pub fn add_trade(
        &mut self,
        timestamp: f64,
        side: OrderSide,
        price: Decimal,
        amount: Decimal,
        fees: Decimal,
    ) {
        // Convert OrderSide to CoreOrderSide
        let core_side = match side {
            OrderSide::Buy => mm_core::market_data::OrderSide::Buy,
            OrderSide::Sell => mm_core::market_data::OrderSide::Sell,
        };

        self.core.add_trade(core_side, price, amount, fees);
        self.trades.push(Trade {
            timestamp,
            side,
            price,
            amount,
            fees,
        });
    }

    pub fn mark_to_market(&self, timestamp: f64, mid_price: Decimal) -> PortfolioSnapshot {
        let snapshot = self.core.mark_to_market(mid_price);
        PortfolioSnapshot {
            timestamp,
            base_balance: snapshot.base_balance,
            quote_balance: snapshot.quote_balance,
            mid_price,
            realized_pnl: snapshot.realized_pnl,
            unrealized_pnl: snapshot.unrealized_pnl,
            total_pnl: snapshot.total_pnl,
            portfolio_value: snapshot.portfolio_value,
            inventory_pct: snapshot.inventory_pct,
            net_position: snapshot.net_position,
        }
    }

    pub fn get_inventory_pct(&self, mid_price: Decimal) -> f64 {
        self.core.get_inventory_pct(mid_price)
    }

    pub fn get_base_balance(&self) -> Decimal {
        self.core.base_balance
    }

    pub fn get_quote_balance(&self) -> Decimal {
        self.core.quote_balance
    }

    pub fn get_realized_pnl(&self) -> Decimal {
        self.core.realized_pnl
    }

    pub fn get_trades(&self) -> &[Trade] {
        &self.trades
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
