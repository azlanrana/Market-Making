//! Inventory exposure and inventory PnL drift tracking.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Default)]
pub struct InventoryTracker {
    sample_count: u64,
    abs_inventory_sum: f64,
    max_abs_inventory: f64,
    first_unrealized_pnl: Option<f64>,
    last_unrealized_pnl: Option<f64>,
}

impl InventoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record portfolio state at a snapshot.
    /// avg_cost: average cost per unit of base (from Portfolio.avg_cost).
    pub fn record(
        &mut self,
        ts: f64,
        base_balance: Decimal,
        quote_balance: Decimal,
        mid_price: Decimal,
        avg_cost: Decimal,
    ) {
        let base_f = base_balance.to_f64().unwrap_or(0.0);
        let mid_f = mid_price.to_f64().unwrap_or(0.0);
        let quote_f = quote_balance.to_f64().unwrap_or(0.0);
        let avg_f = avg_cost.to_f64().unwrap_or(0.0);

        let unrealized_pnl = if base_f > 0.0 && avg_f > 0.0 {
            (mid_f - avg_f) * base_f
        } else if base_f < 0.0 {
            // Short: simplified - treat as zero for now
            0.0
        } else {
            0.0
        };

        let _ = ts;
        let _ = quote_f;

        self.sample_count += 1;
        let abs_inventory = base_f.abs();
        self.abs_inventory_sum += abs_inventory;
        self.max_abs_inventory = self.max_abs_inventory.max(abs_inventory);
        if self.first_unrealized_pnl.is_none() {
            self.first_unrealized_pnl = Some(unrealized_pnl);
        }
        self.last_unrealized_pnl = Some(unrealized_pnl);
    }

    /// Average absolute inventory (in base units) over the run.
    pub fn avg_inventory(&self) -> f64 {
        if self.sample_count == 0 {
            return 0.0;
        }
        self.abs_inventory_sum / self.sample_count as f64
    }

    /// Max absolute inventory reached.
    pub fn max_inventory(&self) -> f64 {
        self.max_abs_inventory
    }

    /// Inventory PnL as fraction of total PnL. Healthy: inventory losses < 30% of profits.
    /// Returns (inventory_pnl, total_pnl, ratio).
    pub fn inventory_pnl_ratio(
        &self,
        _realized_pnl: Decimal,
        first_portfolio_value: f64,
        last_portfolio_value: f64,
    ) -> (f64, f64, f64) {
        let total_pnl = last_portfolio_value - first_portfolio_value;

        let inventory_pnl = if let (Some(first), Some(last)) =
            (self.first_unrealized_pnl, self.last_unrealized_pnl)
        {
            last - first
        } else {
            0.0
        };

        let ratio = if total_pnl.abs() > 1e-10 {
            inventory_pnl / total_pnl
        } else {
            0.0
        };

        (inventory_pnl, total_pnl, ratio)
    }
}
