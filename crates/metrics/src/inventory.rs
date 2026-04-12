//! Inventory exposure and inventory PnL drift tracking.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

#[derive(Debug, Clone, Default)]
pub struct InventorySnapshot {
    #[allow(dead_code)]
    pub ts: f64,
    #[allow(dead_code)]
    pub base_balance: f64,
    #[allow(dead_code)]
    pub mid_price: f64,
    #[allow(dead_code)]
    pub portfolio_value: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone)]
pub struct InventoryTracker {
    snapshots: Vec<InventorySnapshot>,
}

impl InventoryTracker {
    pub fn new() -> Self {
        Self {
            snapshots: Vec::new(),
        }
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

        let portfolio_value = quote_f + base_f * mid_f;

        let unrealized_pnl = if base_f > 0.0 && avg_f > 0.0 {
            (mid_f - avg_f) * base_f
        } else if base_f < 0.0 {
            // Short: simplified - treat as zero for now
            0.0
        } else {
            0.0
        };

        self.snapshots.push(InventorySnapshot {
            ts,
            base_balance: base_f,
            mid_price: mid_f,
            portfolio_value,
            unrealized_pnl,
        });
    }

    /// Average absolute inventory (in base units) over the run.
    pub fn avg_inventory(&self) -> f64 {
        if self.snapshots.is_empty() {
            return 0.0;
        }
        self.snapshots.iter().map(|s| s.base_balance.abs()).sum::<f64>() / self.snapshots.len() as f64
    }

    /// Max absolute inventory reached.
    pub fn max_inventory(&self) -> f64 {
        self.snapshots.iter().map(|s| s.base_balance.abs()).fold(0.0f64, f64::max)
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
            (self.snapshots.first(), self.snapshots.last())
        {
            last.unrealized_pnl - first.unrealized_pnl
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

