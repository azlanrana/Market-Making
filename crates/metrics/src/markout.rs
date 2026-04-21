//! Markout tracking: measures adverse selection as price movement after fills.
//!
//! Formula: markout = mid_price_after_fill - mid_price_at_fill
//! Horizons: 100ms, 500ms, 1s, 5s

use mm_core_types::{Fill, FillReason, Side};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Pending fill awaiting future mid prices for markout resolution.
#[derive(Debug, Clone)]
pub struct PendingMarkout {
    pub fill_ts: f64,
    pub mid_at_fill: f64,
    pub side: Side,
    pub order_id: String,
    pub fill_reason: Option<FillReason>,
    #[allow(dead_code)]
    pub amount: f64,
    #[allow(dead_code)]
    pub price: f64,
    #[allow(dead_code)]
    pub is_taker: bool,
    /// Bitmask: bit 0 = 100ms resolved, bit 1 = 500ms, bit 2 = 1s, bit 3 = 5s
    resolved: u8,
}

/// One row per fill once the 1s markout horizon is resolved (matches dashboard toxic/good logic).
#[derive(Debug, Clone)]
pub struct Markout1sRecord {
    pub fill_timestamp: f64,
    pub order_id: String,
    pub side: Side,
    pub mid_at_fill: f64,
    pub mid_1s: f64,
    pub markout_bps: f64,
    pub adverse_bps: f64,
    pub fill_reason: Option<FillReason>,
}

/// Markout horizons in seconds
pub const MARKOUT_100MS: f64 = 0.1;
pub const MARKOUT_500MS: f64 = 0.5;
pub const MARKOUT_1S: f64 = 1.0;
pub const MARKOUT_5S: f64 = 5.0;

pub const MARKOUT_HORIZONS: [f64; 4] = [MARKOUT_100MS, MARKOUT_500MS, MARKOUT_1S, MARKOUT_5S];

/// Threshold in bps: adverse selection beyond this = toxic, below negative = good
pub const TOXIC_FLOW_THRESHOLD_BPS: f64 = 0.2;

#[derive(Debug, Clone, Default)]
pub struct MarkoutStats {
    pub count: usize,
    pub sum_bps: f64,
}

impl MarkoutStats {
    pub fn avg_bps(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_bps / self.count as f64
        }
    }

    pub fn add(&mut self, markout_bps: f64) {
        self.count += 1;
        self.sum_bps += markout_bps;
    }
}

/// Counts of good/neutral/toxic fills (1s markout) for flow quality diagnostics.
#[derive(Debug, Clone, Default)]
pub struct ToxicFlowCounts {
    pub good_count: u64,
    pub neutral_count: u64,
    pub toxic_count: u64,
    pub toxic_bid_count: u64,
    pub toxic_ask_count: u64,
    pub bid_resolved_count: u64,
    pub ask_resolved_count: u64,
}

impl ToxicFlowCounts {
    pub fn total(&self) -> u64 {
        self.good_count + self.neutral_count + self.toxic_count
    }

    pub fn good_pct(&self) -> f64 {
        let t = self.total();
        if t == 0 {
            0.0
        } else {
            self.good_count as f64 / t as f64 * 100.0
        }
    }

    pub fn neutral_pct(&self) -> f64 {
        let t = self.total();
        if t == 0 {
            0.0
        } else {
            self.neutral_count as f64 / t as f64 * 100.0
        }
    }

    pub fn toxic_pct(&self) -> f64 {
        let t = self.total();
        if t == 0 {
            0.0
        } else {
            self.toxic_count as f64 / t as f64 * 100.0
        }
    }

    /// Among bid fills, what % were toxic?
    pub fn toxic_bid_pct(&self) -> f64 {
        if self.bid_resolved_count == 0 {
            0.0
        } else {
            self.toxic_bid_count as f64 / self.bid_resolved_count as f64 * 100.0
        }
    }

    /// Among ask fills, what % were toxic?
    pub fn toxic_ask_pct(&self) -> f64 {
        if self.ask_resolved_count == 0 {
            0.0
        } else {
            self.toxic_ask_count as f64 / self.ask_resolved_count as f64 * 100.0
        }
    }
}

/// Tracks pending fills and resolves markouts when later snapshots arrive.
pub struct MarkoutTracker {
    pending: Vec<PendingMarkout>,
    stats_by_horizon: [MarkoutStats; 4],
    adverse_stats_by_horizon: [MarkoutStats; 4],
    toxic_flow: ToxicFlowCounts,
    resolved_1s: Vec<Markout1sRecord>,
}

impl MarkoutTracker {
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            stats_by_horizon: Default::default(),
            adverse_stats_by_horizon: Default::default(),
            toxic_flow: ToxicFlowCounts::default(),
            resolved_1s: Vec::new(),
        }
    }

    /// Record a fill for markout tracking.
    pub fn record_fill(&mut self, fill: &Fill, mid_at_fill: Decimal) {
        let mid_f64 = mid_at_fill.to_f64().unwrap_or(0.0);
        let price_f64 = fill.price.to_f64().unwrap_or(0.0);
        let amount_f64 = fill.amount.to_f64().unwrap_or(0.0);
        if mid_f64 <= 0.0 {
            return;
        }
        self.pending.push(PendingMarkout {
            fill_ts: fill.timestamp,
            mid_at_fill: mid_f64,
            side: fill.side,
            order_id: fill.order_id.clone(),
            fill_reason: fill.fill_reason,
            amount: amount_f64,
            price: price_f64,
            is_taker: fill.is_taker,
            resolved: 0,
        });
    }

    /// Process a new snapshot: resolve any pending markouts for which we now have data.
    pub fn process_snapshot(&mut self, snapshot_ts: f64, mid_price: Decimal) {
        let mid_f64 = mid_price.to_f64().unwrap_or(0.0);
        if mid_f64 <= 0.0 {
            return;
        }

        let mut still_pending = Vec::with_capacity(self.pending.len());
        for mut p in std::mem::take(&mut self.pending) {
            for (idx, &horizon) in MARKOUT_HORIZONS.iter().enumerate() {
                let bit = 1u8 << idx;
                if (p.resolved & bit) == 0 && snapshot_ts >= p.fill_ts + horizon {
                    let markout_bps = (mid_f64 - p.mid_at_fill) / p.mid_at_fill * 10000.0;
                    let adverse_bps = match p.side {
                        Side::Buy => -markout_bps,
                        Side::Sell => markout_bps,
                    };
                    self.stats_by_horizon[idx].add(markout_bps);
                    self.adverse_stats_by_horizon[idx].add(adverse_bps);
                    // Classify toxic flow at 1s horizon only
                    if idx == 2 {
                        self.resolved_1s.push(Markout1sRecord {
                            fill_timestamp: p.fill_ts,
                            order_id: p.order_id.clone(),
                            side: p.side,
                            mid_at_fill: p.mid_at_fill,
                            mid_1s: mid_f64,
                            markout_bps,
                            adverse_bps,
                            fill_reason: p.fill_reason,
                        });
                        match p.side {
                            Side::Buy => self.toxic_flow.bid_resolved_count += 1,
                            Side::Sell => self.toxic_flow.ask_resolved_count += 1,
                        }
                        if adverse_bps > TOXIC_FLOW_THRESHOLD_BPS {
                            self.toxic_flow.toxic_count += 1;
                            match p.side {
                                Side::Buy => self.toxic_flow.toxic_bid_count += 1,
                                Side::Sell => self.toxic_flow.toxic_ask_count += 1,
                            }
                        } else if adverse_bps < -TOXIC_FLOW_THRESHOLD_BPS {
                            self.toxic_flow.good_count += 1;
                        } else {
                            self.toxic_flow.neutral_count += 1;
                        }
                    }
                    p.resolved |= bit;
                }
            }
            if p.resolved == 0b1111 || snapshot_ts >= p.fill_ts + MARKOUT_5S {
                continue;
            } else {
                still_pending.push(p);
            }
        }
        self.pending = still_pending;
    }

    /// Get stats for a horizon index (0=100ms, 1=500ms, 2=1s, 3=5s)
    pub fn stats(&self, horizon_idx: usize) -> &MarkoutStats {
        &self.stats_by_horizon[horizon_idx.min(3)]
    }

    /// Get 1s markout avg (index 2)
    pub fn markout_1s_avg_bps(&self) -> f64 {
        self.stats_by_horizon[2].avg_bps()
    }

    /// Get 5s markout avg (index 3)
    pub fn markout_5s_avg_bps(&self) -> f64 {
        self.stats_by_horizon[3].avg_bps()
    }

    /// Get 1s adverse selection avg (positive = takers had better info)
    pub fn adverse_1s_avg_bps(&self) -> f64 {
        self.adverse_stats_by_horizon[2].avg_bps()
    }

    /// Get 5s adverse selection avg (positive = bad for us)
    pub fn adverse_5s_avg_bps(&self) -> f64 {
        self.adverse_stats_by_horizon[3].avg_bps()
    }

    /// Get toxic flow counts for diagnostics (good/neutral/toxic %, toxic_bid/ask %)
    pub fn toxic_flow_counts(&self) -> &ToxicFlowCounts {
        &self.toxic_flow
    }

    /// Per-fill 1s markout rows (same count as fills that survived to 1s horizon resolution).
    pub fn resolved_1s_records(&self) -> &[Markout1sRecord] {
        &self.resolved_1s
    }
}

impl Default for MarkoutTracker {
    fn default() -> Self {
        Self::new()
    }
}
