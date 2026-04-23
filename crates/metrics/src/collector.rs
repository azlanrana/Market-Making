use mm_core_types::{Fill, FillReason};
use mm_portfolio::Portfolio;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;

use crate::{InventoryTracker, MarkoutTracker, RoundTrip};

#[derive(Debug, Clone, Default)]
pub struct BacktestStats {
    pub total_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub win_rate: f64,
    pub sharpe: f64,
    pub max_drawdown: f64,
    pub calmar: f64,
    pub round_trip_count: usize,
    pub fill_count: u64,
    pub total_volume: Decimal,
}

/// MM Dashboard summary for strategy health monitoring.
#[derive(Debug, Clone)]
pub struct MMDashboardSummary {
    pub fill_count: u64,
    pub total_volume: f64,
    pub fill_rate_pct: f64,
    pub maker_ratio_pct: f64,
    pub queue_depletion_fill_count: u64,
    pub crossed_book_fill_count: u64,
    pub cancel_ahead_advance_events: u64,
    pub cancel_ahead_advance_total: f64,
    pub queue_depletion_fill_pct: f64,
    pub crossed_book_fill_pct: f64,
    pub net_edge_bps: f64,
    pub markout_1s_bps: f64,
    pub markout_5s_bps: f64,
    pub adverse_selection_1s_bps: f64,
    pub adverse_selection_5s_bps: f64,
    /// Toxic flow: good/neutral/toxic fill %, toxic_bid/ask %
    pub good_fill_pct: f64,
    pub neutral_fill_pct: f64,
    pub toxic_fill_pct: f64,
    pub toxic_bid_pct: f64,
    pub toxic_ask_pct: f64,
    pub realized_spread_capture_pnl: f64,
    pub realized_spread_capture_bps: f64,
    pub rebate_earned_pnl: f64,
    pub rebate_earned_bps: f64,
    pub inventory_drag_pnl: f64,
    pub inventory_drag_bps: f64,
    pub inventory_pnl_ratio: f64,
    pub turnover_daily: f64,
    pub avg_inventory: f64,
    pub max_inventory: f64,
    pub avg_quote_lifetime_ms: f64,
}

/// Metrics collector - round-trip based, record snapshots at fixed intervals.
pub struct MetricsCollector {
    round_trips: Vec<RoundTrip>,
    daily_pnl: HashMap<String, Decimal>,
    daily_volume: HashMap<String, Decimal>,
    /// Subsampled MTM for CSV/plots only — not used for Sharpe or max drawdown.
    portfolio_snapshots: Vec<(f64, Decimal)>,
    /// Last MTM equity per UTC calendar day (last snapshot of that day). Used for daily Sharpe.
    daily_close_equity: HashMap<String, Decimal>,
    /// First MTM seen in the run (full resolution).
    first_equity: Option<Decimal>,
    /// Most recent MTM (full resolution).
    last_equity: Option<Decimal>,
    /// Running peak MTM (f64) for drawdown on every snapshot.
    equity_peak_f64: f64,
    /// Max (peak - pv) / peak over all snapshots with pv > 0.
    risk_max_drawdown: f64,
    fill_count: u64,
    total_volume: Decimal,
    // MM Dashboard
    orders_placed_count: u64,
    maker_fill_count: u64,
    taker_fill_count: u64,
    queue_depletion_fill_count: u64,
    crossed_book_fill_count: u64,
    maker_rebate_total: Decimal,
    taker_fee_total: Decimal,
    markout_enabled: bool,
    markout_tracker: MarkoutTracker,
    inventory_tracker: InventoryTracker,
    quote_placement_ts: f64,
    quote_lifetime_sum: f64,
    quote_lifetime_count: u64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            round_trips: Vec::new(),
            daily_pnl: HashMap::new(),
            daily_volume: HashMap::new(),
            portfolio_snapshots: Vec::new(),
            daily_close_equity: HashMap::new(),
            first_equity: None,
            last_equity: None,
            equity_peak_f64: 0.0,
            risk_max_drawdown: 0.0,
            fill_count: 0,
            total_volume: Decimal::ZERO,
            orders_placed_count: 0,
            maker_fill_count: 0,
            taker_fill_count: 0,
            queue_depletion_fill_count: 0,
            crossed_book_fill_count: 0,
            maker_rebate_total: Decimal::ZERO,
            taker_fee_total: Decimal::ZERO,
            markout_enabled: true,
            markout_tracker: MarkoutTracker::new(),
            inventory_tracker: InventoryTracker::new(),
            quote_placement_ts: 0.0,
            quote_lifetime_sum: 0.0,
            quote_lifetime_count: 0,
        }
    }

    pub fn record_snapshot(&mut self, ts: f64, portfolio_value: Decimal) {
        self.portfolio_snapshots.push((ts, portfolio_value));
    }

    /// Full-resolution equity path: call once per orderbook snapshot. Drives realistic max drawdown
    /// and end-of-day marks for **daily** Sharpe (√252 on consecutive calendar-day closes).
    pub fn record_equity_sample(&mut self, ts: f64, portfolio_value: Decimal) {
        let pv = match portfolio_value.to_f64() {
            Some(p) if p.is_finite() && p > 0.0 => p,
            _ => return,
        };

        if self.first_equity.is_none() {
            self.first_equity = Some(portfolio_value);
            self.equity_peak_f64 = pv;
        }
        self.last_equity = Some(portfolio_value);

        if pv > self.equity_peak_f64 {
            self.equity_peak_f64 = pv;
        }
        let dd = if self.equity_peak_f64 > 0.0 {
            (self.equity_peak_f64 - pv) / self.equity_peak_f64
        } else {
            0.0
        };
        if dd > self.risk_max_drawdown {
            self.risk_max_drawdown = dd;
        }

        let day = Self::day_string(ts);
        self.daily_close_equity.insert(day, portfolio_value);
    }

    pub fn record_order_placed(&mut self, ts: f64, count: u64) {
        self.orders_placed_count += count;
        self.quote_placement_ts = ts;
    }

    pub fn record_fill(&mut self, fill: &Fill, fee_bps: Decimal) {
        let notional = fill.price * fill.amount;
        self.fill_count += 1;
        self.total_volume += notional;
        let day = Self::day_string(fill.timestamp);
        *self.daily_volume.entry(day).or_insert(Decimal::ZERO) += notional;

        let fee_amount = notional * fee_bps / Decimal::from(10000);

        if fill.is_taker {
            self.taker_fill_count += 1;
            self.taker_fee_total += fee_amount;
        } else {
            self.maker_fill_count += 1;
            if fee_amount < Decimal::ZERO {
                self.maker_rebate_total += -fee_amount;
            }
            match fill.fill_reason {
                Some(FillReason::QueueDepletion) => self.queue_depletion_fill_count += 1,
                Some(FillReason::CrossedBook) => self.crossed_book_fill_count += 1,
                None => {}
            }
        }
    }

    pub fn record_fill_for_markout(&mut self, fill: &Fill, mid_at_fill: Decimal) {
        if self.markout_enabled {
            self.markout_tracker.record_fill(fill, mid_at_fill);
        }
    }

    pub fn record_inventory_snapshot(
        &mut self,
        ts: f64,
        base_balance: Decimal,
        quote_balance: Decimal,
        mid_price: Decimal,
        avg_cost: Decimal,
    ) {
        self.inventory_tracker
            .record(ts, base_balance, quote_balance, mid_price, avg_cost);
    }

    pub fn process_markout_snapshot(&mut self, ts: f64, mid_price: Decimal) {
        if self.markout_enabled {
            self.markout_tracker.process_snapshot(ts, mid_price);
        }
    }

    pub fn record_quote_lifetime(&mut self, lifetime_sec: f64) {
        self.quote_lifetime_sum += lifetime_sec;
        self.quote_lifetime_count += 1;
    }

    pub fn fill_rate_pct(&self) -> f64 {
        if self.orders_placed_count == 0 {
            0.0
        } else {
            self.fill_count as f64 / self.orders_placed_count as f64 * 100.0
        }
    }

    pub fn maker_ratio_pct(&self) -> f64 {
        if self.fill_count == 0 {
            100.0
        } else {
            self.maker_fill_count as f64 / self.fill_count as f64 * 100.0
        }
    }

    pub fn markout_tracker(&self) -> &MarkoutTracker {
        &self.markout_tracker
    }

    pub fn markout_enabled(&self) -> bool {
        self.markout_enabled
    }

    pub fn with_markout_enabled(mut self, enabled: bool) -> Self {
        self.markout_enabled = enabled;
        self
    }

    /// Per-fill 1s adverse/markout (for CSV export / distribution analysis).
    pub fn resolved_1s_markouts(&self) -> Vec<crate::markout::Markout1sRecord> {
        if self.markout_enabled {
            self.markout_tracker.resolved_1s_records().to_vec()
        } else {
            Vec::new()
        }
    }

    pub fn inventory_tracker(&self) -> &InventoryTracker {
        &self.inventory_tracker
    }

    pub fn get_dashboard_summary(
        &self,
        portfolio: &Portfolio,
        initial_capital: Decimal,
        _maker_rebate_bps: f64,
        cancel_ahead_advance_events: u64,
        cancel_ahead_advance_total: Decimal,
    ) -> MMDashboardSummary {
        let fill_rate = self.fill_rate_pct();
        let maker_ratio = self.maker_ratio_pct();
        let queue_depletion_fill_pct = if self.maker_fill_count > 0 {
            self.queue_depletion_fill_count as f64 / self.maker_fill_count as f64 * 100.0
        } else {
            0.0
        };
        let crossed_book_fill_pct = if self.maker_fill_count > 0 {
            self.crossed_book_fill_count as f64 / self.maker_fill_count as f64 * 100.0
        } else {
            0.0
        };
        let markout_1s = self.markout_tracker.markout_1s_avg_bps();
        let markout_5s = self.markout_tracker.markout_5s_avg_bps();
        let adverse_1s = self.markout_tracker.adverse_1s_avg_bps();
        let adverse_5s = self.markout_tracker.adverse_5s_avg_bps();
        let tf = self.markout_tracker.toxic_flow_counts();

        let first_val = self
            .first_equity
            .and_then(|v| v.to_f64())
            .or_else(|| {
                self.portfolio_snapshots
                    .first()
                    .and_then(|(_, v)| v.to_f64())
            })
            .unwrap_or_else(|| initial_capital.to_f64().unwrap_or(0.0));
        let last_val = self
            .last_equity
            .and_then(|v| v.to_f64())
            .or_else(|| {
                self.portfolio_snapshots
                    .last()
                    .and_then(|(_, v)| v.to_f64())
            })
            .unwrap_or(first_val);

        let (inventory_drag_pnl, _, inv_ratio) =
            self.inventory_tracker
                .inventory_pnl_ratio(portfolio.realized_pnl, first_val, last_val);

        let vol_f64 = self.total_volume.to_f64().unwrap_or(0.0);
        let days = self.daily_volume.len().max(1) as f64;
        let daily_vol = vol_f64 / days;
        let turnover = if first_val > 0.0 {
            daily_vol / first_val
        } else {
            0.0
        };

        let total_vol_f64 = vol_f64;
        let net_pnl_per_rt = if self.round_trips.is_empty() {
            0.0
        } else {
            let total_rt_pnl: f64 = self
                .round_trips
                .iter()
                .map(|rt| rt.pnl.to_f64().unwrap_or(0.0))
                .sum();
            total_rt_pnl / self.round_trips.len() as f64
        };
        let notional_per_rt = if self.round_trips.is_empty() {
            1.0
        } else {
            total_vol_f64 / self.round_trips.len() as f64
        };
        let net_edge_bps = if notional_per_rt > 0.0 {
            net_pnl_per_rt / notional_per_rt * 10000.0
        } else {
            0.0
        };
        let total_rt_gross_pnl: f64 = self
            .round_trips
            .iter()
            .map(|rt| {
                let gross = match rt.side {
                    mm_core_types::Side::Buy => (rt.close_price - rt.open_price) * rt.amount,
                    mm_core_types::Side::Sell => (rt.open_price - rt.close_price) * rt.amount,
                };
                gross.to_f64().unwrap_or(0.0)
            })
            .sum();
        let rebate_earned_pnl = self.maker_rebate_total.to_f64().unwrap_or(0.0);
        let realized_spread_capture_pnl = total_rt_gross_pnl;
        let inventory_drag_bps = if vol_f64 > 0.0 {
            inventory_drag_pnl / vol_f64 * 10000.0
        } else {
            0.0
        };
        let realized_spread_capture_bps = if vol_f64 > 0.0 {
            realized_spread_capture_pnl / vol_f64 * 10000.0
        } else {
            0.0
        };
        let rebate_earned_bps = if vol_f64 > 0.0 {
            rebate_earned_pnl / vol_f64 * 10000.0
        } else {
            0.0
        };

        let avg_quote_lifetime_ms = if self.quote_lifetime_count > 0 {
            self.quote_lifetime_sum / self.quote_lifetime_count as f64 * 1000.0
        } else {
            0.0
        };

        MMDashboardSummary {
            fill_count: self.fill_count,
            total_volume: vol_f64,
            fill_rate_pct: fill_rate,
            maker_ratio_pct: maker_ratio,
            queue_depletion_fill_count: self.queue_depletion_fill_count,
            crossed_book_fill_count: self.crossed_book_fill_count,
            cancel_ahead_advance_events,
            cancel_ahead_advance_total: cancel_ahead_advance_total.to_f64().unwrap_or(0.0),
            queue_depletion_fill_pct,
            crossed_book_fill_pct,
            net_edge_bps,
            markout_1s_bps: markout_1s,
            markout_5s_bps: markout_5s,
            adverse_selection_1s_bps: adverse_1s,
            adverse_selection_5s_bps: adverse_5s,
            good_fill_pct: tf.good_pct(),
            neutral_fill_pct: tf.neutral_pct(),
            toxic_fill_pct: tf.toxic_pct(),
            toxic_bid_pct: tf.toxic_bid_pct(),
            toxic_ask_pct: tf.toxic_ask_pct(),
            realized_spread_capture_pnl,
            realized_spread_capture_bps,
            rebate_earned_pnl,
            rebate_earned_bps,
            inventory_drag_pnl,
            inventory_drag_bps,
            inventory_pnl_ratio: inv_ratio * 100.0,
            turnover_daily: turnover,
            avg_inventory: self.inventory_tracker.avg_inventory(),
            max_inventory: self.inventory_tracker.max_inventory(),
            avg_quote_lifetime_ms,
        }
    }

    fn day_string(ts: f64) -> String {
        chrono::DateTime::<chrono::Utc>::from_timestamp(ts as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "unknown".to_string())
    }

    pub fn record_round_trip(&mut self, rt: RoundTrip) {
        let day = Self::day_string(rt.close_ts);
        *self.daily_pnl.entry(day).or_insert(Decimal::ZERO) += rt.pnl;
        self.round_trips.push(rt);
    }

    pub fn win_rate(&self) -> f64 {
        if self.round_trips.is_empty() {
            return 0.0;
        }
        let wins = self
            .round_trips
            .iter()
            .filter(|rt| rt.pnl > Decimal::ZERO)
            .count();
        wins as f64 / self.round_trips.len() as f64
    }

    /// Daily Sharpe: simple returns between **UTC calendar-day** closing MTM marks, annualized with √252.
    /// Requires at least three distinct days (two daily returns) for a sample std.
    pub fn sharpe(&self) -> f64 {
        let mut days: Vec<_> = self.daily_close_equity.keys().cloned().collect();
        days.sort_unstable();
        if days.len() < 3 {
            return 0.0;
        }
        let values: Vec<f64> = days
            .iter()
            .filter_map(|d| self.daily_close_equity.get(d).and_then(|v| v.to_f64()))
            .filter(|v| v.is_finite() && *v > 0.0)
            .collect();
        if values.len() < 3 {
            return 0.0;
        }
        let returns: Vec<f64> = values
            .windows(2)
            .filter_map(|w| {
                if w[0] > 0.0 {
                    Some((w[1] - w[0]) / w[0])
                } else {
                    None
                }
            })
            .collect();
        if returns.len() < 2 {
            return 0.0;
        }
        let n = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
        let std = var.sqrt();
        if std > 1e-10 {
            mean / std * 252_f64.sqrt()
        } else {
            0.0
        }
    }

    /// Peak-to-trough drawdown on **every** MTM sample (full snapshot path), not subsampled curves.
    pub fn max_drawdown(&self) -> f64 {
        self.risk_max_drawdown
    }

    pub fn calmar(&self) -> f64 {
        let max_dd = self.max_drawdown();
        if max_dd < 1e-10 {
            return 0.0;
        }
        let total_return = match (self.first_equity, self.last_equity) {
            (Some(first), Some(last)) if first > Decimal::ZERO => {
                ((last - first) / first).to_f64().unwrap_or(0.0)
            }
            _ => {
                if let (Some(first), Some(last)) = (
                    self.portfolio_snapshots.first(),
                    self.portfolio_snapshots.last(),
                ) {
                    if first.1 > Decimal::ZERO {
                        ((last.1 - first.1) / first.1).to_f64().unwrap_or(0.0)
                    } else {
                        0.0
                    }
                } else {
                    0.0
                }
            }
        };
        total_return / max_dd
    }

    /// Portfolio value over time (timestamp, value). For equity curve / drawdown charts.
    pub fn portfolio_snapshots(&self) -> Vec<(f64, f64)> {
        self.portfolio_snapshots
            .iter()
            .map(|(ts, v)| (*ts, v.to_f64().unwrap_or(0.0)))
            .collect()
    }

    pub fn get_stats(&self, portfolio: &Portfolio) -> BacktestStats {
        let total_pnl = match (self.first_equity, self.last_equity) {
            (Some(first), Some(last)) => last - first,
            _ => {
                if let (Some(first), Some(last)) = (
                    self.portfolio_snapshots.first(),
                    self.portfolio_snapshots.last(),
                ) {
                    last.1 - first.1
                } else {
                    Decimal::ZERO
                }
            }
        };

        BacktestStats {
            total_pnl,
            realized_pnl: portfolio.realized_pnl,
            win_rate: self.win_rate(),
            sharpe: self.sharpe(),
            max_drawdown: self.max_drawdown(),
            calmar: self.calmar(),
            round_trip_count: self.round_trips.len(),
            fill_count: self.fill_count,
            total_volume: self.total_volume,
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod risk_metric_tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// 2024-01-01 12:00 UTC, +1 day, +2 days
    fn ts_day(day_offset: i64) -> f64 {
        let base = chrono::DateTime::parse_from_rfc3339("2024-01-01T12:00:00Z")
            .unwrap()
            .timestamp();
        (base + day_offset * 86_400) as f64
    }

    #[test]
    fn max_drawdown_uses_full_path_not_subsample() {
        let mut m = MetricsCollector::new();
        for (ts, v) in [
            (ts_day(0), dec!(100)),
            (ts_day(0) + 1.0, dec!(100)),
            (ts_day(0) + 2.0, dec!(85)),
            (ts_day(0) + 3.0, dec!(92)),
        ] {
            m.record_equity_sample(ts, v);
        }
        let dd = m.max_drawdown();
        assert!(
            (dd - 0.15).abs() < 1e-9,
            "expected 15% drawdown, got {}",
            dd
        );
    }

    #[test]
    fn sharpe_uses_daily_closes_and_sqrt_252() {
        let mut m = MetricsCollector::new();
        // Three distinct UTC days; two daily returns ~ -1% and +2.02% → moderate ratio.
        m.record_equity_sample(ts_day(0), dec!(1_000_000));
        m.record_equity_sample(ts_day(1), dec!(990_000));
        m.record_equity_sample(ts_day(2), dec!(1_010_000));
        let s = m.sharpe();
        assert!(
            s.is_finite() && s > 1.0 && s < 30.0,
            "unexpected daily Sharpe {}",
            s
        );
    }
}
