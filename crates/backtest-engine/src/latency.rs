use std::time::{SystemTime, UNIX_EPOCH};

/// Default seed for deterministic backtest runs when use_latency is true.
pub const DEFAULT_LATENCY_SEED: u64 = 42;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyType {
    Decision,
    Placement,
    Cancel,
    MarketUpdate,
}

#[derive(Debug, Clone)]
pub struct LatencyConfig {
    pub decision_ms: (f64, f64), // (min, max) in milliseconds
    pub placement_ms: (f64, f64),
    pub cancel_ms: (f64, f64),
    pub market_update_ms: (f64, f64),
    pub distribution: DistributionType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DistributionType {
    Uniform,
    Normal { mean_pct: f64, std_dev_pct: f64 },
}

impl Default for LatencyConfig {
    fn default() -> Self {
        Self {
            decision_ms: (50.0, 200.0),
            placement_ms: (50.0, 200.0),
            cancel_ms: (50.0, 200.0),
            market_update_ms: (10.0, 50.0),
            distribution: DistributionType::Uniform,
        }
    }
}

/// Latency Simulator for realistic backtesting
///
/// Simulates network and exchange latency delays:
/// - Decision latency (time to decide to place order)
/// - Placement latency (time for order to reach exchange)
/// - Cancel latency (time for cancellation to process)
/// - Market update latency (time for market data to reach you)
pub struct LatencySimulator {
    config: LatencyConfig,
    seed: u64,
}

impl LatencySimulator {
    /// Create with system time seed (non-deterministic).
    pub fn new(config: LatencyConfig) -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self { config, seed }
    }

    /// Create with explicit seed for deterministic backtest runs.
    pub fn new_with_seed(config: LatencyConfig, seed: u64) -> Self {
        Self { config, seed }
    }

    pub fn get_latency(&mut self, latency_type: LatencyType) -> f64 {
        let (min_ms, max_ms) = match latency_type {
            LatencyType::Decision => self.config.decision_ms,
            LatencyType::Placement => self.config.placement_ms,
            LatencyType::Cancel => self.config.cancel_ms,
            LatencyType::MarketUpdate => self.config.market_update_ms,
        };

        let latency_ms = match self.config.distribution {
            DistributionType::Uniform => {
                // Simple linear congruential generator for deterministic randomness
                self.seed = self.seed.wrapping_mul(1103515245).wrapping_add(12345);
                let r = (self.seed >> 16) as f64 / 65536.0;
                min_ms + (max_ms - min_ms) * r
            }
            DistributionType::Normal {
                mean_pct,
                std_dev_pct,
            } => {
                // Box-Muller transform for normal distribution
                self.seed = self.seed.wrapping_mul(1103515245).wrapping_add(12345);
                let u1 = (self.seed >> 16) as f64 / 65536.0;
                self.seed = self.seed.wrapping_mul(1103515245).wrapping_add(12345);
                let u2 = (self.seed >> 16) as f64 / 65536.0;

                let z = (-2.0 * (1.0 - u1).ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
                let mean = min_ms + (max_ms - min_ms) * mean_pct;
                let std_dev = (max_ms - min_ms) * std_dev_pct;
                let latency = mean + z * std_dev;
                latency.max(min_ms).min(max_ms)
            }
        };

        // Convert to seconds
        latency_ms / 1000.0
    }

    pub fn apply_decision_latency(&mut self, timestamp: f64) -> f64 {
        timestamp + self.get_latency(LatencyType::Decision)
    }

    pub fn apply_placement_latency(&mut self, timestamp: f64) -> f64 {
        timestamp + self.get_latency(LatencyType::Placement)
    }

    pub fn apply_cancel_latency(&mut self, timestamp: f64) -> f64 {
        timestamp + self.get_latency(LatencyType::Cancel)
    }

    pub fn apply_market_update_latency(&mut self, timestamp: f64) -> f64 {
        timestamp + self.get_latency(LatencyType::MarketUpdate)
    }
}
