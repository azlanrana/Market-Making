/// Unified latency model for believable backtest results.
/// All values in microseconds.
#[derive(Debug, Clone)]
pub struct LatencyModel {
    pub order_submission_us: u64,
    pub fill_notification_us: u64,
    pub book_update_us: u64,
    pub cancel_us: u64,
}

impl Default for LatencyModel {
    fn default() -> Self {
        Self {
            order_submission_us: 100_000,
            fill_notification_us: 50_000,
            book_update_us: 25_000,
            cancel_us: 100_000,
        }
    }
}

impl LatencyModel {
    pub fn disabled() -> Self {
        Self {
            order_submission_us: 0,
            fill_notification_us: 0,
            book_update_us: 0,
            cancel_us: 0,
        }
    }

    pub fn colo() -> Self {
        Self {
            order_submission_us: 50_000,
            fill_notification_us: 25_000,
            book_update_us: 10_000,
            cancel_us: 50_000,
        }
    }
}
