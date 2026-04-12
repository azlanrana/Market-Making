//! FIFO position tracker for round-trip detection.
//! Matches buy-then-sell and sell-then-buy to record round trips.

use mm_core_types::{Fill, Side};
use mm_metrics::RoundTrip;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::VecDeque;

/// FIFO queue entry: (amount, price, timestamp)
type QueueEntry = (Decimal, Decimal, f64);

pub struct RoundTripTracker {
    long_queue: VecDeque<QueueEntry>,
    short_queue: VecDeque<QueueEntry>,
}

impl RoundTripTracker {
    pub fn new() -> Self {
        Self {
            long_queue: VecDeque::new(),
            short_queue: VecDeque::new(),
        }
    }

    /// Process a fill and return any completed round trips.
    /// fee_bps: fee in bps for this fill (close side)
    /// maker_bps: assumed fee for open side (typically maker)
    pub fn process_fill(
        &mut self,
        fill: &Fill,
        fee_bps: Decimal,
        maker_bps: Decimal,
    ) -> Vec<RoundTrip> {
        let mut result = Vec::new();
        let mut remaining = fill.amount;

        match fill.side {
            Side::Buy => {
                // Match against short queue first (close short = round trip)
                while remaining > Decimal::ZERO && !self.short_queue.is_empty() {
                    let (s_amt, s_price, s_ts) = self.short_queue.pop_front().unwrap();
                    let match_amt = remaining.min(s_amt);

                    let open_fee = maker_bps * s_price * match_amt / dec!(10000);
                    let close_fee = fee_bps * fill.price * match_amt / dec!(10000);
                    let pnl = (s_price - fill.price) * match_amt - open_fee - close_fee;

                    result.push(RoundTrip {
                        open_ts: s_ts,
                        close_ts: fill.timestamp,
                        open_price: s_price,
                        close_price: fill.price,
                        amount: match_amt,
                        pnl,
                        side: Side::Sell,
                    });

                    let leftover = s_amt - match_amt;
                    if leftover > Decimal::ZERO {
                        self.short_queue.push_front((leftover, s_price, s_ts));
                    }
                    remaining -= match_amt;
                }
                // Remaining opens a long position
                if remaining > Decimal::ZERO {
                    self.long_queue.push_back((remaining, fill.price, fill.timestamp));
                }
            }
            Side::Sell => {
                // Match against long queue first (close long = round trip)
                while remaining > Decimal::ZERO && !self.long_queue.is_empty() {
                    let (b_amt, b_price, b_ts) = self.long_queue.pop_front().unwrap();
                    let match_amt = remaining.min(b_amt);

                    let open_fee = maker_bps * b_price * match_amt / dec!(10000);
                    let close_fee = fee_bps * fill.price * match_amt / dec!(10000);
                    let pnl = (fill.price - b_price) * match_amt - open_fee - close_fee;

                    result.push(RoundTrip {
                        open_ts: b_ts,
                        close_ts: fill.timestamp,
                        open_price: b_price,
                        close_price: fill.price,
                        amount: match_amt,
                        pnl,
                        side: Side::Buy,
                    });

                    let leftover = b_amt - match_amt;
                    if leftover > Decimal::ZERO {
                        self.long_queue.push_front((leftover, b_price, b_ts));
                    }
                    remaining -= match_amt;
                }
                // Remaining opens a short position
                if remaining > Decimal::ZERO {
                    self.short_queue.push_back((remaining, fill.price, fill.timestamp));
                }
            }
        }

        result
    }
}

impl Default for RoundTripTracker {
    fn default() -> Self {
        Self::new()
    }
}
