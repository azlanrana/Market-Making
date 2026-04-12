use crate::fill::Fill;
use crate::snapshot::OrderBookSnapshot;

#[derive(Debug, Clone)]
pub enum Event {
    BookUpdate(OrderBookSnapshot),
    FillReceived(Fill),
    OrderAccepted(String),
    OrderCancelled(String),
    DayBoundary(String),
    BacktestComplete,
}
