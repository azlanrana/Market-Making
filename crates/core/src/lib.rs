//! Shared data types for the backtest engine.
//! No logic, no dependencies on other mm crates.

pub mod snapshot;
pub mod order;
pub mod fill;
pub mod event;
pub mod fee;

pub use snapshot::OrderBookSnapshot;
pub use order::{Order, OrderStatus, OrderType, Side};
pub use fill::{Fill, FillReason};
pub use event::Event;
pub use fee::FeeModel;
