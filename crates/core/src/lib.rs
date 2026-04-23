//! Shared data types for the backtest engine.
//! No logic, no dependencies on other mm crates.

pub mod event;
pub mod fee;
pub mod fill;
pub mod order;
pub mod snapshot;

pub use event::Event;
pub use fee::FeeModel;
pub use fill::{Fill, FillReason};
pub use order::{Order, OrderStatus, OrderType, Side};
pub use snapshot::OrderBookSnapshot;
