use anyhow::Result;
use orderbook::snapshot::OrderBookSnapshot;

pub trait DataLoader: Send + Sync {
    fn load_snapshots(&self) -> Result<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>>;
}
