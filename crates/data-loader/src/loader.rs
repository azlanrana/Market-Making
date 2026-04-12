use orderbook::snapshot::OrderBookSnapshot;
use anyhow::Result;

pub trait DataLoader: Send + Sync {
    fn load_snapshots(&self) -> Result<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>>;
}
