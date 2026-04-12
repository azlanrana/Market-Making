use crate::loader::DataLoader;
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use std::fs::File;
use std::io::BufReader;
use anyhow::{Context, Result};

pub struct CsvParser {
    file_path: String,
}

impl CsvParser {
    pub fn new(file_path: String) -> Self {
        Self { file_path }
    }
}

impl DataLoader for CsvParser {
    fn load_snapshots(&self) -> Result<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>> {
        let file = File::open(&self.file_path)
            .with_context(|| format!("Failed to open file: {}", self.file_path))?;
        let reader = BufReader::new(file);
        let rdr = csv::Reader::from_reader(reader);

        let iter = CsvSnapshotIterator {
            records: rdr.into_records(),
        };

        Ok(Box::new(iter))
    }
}

struct CsvSnapshotIterator {
    records: csv::StringRecordsIntoIter<BufReader<File>>,
}

impl Iterator for CsvSnapshotIterator {
    type Item = Result<OrderBookSnapshot>;

    fn next(&mut self) -> Option<Self::Item> {
        let record_res = self.records.next()?;
        
        match record_res {
            Ok(record) => {
                // Parse required fields
                let timestamp: f64 = match record.get(0).and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return Some(Err(anyhow::anyhow!("Missing or invalid timestamp"))),
                };
                
                let mid_price: f64 = match record.get(1).and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return Some(Err(anyhow::anyhow!("Missing or invalid mid_price"))),
                };
                
                let best_bid: f64 = match record.get(2).and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return Some(Err(anyhow::anyhow!("Missing or invalid best_bid"))),
                };
                
                let best_ask: f64 = match record.get(3).and_then(|s| s.parse().ok()) {
                    Some(v) => v,
                    None => return Some(Err(anyhow::anyhow!("Missing or invalid best_ask"))),
                };
                
                let bids_json = match record.get(10) {
                    Some(v) => v,
                    None => return Some(Err(anyhow::anyhow!("Missing bids column"))),
                };
                
                let asks_json = match record.get(11) {
                    Some(v) => v,
                    None => return Some(Err(anyhow::anyhow!("Missing asks column"))),
                };

                let snapshot_res = OrderBookSnapshot::from_csv_row(
                    timestamp,
                    Decimal::from_f64_retain(mid_price).unwrap(),
                    Decimal::from_f64_retain(best_bid).unwrap(),
                    Decimal::from_f64_retain(best_ask).unwrap(),
                    bids_json,
                    asks_json,
                )
                .map_err(|e| anyhow::anyhow!("Failed to parse snapshot: {}", e));

                Some(snapshot_res)
            },
            Err(e) => Some(Err(anyhow::anyhow!("CSV error: {}", e))),
        }
    }
}
