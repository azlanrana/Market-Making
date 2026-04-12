use crate::loader::DataLoader;
use orderbook::snapshot::OrderBookSnapshot;
use anyhow::Result;

/// Loads multiple CSV files and combines them chronologically
pub struct MultiCsvParser {
    file_paths: Vec<String>,
}

impl MultiCsvParser {
    pub fn new(file_paths: Vec<String>) -> Self {
        Self { file_paths }
    }
}

use crate::csv_parser::CsvParser;

impl DataLoader for MultiCsvParser {
    fn load_snapshots(&self) -> Result<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>> {
        // We can't easily chain iterators that return Results of Boxes.
        // So we'll implement a custom iterator that goes through files.
        
        let file_paths = self.file_paths.clone();
        
        // Sort files if needed? The original implementation collected all and sorted by timestamp.
        // Here we'll assume file names or order provided is chronological.
        // If we want to sort by file name, we can do it here.
        // Let's assume input order is correct or the user should sort them.
        
        let iter = MultiCsvIterator {
            file_paths,
            current_file_idx: 0,
            current_iter: None,
        };
        
        Ok(Box::new(iter))
    }
}

struct MultiCsvIterator {
    file_paths: Vec<String>,
    current_file_idx: usize,
    current_iter: Option<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>>,
}

impl Iterator for MultiCsvIterator {
    type Item = Result<OrderBookSnapshot>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = &mut self.current_iter {
                if let Some(item) = iter.next() {
                    return Some(item);
                }
                // Current iterator finished
                self.current_iter = None;
                self.current_file_idx += 1;
            }

            if self.current_file_idx >= self.file_paths.len() {
                return None;
            }

            // Open next file
            let file_path = &self.file_paths[self.current_file_idx];
            let parser = CsvParser::new(file_path.clone());
            match parser.load_snapshots() {
                Ok(iter) => {
                    self.current_iter = Some(iter);
                },
                Err(e) => return Some(Err(anyhow::anyhow!("Failed to load file {}: {}", file_path, e))),
            }
        }
    }
}

