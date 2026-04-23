use crate::loader::DataLoader;
use crate::sftp_loader::SnapshotRow;
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use chrono::NaiveDate;
use flate2::read::GzDecoder;
use lru::LruCache;
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use std::io::prelude::*;
use std::io::BufReader;
use std::num::NonZeroUsize;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// Optional inclusive calendar range on object keys. Set both `S3_START_DATE` and `S3_END_DATE`
/// to `YYYY-MM-DD`, or set neither. Keys that do not contain a valid `/YYYY/MM/DD/` path segment
/// triplet are **kept** (so odd layouts still load).
pub fn parse_s3_inclusive_date_range_from_env() -> Result<Option<(NaiveDate, NaiveDate)>> {
    match (std::env::var("S3_START_DATE"), std::env::var("S3_END_DATE")) {
        (Err(_), Err(_)) => Ok(None),
        (Ok(start_s), Ok(end_s)) => {
            let start = NaiveDate::parse_from_str(&start_s.trim(), "%Y-%m-%d")
                .with_context(|| format!("S3_START_DATE={start_s:?} (expected YYYY-MM-DD)"))?;
            let end = NaiveDate::parse_from_str(&end_s.trim(), "%Y-%m-%d")
                .with_context(|| format!("S3_END_DATE={end_s:?} (expected YYYY-MM-DD)"))?;
            if end < start {
                anyhow::bail!("S3_END_DATE must be on or after S3_START_DATE");
            }
            Ok(Some((start, end)))
        }
        _ => anyhow::bail!("Set both S3_START_DATE and S3_END_DATE, or neither"),
    }
}

/// Last valid Gregorian `YYYY/MM/DD` along the key path (three **consecutive** segments).
/// Handles layouts like `.../2025/2025/01/15/cdc/...` by taking the final valid triplet (`2025-01-15`).
fn last_calendar_date_in_key(key: &str) -> Option<NaiveDate> {
    let parts: Vec<&str> = key.split('/').collect();
    let mut last = None;
    for i in 0..parts.len().saturating_sub(2) {
        if let Some(d) = try_parse_yyyymmdd_triplet(parts[i], parts[i + 1], parts[i + 2]) {
            last = Some(d);
        }
    }
    last
}

fn try_parse_yyyymmdd_triplet(y: &str, m: &str, d: &str) -> Option<NaiveDate> {
    if y.len() != 4 || !y.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let yy: i32 = y.parse().ok()?;
    let mm: u32 = m.parse().ok()?;
    let dd: u32 = d.parse().ok()?;
    NaiveDate::from_ymd_opt(yy, mm, dd)
}

/// S3 loader for orderbook snapshot history stored in AWS S3
///
/// Downloads compressed orderbook snapshots from S3, decompresses and parses them
/// Folder structure in S3: bucket/prefix/yyyy/mm/dd/filename.gz
pub struct S3Loader {
    s3_client: Arc<S3Client>,
    bucket: String,
    prefix: String,
    #[allow(dead_code)]
    region: String,
    max_files: Option<usize>,
    max_concurrent_downloads: usize,
    /// When set, only load files whose S3 key contains /cdc/{pair}/. Filters out wrong-pair data in mixed prefix.
    pair_filter: Option<String>,
    /// Inclusive calendar range on [`last_calendar_date_in_key`]. Keys with no parseable date are kept.
    key_date_range: Option<(NaiveDate, NaiveDate)>,
}

impl S3Loader {
    /// Create a new S3 loader
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `prefix` - S3 prefix/path (e.g., "backtest-data/2023/10/25/")
    /// * `region` - AWS region (e.g., "us-east-1")
    /// * `max_concurrent_downloads` - Maximum parallel downloads
    pub async fn new(
        bucket: String,
        prefix: String,
        region: String,
        max_concurrent_downloads: usize,
    ) -> Result<Self> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .load()
            .await;

        let s3_client = S3Client::new(&config);

        Ok(Self {
            s3_client: Arc::new(s3_client),
            bucket,
            prefix,
            region,
            max_files: None,
            max_concurrent_downloads,
            pair_filter: None,
            key_date_range: None,
        })
    }

    /// Only load files whose S3 key contains /cdc/{pair}/. Filters out wrong-pair data (e.g. BTC_USDT under ETH prefix).
    pub fn with_pair_filter(mut self, pair: impl Into<String>) -> Self {
        self.pair_filter = Some(pair.into());
        self
    }

    /// Cap how many objects to list (after pair + optional date filters). `None` = no cap.
    pub fn with_max_files(mut self, max_files: Option<usize>) -> Self {
        self.max_files = max_files;
        self
    }

    /// Inclusive calendar filter on key path (see [`last_calendar_date_in_key`]). `None` = no filter.
    pub fn with_s3_key_date_range(mut self, range: Option<(NaiveDate, NaiveDate)>) -> Self {
        self.key_date_range = range;
        self
    }

    /// List `.gz` keys under `prefix` in **S3 lexicographic order** (same as `list_objects_v2` order).
    /// Optional `/cdc/{pair}/` filter. If `max_take` is set, **stop paginating** once that many keys
    /// pass the filter — avoids listing the entire prefix when only the first N files are needed
    /// (equivalent to list-all → filter → sort → take(N), since S3 order is already sorted).
    async fn list_gz_keys_limited(&self, max_take: Option<usize>) -> Result<Vec<String>> {
        let pattern = self.pair_filter.as_ref().map(|p| format!("/cdc/{}/", p));
        let mut keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .s3_client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);

            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }

            let response = request.send().await.with_context(|| {
                format!(
                    "Failed to list objects in s3://{}/{}",
                    self.bucket, self.prefix
                )
            })?;

            for object in response.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                if !key.ends_with(".gz") {
                    continue;
                }
                if let Some(ref pat) = pattern {
                    if !key.contains(pat.as_str()) {
                        continue;
                    }
                }
                if let Some((start, end)) = self.key_date_range {
                    if let Some(dt) = last_calendar_date_in_key(key) {
                        if dt < start || dt > end {
                            continue;
                        }
                    }
                }
                keys.push(key.to_string());
                if let Some(max) = max_take {
                    if keys.len() >= max {
                        if let Some(ref pair) = self.pair_filter {
                            println!(
                                "Pair filter '{}': {} files (listing stopped at max_files={}; total .gz under prefix not fully scanned)",
                                pair, keys.len(), max
                            );
                        } else {
                            println!(
                                "Stopped listing at max_files={} (.gz under prefix not fully scanned)",
                                max
                            );
                        }
                        return Ok(keys);
                    }
                }
            }

            continuation_token = response.next_continuation_token().map(|s| s.to_string());
            if continuation_token.is_none() {
                break;
            }
        }

        if let Some(ref pair) = self.pair_filter {
            println!("Pair filter '{}': {} files selected", pair, keys.len());
        }
        if let Some((s, e)) = self.key_date_range {
            println!(
                "Key date filter: {} .. {} (inclusive) → {} files",
                s,
                e,
                keys.len()
            );
        }

        Ok(keys)
    }

    /// Parse snapshot data (reuse logic from SftpLoader)
    fn parse_snapshot_data(&self, data: &[u8]) -> Result<Vec<OrderBookSnapshot>> {
        let mut snapshots = Vec::new();

        let first_bytes = if data.len() > 100 { &data[..100] } else { data };
        let is_json = first_bytes.starts_with(b"{") || first_bytes.starts_with(b"[");
        let is_text = std::str::from_utf8(first_bytes).is_ok();

        if is_text || is_json {
            let text = std::str::from_utf8(data)?;
            let reader = BufReader::new(text.as_bytes());

            for (line_num, line_result) in reader.lines().enumerate() {
                let line = line_result?;
                if line.trim().is_empty() {
                    continue;
                }

                if let Ok(row) = serde_json::from_str::<SnapshotRow>(&line) {
                    let timestamp_ms =
                        row.t
                            .or(row.timestamp)
                            .or(row.ts)
                            .or(row.time)
                            .ok_or_else(|| {
                                anyhow::anyhow!("Missing timestamp at line {}", line_num + 1)
                            })?;

                    let timestamp = timestamp_ms as f64 / 1000.0;
                    let bids = row.b.as_deref().unwrap_or(&[]);
                    let asks = row.a.as_deref().unwrap_or(&[]);

                    if bids.is_empty() && asks.is_empty() {
                        continue;
                    }

                    let best_bid = bids
                        .iter()
                        .filter_map(|level: &Vec<f64>| level.get(0).copied())
                        .fold(f64::NEG_INFINITY, f64::max);

                    let best_ask = asks
                        .iter()
                        .filter_map(|level: &Vec<f64>| level.get(0).copied())
                        .fold(f64::INFINITY, f64::min);

                    if best_bid == f64::NEG_INFINITY || best_ask == f64::INFINITY {
                        continue;
                    }

                    let mid_price = (best_bid + best_ask) / 2.0;

                    let snapshot = OrderBookSnapshot::from_price_levels(
                        timestamp,
                        Decimal::from_f64_retain(mid_price).unwrap(),
                        Decimal::from_f64_retain(best_bid).unwrap(),
                        Decimal::from_f64_retain(best_ask).unwrap(),
                        bids.to_vec(),
                        asks.to_vec(),
                    )
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to parse snapshot at line {}: {}", line_num + 1, e)
                    })?;

                    snapshots.push(snapshot);
                } else {
                    // Try CSV format
                    let parts: Vec<&str> = line.split(',').collect();
                    if parts.len() >= 12 {
                        let timestamp: f64 = parts[0].parse().with_context(|| {
                            format!("Invalid timestamp at line {}", line_num + 1)
                        })?;
                        let mid_price: f64 = parts[1].parse().with_context(|| {
                            format!("Invalid mid_price at line {}", line_num + 1)
                        })?;
                        let best_bid: f64 = parts[2].parse().with_context(|| {
                            format!("Invalid best_bid at line {}", line_num + 1)
                        })?;
                        let best_ask: f64 = parts[3].parse().with_context(|| {
                            format!("Invalid best_ask at line {}", line_num + 1)
                        })?;

                        let bids_json = parts.get(10).unwrap_or(&"[]");
                        let asks_json = parts.get(11).unwrap_or(&"[]");

                        let snapshot = OrderBookSnapshot::from_csv_row(
                            timestamp,
                            Decimal::from_f64_retain(mid_price).unwrap(),
                            Decimal::from_f64_retain(best_bid).unwrap(),
                            Decimal::from_f64_retain(best_ask).unwrap(),
                            bids_json,
                            asks_json,
                        )
                        .map_err(|e| anyhow::anyhow!("Failed to parse snapshot: {}", e))?;

                        snapshots.push(snapshot);
                    }
                }
            }
        }

        Ok(snapshots)
    }

    /// Download and parse worker (async version that gets converted to sync)
    async fn download_and_parse_worker_async(
        s3_client: Arc<S3Client>,
        bucket: String,
        key: String,
        cache: Option<Arc<Mutex<LruCache<String, Vec<OrderBookSnapshot>>>>>,
    ) -> Result<Vec<OrderBookSnapshot>> {
        // Extract cache key
        let cache_key = key
            .split('/')
            .last()
            .unwrap_or(&key)
            .strip_suffix(".gz")
            .unwrap_or(&key)
            .to_string();

        // Check cache first
        if let Some(ref cache) = cache {
            let mut cache_guard = cache.lock().unwrap();
            if let Some(cached_snapshots) = cache_guard.get(&cache_key) {
                return Ok(cached_snapshots.clone());
            }
        }

        // Download and decompress
        let response = s3_client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .with_context(|| format!("Failed to download s3://{}/{}", bucket, key))?;

        let mut compressed_data = Vec::new();
        let mut body = response.body;

        while let Some(chunk) = body.next().await {
            let chunk = chunk.context("Failed to read S3 object chunk")?;
            compressed_data.extend_from_slice(&chunk);
        }

        // Decompress
        let decompressed = if key.ends_with(".gz") {
            let mut decoder = GzDecoder::new(compressed_data.as_slice());
            let mut data = Vec::new();
            decoder
                .read_to_end(&mut data)
                .with_context(|| "Failed to decompress file")?;
            data
        } else {
            compressed_data
        };

        // Parse (create temporary loader for parsing)
        let temp_loader = S3Loader {
            s3_client: Arc::clone(&s3_client),
            bucket: bucket.clone(),
            prefix: String::new(),
            region: String::new(),
            max_files: None,
            max_concurrent_downloads: 1,
            pair_filter: None,
            key_date_range: None,
        };

        let snapshots = temp_loader
            .parse_snapshot_data(&decompressed)
            .with_context(|| format!("Failed to parse file: {}", key))?;

        // Cache parsed snapshots
        if let Some(ref cache) = cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.put(cache_key, snapshots.clone());
        }

        Ok(snapshots)
    }
}

impl DataLoader for S3Loader {
    fn load_snapshots(&self) -> Result<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>> {
        let (sender, receiver) = mpsc::sync_channel(100000);

        let s3_client = Arc::clone(&self.s3_client);
        let bucket = self.bucket.clone();
        let prefix = self.prefix.clone();
        let max_files = self.max_files;
        let max_concurrent_downloads = self.max_concurrent_downloads;
        let pair_filter = self.pair_filter.clone();
        let key_date_range = self.key_date_range;

        // Create LRU cache for parsed snapshots
        let cache_size = NonZeroUsize::new(1000).unwrap();
        let cache: Arc<Mutex<LruCache<String, Vec<OrderBookSnapshot>>>> =
            Arc::new(Mutex::new(LruCache::new(cache_size)));

        // Create a single runtime for this thread
        thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("Failed to create tokio runtime: {}", e);
                    let _ = sender.send(Err(anyhow::anyhow!(
                        "Failed to create tokio runtime: {}",
                        e
                    )));
                    return;
                }
            };

            let result = rt.block_on(async {
                println!("Connecting to S3 to get file list...");
                let loader = S3Loader {
                    s3_client: Arc::clone(&s3_client),
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    region: String::new(),
                    max_files: max_files,
                    max_concurrent_downloads: max_concurrent_downloads,
                    pair_filter: pair_filter.clone(),
                    key_date_range,
                };

                let files_to_process = loader.list_gz_keys_limited(max_files).await?;

                if files_to_process.is_empty() {
                    return Err(anyhow::anyhow!("No .gz files found in S3"));
                }

                // Cap parallel S3 downloads (env), upper bound avoids huge connection bursts.
                let batch_size = max_concurrent_downloads.max(1).min(512);
                println!(
                    "Streaming {} files from S3 (parallel batches of {})...",
                    files_to_process.len(),
                    batch_size
                );

                let total = files_to_process.len();
                let progress = Arc::new(Mutex::new(0));
                let sender_clone = sender.clone();

                // Process files in parallel batches for faster loading
                for chunk_start in (0..total).step_by(batch_size) {
                    let chunk_end = (chunk_start + batch_size).min(total);
                    let keys: Vec<String> = files_to_process[chunk_start..chunk_end].to_vec();

                    // Download and parse all files in this batch in parallel
                    let tasks: Vec<_> = keys
                        .iter()
                        .map(|key| {
                            let s3 = Arc::clone(&s3_client);
                            let b = bucket.clone();
                            let k = key.clone();
                            let c = cache.clone();
                            tokio::spawn(Self::download_and_parse_worker_async(s3, b, k, Some(c)))
                        })
                        .collect();

                    let results = futures::future::join_all(tasks).await;

                    for (j, result) in results.into_iter().enumerate() {
                        let snapshots = match result {
                            Ok(Ok(s)) => s,
                            Ok(Err(e)) => {
                                eprintln!("Error processing file {}: {}", &keys[j], e);
                                continue;
                            }
                            Err(e) => {
                                eprintln!("Task error: {}", e);
                                continue;
                            }
                        };
                        for snapshot in snapshots {
                            if sender_clone.send(Ok(snapshot)).is_err() {
                                break;
                            }
                        }
                    }

                    let mut prog = progress.lock().unwrap();
                    *prog += chunk_end - chunk_start;
                    if *prog % 100 == 0 || *prog == total {
                        println!(
                            "Processed {}/{} files ({:.1}%)",
                            *prog,
                            total,
                            (*prog as f64 / total as f64) * 100.0
                        );
                    }
                }

                Ok::<(), anyhow::Error>(())
            });

            match result {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Streaming error: {}", e);
                    let _ = sender.send(Err(e));
                }
            }
        });

        Ok(Box::new(receiver.into_iter()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_date_from_double_year_layout() {
        let k = "x/2025/2025/01/15/cdc/ETH_USD/f.gz";
        assert_eq!(
            last_calendar_date_in_key(k),
            NaiveDate::from_ymd_opt(2025, 1, 15)
        );
    }

    #[test]
    fn last_date_simple_ymd() {
        let k = "prefix/2024/03/02/cdc/BTC_USDT/a.gz";
        assert_eq!(
            last_calendar_date_in_key(k),
            NaiveDate::from_ymd_opt(2024, 3, 2)
        );
    }
}
