use crate::loader::DataLoader;
use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use lru::LruCache;
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use serde::Deserialize;
use ssh2::Session;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::TcpStream;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

#[derive(Deserialize)]
pub struct SnapshotRow {
    pub t: Option<i64>,
    pub timestamp: Option<i64>,
    pub ts: Option<i64>,
    pub time: Option<i64>,

    #[serde(default, alias = "bids", alias = "bid_levels")]
    pub b: Option<Vec<Vec<f64>>>,

    #[serde(default, alias = "asks", alias = "ask_levels")]
    pub a: Option<Vec<Vec<f64>>>,
}

/// SFTP loader for Crypto.com orderbook snapshot history
///
/// Connects to Crypto.com SFTP server and downloads orderbook snapshots
/// Folder structure: exchange/book_l2_150_0010/yyyy/mm/dd/xxx.gz
pub struct SftpLoader {
    host: String,
    username: String,
    private_key_path: String,
    remote_path: String,
    #[allow(dead_code)]
    local_cache_dir: Option<String>,
    max_files: Option<usize>,
    max_concurrent_downloads: usize,
}

impl SftpLoader {
    /// Create a new SFTP loader
    ///
    /// # Arguments
    /// * `username` - SFTP username assigned by Crypto.com
    /// * `private_key_path` - Path to private key file (e.g., ~/.ssh/crypto_com_key)
    /// * `remote_path` - Remote path to orderbook files (e.g., "exchange/book_l2_150_0010/2023/10/25/cdc/BTC_USDT")
    /// * `local_cache_dir` - Optional local directory to cache downloaded files
    pub fn new(
        username: String,
        private_key_path: String,
        remote_path: String,
        local_cache_dir: Option<String>,
    ) -> Self {
        Self {
            host: "data.crypto.com".to_string(),
            username,
            private_key_path,
            remote_path,
            local_cache_dir,
            max_files: None,
            max_concurrent_downloads: 50, // Default: 50 parallel downloads for better throughput
        }
    }

    /// Set maximum number of files to download (for faster testing)
    pub fn with_max_files(mut self, max_files: usize) -> Self {
        self.max_files = Some(max_files);
        self
    }

    /// Set maximum number of concurrent downloads
    pub fn with_max_concurrent_downloads(mut self, max_concurrent: usize) -> Self {
        self.max_concurrent_downloads = max_concurrent;
        self
    }

    /// Connect to SFTP server and authenticate
    fn connect(&self) -> Result<Session> {
        let tcp = TcpStream::connect(format!("{}:22", self.host))
            .with_context(|| format!("Failed to connect to {}", self.host))?;

        let mut sess =
            Session::new().map_err(|e| anyhow::anyhow!("Failed to create SSH session: {:?}", e))?;

        sess.set_tcp_stream(tcp);
        sess.handshake().with_context(|| "SSH handshake failed")?;

        // Authenticate using private key
        sess.userauth_pubkey_file(
            &self.username,
            None,
            Path::new(&self.private_key_path),
            None,
        )
        .with_context(|| format!("Authentication failed for user {}", self.username))?;

        if !sess.authenticated() {
            return Err(anyhow::anyhow!("Authentication failed"));
        }

        Ok(sess)
    }

    /// List files in remote directory
    pub fn list_files(&self) -> Result<Vec<String>> {
        let sess = self.connect()?;
        let sftp = sess
            .sftp()
            .with_context(|| "Failed to create SFTP session")?;

        // Check if we're at the month level (need to traverse day directories) or day level (direct files)
        // Path format: exchange/book_l2_150_0010/2023/10/25/cdc/BTC_USDT
        // If path ends with a number (day), list files directly
        // If path ends with a number but we want multiple days, traverse up to month level

        let path_parts: Vec<&str> = self.remote_path.split('/').collect();
        let mut all_files = Vec::new();

        // Check if remote_path points to a specific day directory
        // Format: exchange/book_l2_150_0010/YYYY/MM/DD/cdc/PAIR
        // Example: exchange/book_l2_150_0010/2023/10/25/cdc/BTC_USDT
        if path_parts.len() >= 6 {
            // Find the day component (should be numeric, 4th from end: .../YYYY/MM/DD/cdc/PAIR)
            let day_idx = path_parts.len() - 3; // Index of day (before cdc and pair)
            if day_idx >= 3 && day_idx < path_parts.len() {
                let year_str = path_parts[day_idx - 2];
                let month_str = path_parts[day_idx - 1];
                let day_str = &path_parts[day_idx];

                // Parse year, month, day
                if let (Ok(year), Ok(month), Ok(start_day)) = (
                    year_str.parse::<i32>(),
                    month_str.parse::<u32>(),
                    day_str.parse::<u32>(),
                ) {
                    // We're at day level. For 1 week, traverse 7 days starting from this day
                    // Build base path up to (but not including) year level: exchange/book_l2_150_0010
                    let base_path = path_parts[..day_idx - 2].join("/");
                    let cdc_part = path_parts.get(day_idx + 1).unwrap_or(&"cdc");
                    let pair_part = path_parts.last().unwrap();

                    println!(
                        "Starting multi-day traversal from {}-{:02}-{:02}",
                        year, month, start_day
                    );
                    println!("Will traverse 7 days forward...");
                    println!("Base path: {}", base_path);

                    // Traverse 7 days (1 week) with proper date arithmetic
                    let mut files_found_by_day = std::collections::HashMap::new();

                    for day_offset in 0..7 {
                        // Calculate the target date
                        let (target_year, target_month, target_day) = {
                            let mut y = year;
                            let mut m = month;
                            let mut d = start_day + day_offset;

                            // Handle month boundaries
                            let days_in_month = match m {
                                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                                4 | 6 | 9 | 11 => 30,
                                2 => {
                                    // February - check for leap year
                                    if (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0) {
                                        29
                                    } else {
                                        28
                                    }
                                }
                                _ => 31,
                            };

                            if d > days_in_month {
                                d -= days_in_month;
                                m += 1;
                                if m > 12 {
                                    m = 1;
                                    y += 1;
                                }
                            }

                            (y, m, d)
                        };

                        // Build the day path: exchange/book_l2_150_0010/YYYY/MM/DD/cdc/PAIR
                        let day_path = format!(
                            "{}/{:04}/{:02}/{:02}/{}/{}",
                            base_path, target_year, target_month, target_day, cdc_part, pair_part
                        );

                        match sftp.readdir(Path::new(&day_path)) {
                            Ok(files) => {
                                let mut day_files = Vec::new();
                                for (path, _) in files {
                                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                        if name.ends_with(".gz") {
                                            // Store with full date path prefix: "{year}/{month}/{day}/{cdc}/{pair}/{filename}"
                                            let file_path = format!(
                                                "{:04}/{:02}/{:02}/{}/{}/{}",
                                                target_year,
                                                target_month,
                                                target_day,
                                                cdc_part,
                                                pair_part,
                                                name
                                            );
                                            day_files.push(file_path.clone());
                                            all_files.push(file_path);
                                        }
                                    }
                                }

                                if !day_files.is_empty() {
                                    files_found_by_day.insert(
                                        format!(
                                            "{:04}-{:02}-{:02}",
                                            target_year, target_month, target_day
                                        ),
                                        day_files.len(),
                                    );
                                } else {
                                    println!(
                                        "  {:04}-{:02}-{:02}: No files found",
                                        target_year, target_month, target_day
                                    );
                                }
                            }
                            Err(e) => {
                                // Day directory doesn't exist or error reading
                                println!(
                                    "  {:04}-{:02}-{:02}: Directory not found or error: {}",
                                    target_year, target_month, target_day, e
                                );
                                continue;
                            }
                        }
                    }

                    // Print summary
                    println!("\nFiles found by day:");
                    let mut sorted_days: Vec<_> = files_found_by_day.iter().collect();
                    sorted_days.sort();
                    for (day, count) in sorted_days {
                        println!("  {}: {} files", day, count);
                    }
                    println!("Total files across all days: {}\n", all_files.len());

                    if !all_files.is_empty() {
                        // Sort files by timestamp (extracted from filename)
                        // Filenames are Unix timestamps in milliseconds, e.g., "1698192645124.gz"
                        all_files.sort_by(|a, b| {
                            let a_ts = a
                                .split('/')
                                .last()
                                .and_then(|f| f.strip_suffix(".gz"))
                                .and_then(|f| f.parse::<u64>().ok())
                                .unwrap_or(0);
                            let b_ts = b
                                .split('/')
                                .last()
                                .and_then(|f| f.strip_suffix(".gz"))
                                .and_then(|f| f.parse::<u64>().ok())
                                .unwrap_or(0);
                            a_ts.cmp(&b_ts)
                        });

                        return Ok(all_files);
                    }
                }
            }
        }

        // Fallback: list files from current directory (single day)
        let files = sftp
            .readdir(Path::new(&self.remote_path))
            .with_context(|| format!("Failed to read directory: {}", self.remote_path))?;

        Ok(files
            .iter()
            .filter_map(|(path, _)| {
                path.file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.to_string())
            })
            .collect())
    }

    /// Download a file from SFTP server
    #[allow(dead_code)]
    fn download_file(&self, remote_filename: &str, local_path: &Path) -> Result<()> {
        let sess = self.connect()?;
        let sftp = sess
            .sftp()
            .with_context(|| "Failed to create SFTP session")?;

        // Check if remote_filename already contains a path (from multi-day listing)
        // Format: "{year}/{month}/{day}/{cdc}/{pair}/{filename}" or just "{filename}"
        let remote_file_path = if remote_filename.contains('/') {
            // File already has full date path, construct from base path
            let path_parts: Vec<&str> = self.remote_path.split('/').collect();
            if path_parts.len() >= 6 {
                // Find the day component to determine base path
                // Format: exchange/book_l2_150_0010/YYYY/MM/DD/cdc/PAIR
                let day_idx = path_parts.len() - 3; // Index of day
                if day_idx >= 2 {
                    // Build base path up to (but not including) year: exchange/book_l2_150_0010
                    let base_path = path_parts[..day_idx - 2].join("/");
                    // Append the full date path from remote_filename: YYYY/MM/DD/cdc/PAIR/filename
                    format!("{}/{}", base_path, remote_filename)
                } else {
                    // Fallback: use as-is
                    remote_filename.to_string()
                }
            } else {
                // Fallback: use as-is
                remote_filename.to_string()
            }
        } else {
            // Single day: use original remote_path
            format!("{}/{}", self.remote_path, remote_filename)
        };

        let mut remote_file = sftp
            .open(Path::new(&remote_file_path))
            .with_context(|| format!("Failed to open remote file: {}", remote_file_path))?;

        let mut local_file = std::fs::File::create(local_path)
            .with_context(|| format!("Failed to create local file: {}", local_path.display()))?;

        std::io::copy(&mut remote_file, &mut local_file).with_context(|| "Failed to copy file")?;

        Ok(())
    }

    /// Download and decompress a .gz file
    #[allow(dead_code)]
    fn download_and_decompress(&self, remote_filename: &str) -> Result<Vec<u8>> {
        // Extract just the filename for caching (remove day directory prefix if present)
        // remote_filename might be "25/cdc/BTC_USDT/1698278400008.gz" or just "1698278400008.gz"
        let cache_filename = remote_filename
            .split('/')
            .last()
            .unwrap_or(remote_filename)
            .strip_suffix(".gz")
            .unwrap_or(remote_filename);

        // Cache decompressed data, not compressed
        let decompressed_cache_path = if let Some(ref cache_dir) = self.local_cache_dir {
            // Create cache directory if it doesn't exist
            std::fs::create_dir_all(cache_dir)
                .with_context(|| format!("Failed to create cache directory: {}", cache_dir))?;

            Some(Path::new(cache_dir).join(cache_filename))
        } else {
            None
        };

        // Check if decompressed cache exists
        if let Some(ref cache_path) = decompressed_cache_path {
            if cache_path.exists() {
                return Ok(std::fs::read(cache_path).with_context(|| {
                    format!("Failed to read cached file: {}", cache_path.display())
                })?);
            }
        }

        // Download compressed file to temp location
        let temp_compressed = std::env::temp_dir().join(remote_filename);
        self.download_file(remote_filename, &temp_compressed)?;

        // Decompress if it's a .gz file
        let data = if remote_filename.ends_with(".gz") {
            let file = std::fs::File::open(&temp_compressed).with_context(|| {
                format!(
                    "Failed to open downloaded file: {}",
                    temp_compressed.display()
                )
            })?;
            let mut decoder = GzDecoder::new(file);
            let mut decompressed = Vec::new();
            decoder
                .read_to_end(&mut decompressed)
                .with_context(|| "Failed to decompress file")?;

            // Cache decompressed data
            if let Some(ref cache_path) = decompressed_cache_path {
                std::fs::write(cache_path, &decompressed).with_context(|| {
                    format!("Failed to write cache file: {}", cache_path.display())
                })?;
            }

            decompressed
        } else {
            let data = std::fs::read(&temp_compressed)
                .with_context(|| format!("Failed to read file: {}", temp_compressed.display()))?;

            // Cache if needed
            if let Some(ref cache_path) = decompressed_cache_path {
                std::fs::write(cache_path, &data).with_context(|| {
                    format!("Failed to write cache file: {}", cache_path.display())
                })?;
            }

            data
        };

        Ok(data)
    }

    /// Stream download and decompress a .gz file directly (no temp files)
    /// Returns decompressed data as Vec<u8>
    fn stream_download_and_decompress(sess: &Session, remote_file_path: &str) -> Result<Vec<u8>> {
        let sftp = sess
            .sftp()
            .with_context(|| "Failed to create SFTP session")?;

        let mut remote_file = sftp
            .open(Path::new(remote_file_path))
            .with_context(|| format!("Failed to open remote file: {}", remote_file_path))?;

        // Stream decompress directly from SFTP without temp files
        if remote_file_path.ends_with(".gz") {
            let decoder = GzDecoder::new(&mut remote_file);
            let mut decompressed = Vec::new();
            BufReader::new(decoder)
                .read_to_end(&mut decompressed)
                .with_context(|| "Failed to decompress file")?;
            Ok(decompressed)
        } else {
            let mut data = Vec::new();
            remote_file
                .read_to_end(&mut data)
                .with_context(|| "Failed to read file")?;
            Ok(data)
        }
    }

    /// Download and parse snapshots with caching (thread-safe worker function)
    /// Uses connection reuse and streaming decompression
    fn download_and_parse_worker(
        host: &str,
        username: &str,
        private_key_path: &str,
        remote_path: &str,
        remote_filename: &str,
        cache: Option<Arc<Mutex<LruCache<String, Vec<OrderBookSnapshot>>>>>,
    ) -> Result<Vec<OrderBookSnapshot>> {
        // Extract cache key (filename without path)
        let cache_key = remote_filename
            .split('/')
            .last()
            .unwrap_or(remote_filename)
            .strip_suffix(".gz")
            .unwrap_or(remote_filename)
            .to_string();

        // Check cache first
        if let Some(ref cache) = cache {
            let mut cache_guard = cache.lock().unwrap();
            if let Some(cached_snapshots) = cache_guard.get(&cache_key) {
                return Ok(cached_snapshots.clone());
            }
        }

        // Build full remote path
        let path_parts: Vec<&str> = remote_path.split('/').collect();
        let remote_file_path = if remote_filename.contains('/') {
            let day_idx = path_parts.len() - 3;
            if day_idx >= 2 {
                let base_path = path_parts[..day_idx - 2].join("/");
                format!("{}/{}", base_path, remote_filename)
            } else {
                format!("{}/{}", remote_path, remote_filename)
            }
        } else {
            format!("{}/{}", remote_path, remote_filename)
        };

        // Create connection for this worker (reused within worker)
        let tcp = TcpStream::connect(format!("{}:22", host))
            .with_context(|| format!("Failed to connect to {}", host))?;

        let mut sess =
            Session::new().map_err(|e| anyhow::anyhow!("Failed to create SSH session: {:?}", e))?;

        sess.set_tcp_stream(tcp);
        sess.handshake().with_context(|| "SSH handshake failed")?;

        sess.userauth_pubkey_file(username, None, Path::new(private_key_path), None)
            .with_context(|| format!("Authentication failed for user {}", username))?;

        if !sess.authenticated() {
            return Err(anyhow::anyhow!("Authentication failed"));
        }

        // Stream download and decompress
        let data = Self::stream_download_and_decompress(&sess, &remote_file_path)?;

        // Parse snapshots
        let temp_loader = SftpLoader {
            host: host.to_string(),
            username: username.to_string(),
            private_key_path: private_key_path.to_string(),
            remote_path: remote_path.to_string(),
            local_cache_dir: None,
            max_files: None,
            max_concurrent_downloads: 1,
        };

        let snapshots = temp_loader
            .parse_snapshot_data(&data)
            .with_context(|| format!("Failed to parse file: {}", remote_filename))?;

        // Cache parsed snapshots
        if let Some(ref cache) = cache {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.put(cache_key, snapshots.clone());
        }

        Ok(snapshots)
    }

    fn parse_snapshot_data(&self, data: &[u8]) -> Result<Vec<OrderBookSnapshot>> {
        let mut snapshots = Vec::new();

        // Try to detect format by checking first few bytes
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

                // Try to parse as JSON first using the optimized struct
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

                    // Calculate best bid (highest price) and best ask (lowest price)
                    let best_bid = bids
                        .iter()
                        .filter_map(|level| level.get(0).copied())
                        .fold(f64::NEG_INFINITY, f64::max);

                    let best_ask = asks
                        .iter()
                        .filter_map(|level| level.get(0).copied())
                        .fold(f64::INFINITY, f64::min);

                    if best_bid == f64::NEG_INFINITY || best_ask == f64::INFINITY {
                        continue;
                    }

                    let mid_price = (best_bid + best_ask) / 2.0;

                    // Use direct constructor to avoid JSON serialization overhead
                    // Convert slices to owned vectors
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
                    } else {
                        // Log first few lines for debugging
                        if line_num < 3 {
                            eprintln!(
                                "Skipping line {} (doesn't match expected format): {}",
                                line_num + 1,
                                if line.len() > 200 {
                                    format!("{}...", &line[..200])
                                } else {
                                    line
                                }
                            );
                        }
                    }
                }
            }
        } else {
            // Try to parse as binary format - might be protobuf or custom binary
            // For now, return error with helpful message
            let format_hint = if data.len() > 0 {
                format!("First bytes: {:?}", &data[..data.len().min(50)])
            } else {
                "Empty file".to_string()
            };
            return Err(anyhow::anyhow!(
                "File appears to be binary format (not text JSON/CSV). \
                Crypto.com may use protobuf format. \
                {} \
                Please check the file format documentation or contact Crypto.com support.",
                format_hint
            ));
        }

        Ok(snapshots)
    }
}

// ...

impl DataLoader for SftpLoader {
    fn load_snapshots(&self) -> Result<Box<dyn Iterator<Item = Result<OrderBookSnapshot>> + Send>> {
        let (sender, receiver) = mpsc::sync_channel(100000); // Large buffer for streaming

        // Clone config for the worker thread
        let host = self.host.clone();
        let username = self.username.clone();
        let private_key_path = self.private_key_path.clone();
        let remote_path = self.remote_path.clone();
        let max_files = self.max_files;
        let max_concurrent_downloads = self.max_concurrent_downloads;

        // Create LRU cache for parsed snapshots (limit to 1000 files to prevent memory bloat)
        let cache_size = NonZeroUsize::new(1000).unwrap();
        let cache: Arc<Mutex<LruCache<String, Vec<OrderBookSnapshot>>>> =
            Arc::new(Mutex::new(LruCache::new(cache_size)));

        // Spawn a thread to drive the streaming download process
        thread::spawn(move || {
            let result = (|| -> Result<()> {
                // Create a temporary loader instance to use helper methods
                let loader = SftpLoader {
                    host: host.clone(),
                    username: username.clone(),
                    private_key_path: private_key_path.clone(),
                    remote_path: remote_path.clone(),
                    local_cache_dir: None,
                    max_files: max_files,
                    max_concurrent_downloads: max_concurrent_downloads,
                };

                // Get full file list from SFTP
                println!("Connecting to SFTP to get file list...");
                let mut files = loader.list_files()?;

                files.retain(|f| f.ends_with(".gz"));

                if files.is_empty() {
                    return Err(anyhow::anyhow!("No .gz files found"));
                }

                files.sort();

                // Limit files
                let files_to_process: Vec<String> = if let Some(max) = max_files {
                    files.into_iter().take(max).collect()
                } else {
                    files
                };

                println!(
                    "Streaming {} files (sequential for chronological order)...",
                    files_to_process.len()
                );

                let total = files_to_process.len();
                let progress = Arc::new(Mutex::new(0));
                let sender_clone = sender.clone();

                // Process files sequentially to preserve chronological snapshot order
                // (required for correct backtest - parallel processing caused look-ahead bias)
                for filename in files_to_process.iter() {
                    match Self::download_and_parse_worker(
                        &host,
                        &username,
                        &private_key_path,
                        &remote_path,
                        filename,
                        Some(cache.clone()),
                    ) {
                        Ok(snapshots) => {
                            for snapshot in snapshots {
                                if sender_clone.send(Ok(snapshot)).is_err() {
                                    break; // Receiver dropped
                                }
                            }

                            let mut prog = progress.lock().unwrap();
                            *prog += 1;
                            if *prog % 100 == 0 || *prog == total {
                                println!(
                                    "Processed {}/{} files ({:.1}%)",
                                    *prog,
                                    total,
                                    (*prog as f64 / total as f64) * 100.0
                                );
                            }
                        }
                        Err(e) => {
                            eprintln!("Error processing file {}: {}", filename, e);
                        }
                    }
                }

                Ok(())
            })();

            if let Err(e) = result {
                eprintln!("Streaming error: {}", e);
                let _ = sender.send(Err(e));
            }
        });

        Ok(Box::new(receiver.into_iter()))
    }
}
