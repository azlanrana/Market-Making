// SFTP to S3 uploader - streams files directly from Crypto.com SFTP to AWS S3

use crate::sftp_key::ensure_private_key_path;
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use crossbeam_channel::unbounded;
use ssh2::Session;
use std::collections::HashSet;
use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

#[derive(Default)]
struct UploadStats {
    total_files: AtomicU64,
    uploaded: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
    retried: AtomicU64,
    bytes_uploaded: AtomicU64,
}

pub async fn upload_sftp_to_s3(
    sftp_host: &str,
    sftp_username: &str,
    sftp_key_path: &str,
    sftp_remote_path: &str,
    s3_bucket: &str,
    s3_prefix: &str,
    s3_region: &str,
    start_date: &str,
    end_date: &str,
    max_concurrent: Option<usize>,
    max_s3_concurrent: Option<usize>,
    skip_s3_check: bool,
) -> Result<()> {
    println!("=== SFTP to S3 Direct Upload ===");
    println!("SFTP: {}@{}", sftp_username, sftp_host);
    println!("SFTP Path: {}", sftp_remote_path);
    println!("S3: s3://{}/{}", s3_bucket, s3_prefix);
    println!("Date Range: {} to {}", start_date, end_date);
    println!();

    // Initialize S3 client
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(s3_region.to_string()))
        .load()
        .await;
    let s3_client = Arc::new(S3Client::new(&config));

    // One persistent SFTP session per worker (default: 20). Avoids SSH handshake per file.
    let max_concurrent = max_concurrent.unwrap_or(20).max(1);
    let max_s3_concurrent = max_s3_concurrent.unwrap_or(64).max(1);
    println!(
        "Max concurrent SFTP workers (reuse session per worker): {}",
        max_concurrent
    );
    println!("Max concurrent S3 PutObject: {}", max_s3_concurrent);

    ensure_private_key_path(sftp_key_path)?;

    // Statistics tracking
    let stats = Arc::new(UploadStats::default());

    // Connect to SFTP
    println!("Connecting to SFTP...");
    let tcp = TcpStream::connect(format!("{}:22", sftp_host))
        .with_context(|| format!("Failed to connect to {}", sftp_host))?;

    let mut sess =
        Session::new().map_err(|e| anyhow::anyhow!("Failed to create SSH session: {:?}", e))?;

    sess.set_tcp_stream(tcp);
    sess.handshake().with_context(|| "SSH handshake failed")?;

    sess.userauth_pubkey_file(sftp_username, None, Path::new(sftp_key_path), None)
        .with_context(|| format!("Authentication failed for user {}", sftp_username))?;

    if !sess.authenticated() {
        return Err(anyhow::anyhow!("Authentication failed"));
    }

    let sftp = sess
        .sftp()
        .with_context(|| "Failed to create SFTP session")?;

    // Parse date range
    let start = parse_date(start_date)?;
    let end = parse_date(end_date)?;

    // Build base path - ensure leading slash for absolute SFTP path
    let path_parts: Vec<&str> = sftp_remote_path
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    let day_idx = path_parts.len().saturating_sub(3);
    let base_path = if day_idx >= 2 {
        let joined = path_parts[..day_idx - 2].join("/");
        if joined.starts_with('/') {
            joined
        } else {
            format!("/{}", joined)
        }
    } else {
        return Err(anyhow::anyhow!("Invalid remote path format"));
    };

    let cdc_part = path_parts.get(day_idx + 1).unwrap_or(&"cdc");
    let pair_part = path_parts.last().unwrap();

    // Collect all files first
    let mut all_files = Vec::new();
    let mut current_date = start;

    println!("Scanning files...");
    while current_date <= end {
        let (year, month, day) = date_to_components(current_date);
        // Crypto.com SFTP uses unpadded month/day: /exchange/.../2025/1/1/cdc/PAIR
        let day_path = format!(
            "{}/{}/{}/{}/{}/{}",
            base_path, year, month, day, cdc_part, pair_part
        );

        println!("  Checking: {}", day_path);
        match sftp.readdir(Path::new(&day_path)) {
            Ok(files) => {
                let mut total_files_in_dir = 0;
                let mut gz_files_in_dir = 0;
                let mut non_gz_files = Vec::new();
                for (path, _) in &files {
                    total_files_in_dir += 1;
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        if name.ends_with(".gz") {
                            gz_files_in_dir += 1;
                            let remote_file_path = format!("{}/{}", day_path, name);
                            let s3_key = format!(
                                "{}{:04}/{:02}/{:02}/{}/{}/{}",
                                s3_prefix, year, month, day, cdc_part, pair_part, name
                            );
                            all_files.push((remote_file_path, s3_key, (year, month, day)));
                        } else {
                            non_gz_files.push(name.to_string());
                        }
                    }
                }
                if total_files_in_dir == 0 {
                    println!("    Directory exists but is empty");
                } else if gz_files_in_dir == 0 {
                    println!(
                        "    Found {} files but none are .gz files",
                        total_files_in_dir
                    );
                    if !non_gz_files.is_empty() {
                        let sample: Vec<String> = non_gz_files.iter().take(5).cloned().collect();
                        println!("    Sample files: {}", sample.join(", "));
                    }
                } else {
                    println!(
                        "    Found {} .gz files ({} total files)",
                        gz_files_in_dir, total_files_in_dir
                    );
                }
            }
            Err(e) => {
                println!("    Error: Directory not found or inaccessible: {}", e);
            }
        }
        current_date = next_day(current_date);
    }

    all_files.sort();
    stats
        .total_files
        .store(all_files.len() as u64, Ordering::Relaxed);

    println!("Found {} files to process", all_files.len());

    let files_to_upload = if skip_s3_check {
        println!("Skipping S3 check (assuming all files need to be uploaded)...");
        all_files
            .iter()
            .map(|(r, s, d)| (r.clone(), s.clone(), *d))
            .collect()
    } else {
        println!("Checking existing files in S3...");
        let existing_keys = list_existing_s3_keys(&s3_client, s3_bucket, s3_prefix).await?;
        println!(
            "  Found {} existing keys under {}",
            existing_keys.len(),
            s3_prefix
        );

        let mut files_to_upload = Vec::new();
        let mut existing_count = 0;
        let total_to_check = all_files.len();
        let check_start = Instant::now();

        for (idx, (remote_path, s3_key, date)) in all_files.iter().enumerate() {
            let checked_count = idx + 1;

            // Show progress every 5000 files plus a final checkpoint
            if checked_count % 5000 == 0
                || (checked_count == total_to_check && check_start.elapsed().as_secs() >= 1)
            {
                let percent = (checked_count as f64 / total_to_check as f64) * 100.0;
                println!(
                    "  Checked {}/{} files ({:.1}%) - Found {} existing, {} to upload",
                    checked_count,
                    total_to_check,
                    percent,
                    existing_count,
                    files_to_upload.len()
                );
            }

            if existing_keys.contains(s3_key) {
                existing_count += 1;
                stats.skipped.fetch_add(1, Ordering::Relaxed);
            } else {
                files_to_upload.push((remote_path.clone(), s3_key.clone(), *date));
            }
        }

        files_to_upload
    };

    let existing_count = all_files.len() - files_to_upload.len();

    println!("  {} files already exist in S3 (skipped)", existing_count);
    println!("  {} files need to be uploaded", files_to_upload.len());
    println!();

    if files_to_upload.is_empty() {
        println!("All files already uploaded!");
        return Ok(());
    }

    // Upload files with progress tracking
    let start_time = Arc::new(Instant::now());

    // Start background progress reporter
    let stats_for_progress = stats.clone();
    let start_time_for_progress = start_time.clone();
    let progress_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            let uploaded = stats_for_progress.uploaded.load(Ordering::Relaxed);
            let skipped = stats_for_progress.skipped.load(Ordering::Relaxed);
            let failed = stats_for_progress.failed.load(Ordering::Relaxed);
            let total = stats_for_progress.total_files.load(Ordering::Relaxed);
            let processed = uploaded + skipped + failed;

            if total > 0 && processed < total {
                let percent = (processed as f64 / total as f64) * 100.0;
                let elapsed = start_time_for_progress.as_ref().elapsed();
                let rate = if elapsed.as_secs() > 0 {
                    uploaded as f64 / elapsed.as_secs() as f64
                } else {
                    0.0
                };
                let remaining = total - processed;
                let eta_secs = if rate > 0.0 {
                    (remaining as f64 / rate) as u64
                } else {
                    0
                };

                println!("  Progress: {:.1}% | Uploaded: {} | Skipped: {} | Failed: {} | Rate: {:.1} files/s | ETA: {}s", 
                    percent, uploaded, skipped, failed, rate, eta_secs);
            } else if processed >= total {
                break;
            }
        }
    });

    // Bounded result queue: SFTP workers block if S3 / main task falls behind (backpressure).
    let result_buf = (max_concurrent.max(max_s3_concurrent) * 4).max(32);
    let (result_tx, mut result_rx) = tokio::sync::mpsc::channel::<SftpDownloadMsg>(result_buf);
    let (job_tx, job_rx) = unbounded::<(String, String)>();

    let host_owned = sftp_host.to_string();
    let user_owned = sftp_username.to_string();
    let key_owned = sftp_key_path.to_string();

    for _ in 0..max_concurrent {
        let jr = job_rx.clone();
        let rt = result_tx.clone();
        let h = host_owned.clone();
        let u = user_owned.clone();
        let k = key_owned.clone();
        std::thread::spawn(move || sftp_download_worker(h, u, k, jr, rt));
    }
    drop(job_rx);

    for (remote_path, s3_key, _) in files_to_upload {
        job_tx
            .send((remote_path, s3_key))
            .map_err(|_| anyhow::anyhow!("SFTP worker panicked or disconnected"))?;
    }
    drop(job_tx);
    drop(result_tx);

    let bucket_owned = s3_bucket.to_string();
    let mut join_set: JoinSet<(String, anyhow::Result<()>)> = JoinSet::new();

    while let Some(msg) = result_rx.recv().await {
        while join_set.len() >= max_s3_concurrent {
            match join_set.join_next().await {
                Some(Ok((_, Ok(())))) => {}
                Some(Ok((s3_key, Err(e)))) => {
                    eprintln!("  S3 upload failed: {} -> {}", s3_key, e);
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                }
                Some(Err(e)) => {
                    eprintln!("  S3 upload task join error: {}", e);
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                }
                None => break,
            }
        }

        match msg {
            SftpDownloadMsg::Ok { s3_key, data } => {
                let client = s3_client.clone();
                let bucket = bucket_owned.clone();
                let st = stats.clone();
                join_set.spawn(async move {
                    let res = put_s3_with_retry(&client, &bucket, &s3_key, data, &st).await;
                    (s3_key, res)
                });
            }
            SftpDownloadMsg::Err { remote_path, error } => {
                eprintln!("  SFTP read failed: {} -> {}", remote_path, error);
                stats.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    while let Some(join_res) = join_set.join_next().await {
        match join_res {
            Ok((_, Ok(()))) => {}
            Ok((s3_key, Err(e))) => {
                eprintln!("  S3 upload failed: {} -> {}", s3_key, e);
                stats.failed.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("  S3 upload task join error: {}", e);
                stats.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Stop progress reporter
    progress_handle.abort();

    let elapsed = start_time.as_ref().elapsed();
    let uploaded = stats.uploaded.load(Ordering::Relaxed);
    let skipped = stats.skipped.load(Ordering::Relaxed);
    let failed = stats.failed.load(Ordering::Relaxed);
    let retried = stats.retried.load(Ordering::Relaxed);
    let bytes_uploaded = stats.bytes_uploaded.load(Ordering::Relaxed);

    println!();
    println!("=== Upload Complete ===");
    println!(
        "Total files found: {}",
        stats.total_files.load(Ordering::Relaxed)
    );
    println!("Files uploaded: {}", uploaded);
    println!("Files skipped (already exist): {}", skipped);
    println!("Files failed: {}", failed);
    println!("Retries: {}", retried);
    println!("Bytes uploaded: {} MB", bytes_uploaded / 1_000_000);
    println!("Time elapsed: {:.2}s", elapsed.as_secs_f64());
    if elapsed.as_secs() > 0 {
        println!(
            "Average rate: {:.1} files/s",
            uploaded as f64 / elapsed.as_secs() as f64
        );
    }
    println!("S3 location: s3://{}/{}", s3_bucket, s3_prefix);

    if failed > 0 {
        return Err(anyhow::anyhow!("{} files failed to upload", failed));
    }

    Ok(())
}

async fn list_existing_s3_keys(
    s3_client: &S3Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashSet<String>> {
    let mut keys = HashSet::new();
    let mut continuation_token: Option<String> = None;
    let mut pages = 0usize;

    loop {
        let mut request = s3_client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .max_keys(1000);

        if let Some(token) = continuation_token.as_deref() {
            request = request.continuation_token(token);
        }

        let response = request.send().await?;
        pages += 1;

        if let Some(objects) = response.contents {
            for object in objects {
                if let Some(key) = object.key {
                    keys.insert(key);
                }
            }
        }

        if pages % 50 == 0 {
            println!(
                "  Listed {} S3 pages under {} ({} keys so far)",
                pages,
                prefix,
                keys.len()
            );
        }

        if response.is_truncated.unwrap_or(false) {
            continuation_token = response.next_continuation_token;
        } else {
            break;
        }
    }

    Ok(keys)
}

enum SftpDownloadMsg {
    Ok { s3_key: String, data: Vec<u8> },
    Err { remote_path: String, error: String },
}

fn connect_sftp_session(host: &str, username: &str, key_path: &str) -> Result<Session> {
    let tcp = TcpStream::connect(format!("{}:22", host))
        .with_context(|| format!("Failed to connect to {}", host))?;
    let mut sess = Session::new().map_err(|e| anyhow::anyhow!("SSH session: {:?}", e))?;
    sess.set_tcp_stream(tcp);
    sess.handshake().context("SSH handshake failed")?;
    sess.userauth_pubkey_file(username, None, Path::new(key_path), None)
        .with_context(|| format!("Authentication failed for user {}", username))?;
    if !sess.authenticated() {
        return Err(anyhow::anyhow!("Authentication failed"));
    }
    Ok(sess)
}

fn read_remote_file(sess: &mut Session, remote_path: &str) -> Result<Vec<u8>> {
    let sftp = sess.sftp().context("SFTP subsystem")?;
    let mut remote_file = sftp
        .open(Path::new(remote_path))
        .with_context(|| format!("Open {}", remote_path))?;
    let mut data = Vec::new();
    remote_file
        .read_to_end(&mut data)
        .with_context(|| format!("Read {}", remote_path))?;
    Ok(data)
}

fn sftp_download_worker(
    host: String,
    username: String,
    key_path: String,
    jobs: crossbeam_channel::Receiver<(String, String)>,
    results: tokio::sync::mpsc::Sender<SftpDownloadMsg>,
) {
    const READ_RETRIES: u32 = 3;
    let mut sess: Option<Session> = None;

    while let Ok((remote_path, s3_key)) = jobs.recv() {
        let mut last_err: Option<anyhow::Error> = None;
        let mut data: Option<Vec<u8>> = None;

        for attempt in 0..READ_RETRIES {
            if sess.is_none() {
                match connect_sftp_session(&host, &username, &key_path) {
                    Ok(s) => sess = Some(s),
                    Err(e) => {
                        last_err = Some(e);
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                }
            }

            let read_result = read_remote_file(sess.as_mut().unwrap(), &remote_path);
            match read_result {
                Ok(bytes) => {
                    data = Some(bytes);
                    break;
                }
                Err(e) => {
                    last_err = Some(e);
                    sess = None;
                    if attempt + 1 < READ_RETRIES {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        }

        match data {
            Some(bytes) => {
                let _ = results.blocking_send(SftpDownloadMsg::Ok {
                    s3_key,
                    data: bytes,
                });
            }
            None => {
                let err = last_err
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "SFTP read failed".to_string());
                let _ = results.blocking_send(SftpDownloadMsg::Err {
                    remote_path,
                    error: err,
                });
            }
        }
    }
}

async fn put_s3_with_retry(
    s3_client: &S3Client,
    bucket: &str,
    s3_key: &str,
    data: Vec<u8>,
    stats: &UploadStats,
) -> Result<()> {
    const MAX_RETRIES: u32 = 3;
    let byte_len = data.len() as u64;
    let body_bytes = Bytes::from(data);

    for attempt in 0..MAX_RETRIES {
        let body = ByteStream::from(body_bytes.clone());
        match s3_client
            .put_object()
            .bucket(bucket)
            .key(s3_key)
            .body(body)
            .send()
            .await
        {
            Ok(_) => {
                stats.uploaded.fetch_add(1, Ordering::Relaxed);
                stats.bytes_uploaded.fetch_add(byte_len, Ordering::Relaxed);
                return Ok(());
            }
            Err(e) => {
                if attempt + 1 >= MAX_RETRIES {
                    return Err(anyhow::anyhow!("{:?}", e));
                }
                stats.retried.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Err(anyhow::anyhow!("S3 put_object exhausted retries"))
}

fn parse_date(date_str: &str) -> Result<(i32, u32, u32)> {
    let parts: Vec<&str> = date_str.split('-').collect();
    if parts.len() != 3 {
        return Err(anyhow::anyhow!("Invalid date format: {}", date_str));
    }

    let year: i32 = parts[0]
        .parse()
        .with_context(|| format!("Invalid year: {}", parts[0]))?;
    let month: u32 = parts[1]
        .parse()
        .with_context(|| format!("Invalid month: {}", parts[1]))?;
    let day: u32 = parts[2]
        .parse()
        .with_context(|| format!("Invalid day: {}", parts[2]))?;

    Ok((year, month, day))
}

fn date_to_components(date: (i32, u32, u32)) -> (i32, u32, u32) {
    date
}

fn next_day(date: (i32, u32, u32)) -> (i32, u32, u32) {
    let (mut y, mut m, mut d) = date;
    d += 1;

    let days_in_month = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 31,
    };

    if d > days_in_month {
        d = 1;
        m += 1;
        if m > 12 {
            m = 1;
            y += 1;
        }
    }

    (y, m, d)
}
