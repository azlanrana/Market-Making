// Check available dates on Crypto.com SFTP server

use crate::sftp_key::ensure_private_key_path;
use anyhow::{Context, Result};
use ssh2::Session;
use std::net::TcpStream;
use std::path::Path;

/// Ensure base path is absolute (SFTP needs /exchange/... not exchange/...)
fn normalize_base_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path.trim_start_matches('/'))
    }
}

pub fn check_available_dates(
    sftp_host: &str,
    sftp_username: &str,
    sftp_key_path: &str,
    base_path: &str,
    trading_pair: &str,
    year_filter: Option<&str>,
) -> Result<()> {
    let base_path = normalize_base_path(base_path);
    println!("=== Checking Available Dates on Crypto.com SFTP ===");
    println!("SFTP: {}@{}", sftp_username, sftp_host);
    println!("Base Path: {}", base_path);
    println!("Trading Pair: {}", trading_pair);
    println!();

    ensure_private_key_path(sftp_key_path)?;

    // Connect to SFTP
    println!("Connecting to SFTP...");
    let tcp = TcpStream::connect(format!("{}:22", sftp_host))
        .with_context(|| format!("Failed to connect to {}", sftp_host))?;
    
    let mut sess = Session::new()
        .map_err(|e| anyhow::anyhow!("Failed to create SSH session: {:?}", e))?;
    
    sess.set_tcp_stream(tcp);
    sess.handshake()
        .with_context(|| "SSH handshake failed")?;
    
    sess.userauth_pubkey_file(
        sftp_username,
        None,
        Path::new(sftp_key_path),
        None,
    )
    .with_context(|| format!("Authentication failed for user {}", sftp_username))?;
    
    if !sess.authenticated() {
        return Err(anyhow::anyhow!("Authentication failed"));
    }

    let sftp = sess.sftp()
        .with_context(|| "Failed to create SFTP session")?;

    // Check available years
    println!("Checking available years...");
    let years_path = format!("{}/", base_path);
    match sftp.readdir(Path::new(&years_path)) {
        Ok(entries) => {
            let mut years: Vec<String> = entries.iter()
                .filter_map(|(path, _)| {
                    path.file_name()
                        .and_then(|n| n.to_str())
                        .map(|s| s.to_string())
                })
                .filter(|name| name.chars().all(|c| c.is_ascii_digit()) && name.len() == 4)
                .collect();
            years.sort();
            
            if years.is_empty() {
                println!("No years found in {}", years_path);
                return Ok(());
            }
            
            println!("Found years: {}", years.join(", "));
            println!();

            let years_to_check: Vec<&str> = match year_filter {
                None => years.iter().map(|s| s.as_str()).collect(),
                Some(y) if years.contains(&y.to_string()) => vec![y],
                Some(y) => {
                    println!("Year '{}' not found. Available: {}", y, years.join(", "));
                    return Ok(());
                }
            };

            for year in years_to_check {
                println!("Checking year: {}", year);
                check_year(&sftp, &base_path, year, trading_pair)?;
                println!();
            }
        }
        Err(e) => {
            println!("Error reading years directory: {}", e);
            println!("Trying to list base path directly...");
            
            // Try listing the base path
            match sftp.readdir(Path::new(&base_path)) {
                Ok(entries) => {
                    println!("Found in base path:");
                    for (path, file_stat) in entries {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            let size = file_stat.size.unwrap_or(0);
                            if file_stat.is_dir() {
                                println!("  📁 {}", name);
                            } else {
                                println!("  📄 {} ({} bytes)", name, size);
                            }
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to read base path: {}", e));
                }
            }
        }
    }

    Ok(())
}

fn check_year(
    sftp: &ssh2::Sftp,
    base_path: &str,
    year: &str,
    trading_pair: &str,
) -> Result<()> {
    let year_path = format!("{}/{}/", base_path, year);
    
    // Check available months
    match sftp.readdir(Path::new(&year_path)) {
        Ok(entries) => {
            let mut months: Vec<u32> = entries.iter()
                .filter_map(|(path, _)| {
                    path.file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|s| s.parse::<u32>().ok())
                        .filter(|&m| m >= 1 && m <= 12)
                })
                .collect();
            months.sort();
            
            if months.is_empty() {
                println!("No months found for year {}", year);
                return Ok(());
            }
            
            println!("Found months: {:?}", months);
            println!();
            
            // Check each month for available days
            for month in months {
                check_month(sftp, base_path, year, month, trading_pair)?;
            }
        }
        Err(e) => {
            println!("Error reading months for {}: {}", year, e);
        }
    }
    
    Ok(())
}

fn check_month(
    sftp: &ssh2::Sftp,
    base_path: &str,
    year: &str,
    month: u32,
    trading_pair: &str,
) -> Result<()> {
    // Crypto.com uses unpadded month/day: 2025/1/1 not 2025/01/01
    let month_path = format!("{}/{}/{}/", base_path, year, month);
    
    match sftp.readdir(Path::new(&month_path)) {
        Ok(entries) => {
            let mut days: Vec<u32> = entries.iter()
                .filter_map(|(path, _)| {
                    path.file_name()
                        .and_then(|n| n.to_str())
                        .and_then(|s| s.parse::<u32>().ok())
                        .filter(|&d| d >= 1 && d <= 31)
                })
                .collect();
            days.sort();
            
            if days.is_empty() {
                return Ok(());
            }
            
            // Check which days have data for the trading pair
            let mut days_with_data = Vec::new();
            
            for day in &days {
                let day_path = format!("{}/{}/{}/{}/cdc/{}", 
                    base_path, year, month, day, trading_pair);
                
                match sftp.readdir(Path::new(&day_path)) {
                    Ok(files) => {
                        let file_count = files.iter()
                            .filter(|(path, _)| {
                                path.file_name()
                                    .and_then(|n| n.to_str())
                                    .map(|s| s.ends_with(".gz"))
                                    .unwrap_or(false)
                            })
                            .count();
                        
                        if file_count > 0 {
                            days_with_data.push((*day, file_count));
                        }
                    }
                    Err(_) => {
                        // Directory doesn't exist or no data
                    }
                }
            }
            
            if !days_with_data.is_empty() {
                println!("{}-{:02}: {} days with data", year, month, days_with_data.len());
                println!("  Days: {}", 
                    days_with_data.iter()
                        .map(|(d, _)| d.to_string())
                        .collect::<Vec<_>>()
                        .join(", "));
                
                // Show file counts for first and last day
                if let Some((first_day, first_count)) = days_with_data.first() {
                    if let Some((last_day, last_count)) = days_with_data.last() {
                        println!("  Files: Day {} = {}, Day {} = {}", 
                            first_day, first_count, last_day, last_count);
                    }
                }
                println!();
            }
        }
        Err(_) => {
            // Month directory doesn't exist
        }
    }
    
    Ok(())
}

