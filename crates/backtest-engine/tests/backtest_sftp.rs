// SFTP backtest - downloads orderbook data from Crypto.com SFTP server

use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::SftpLoader;
use rust_decimal_macros::dec;
use std::time::Instant;

#[tokio::test]
#[ignore] // Ignore by default - requires SFTP credentials
async fn backtest_sftp_24h() {
    println!("\n=== Running 1-Week SFTP Backtest ===");
    let start_time = Instant::now();
    
    // SFTP Configuration
    // Username: user080
    // Private key is in the project directory: user080 (1) 2
    let username = std::env::var("CRYPTO_COM_SFTP_USERNAME")
        .unwrap_or_else(|_| "user080".to_string());
    
    // Key path - can be set via env var or use default project location
    let key_path = std::env::var("CRYPTO_COM_SFTP_KEY_PATH")
        .unwrap_or_else(|_| {
            // Try to find the key file relative to the test
            let project_key = "../../user080 (1) 2";
            if std::path::Path::new(project_key).exists() {
                project_key.to_string()
            } else {
                "~/.ssh/crypto_com_key".to_string()
            }
        });
    
    // Remote path - can be set via env var
    // Format: exchange/book_l2_150_0010/yyyy/mm/dd/cdc/PAIR/
    // Example for BTC_USDT: exchange/book_l2_150_0010/2023/10/25/cdc/BTC_USDT
    let remote_path = std::env::var("CRYPTO_COM_SFTP_REMOTE_PATH")
        .unwrap_or_else(|_| "exchange/book_l2_150_0010/2023/10/25/cdc/BTC_USDT".to_string());
    
    let cache_dir = Some("./cache".to_string()); // Cache downloaded files
    
    // Estimate: ~85 files per hour (2029 files / 24 hours ≈ 85 files/hour)
    // For 1 week (7 days): ~14,280 files (168 hours × 85 files/hour)
    let max_files = std::env::var("MAX_FILES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(14280); // Default: 1 week worth
    
    let max_concurrent = std::env::var("MAX_CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100); // Default: 100 parallel downloads for maximum throughput
    
    println!("SFTP Configuration:");
    println!("  Host: data.crypto.com");
    println!("  Username: {}", username);
    println!("  Key path: {}", key_path);
    println!("  Remote path: {}", remote_path);
    println!("  Cache dir: {:?}", cache_dir);
    println!("  Max files: {} (~{} hours)", max_files, max_files as f64 / 85.0);
    println!("  Max concurrent downloads: {}", max_concurrent);
    println!();
    
    // Create SFTP loader with file limit and parallel downloads
    let loader = SftpLoader::new(
        username,
        key_path,
        remote_path,
        cache_dir,
    )
    .with_max_files(max_files)
    .with_max_concurrent_downloads(max_concurrent);
    
    // Create optimized strategy with inventory control
    let strategy = BalancedMMStrategy::new_with_config(
        dec!(0.1),  // Order size: 0.1 BTC
        5.0,        // Refresh every 5 seconds
        0.5,        // Replenish after 0.5 seconds
    )
    .with_target_inventory_pct(0.15)  // Target 15% inventory
    .with_inventory_limits(0.35, 0.50); // Soft 35%, Hard 50% (Wider bands)
    
    // Create backtest runner with Crypto.com fees
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000), // $1M initial capital
        dec!(92797),   // Initial BTC price (will be read from data)
        dec!(0.25),    // Maker rebate: 0.25 bps = 0.0025%
        false,         // No latency simulation
        dec!(0.01),    // BTC tick size
    );
    
    println!("Strategy: Multi-layer MM (1/2/3 bps, 0.1 BTC orders)");
    println!("Config: Refresh=5s, FillDelay=0.5s, Inventory Control Enabled");
    println!("Initial capital: $1,000,000");
    println!("Maker rebate: -0.0025%\n");
    println!("Connecting to SFTP and downloading data...\n");
    
    match runner.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();
            
            println!("=== 1-Week SFTP Backtest Results ===");
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!("\n--- Portfolio Performance ---");
            println!("Initial portfolio value: ${}", results.stats.initial_value);
            println!("Final portfolio value: ${}", results.stats.final_value);
            println!("Total return: ${}", results.stats.total_return);
            println!("Return %: {:.4}%", results.stats.return_pct * 100.0);
            
            // Calculate annualized return (scaled from 1 week)
            let hours = max_files as f64 / 85.0;
            let annualized_return = results.stats.return_pct * (365.0 * 24.0 / hours) * 100.0;
            println!("Annualized return (scaled): {:.2}%", annualized_return);
            
            println!("\n--- P&L Breakdown ---");
            println!("Realized P&L: ${}", results.stats.realized_pnl);
            println!("Unrealized P&L: ${}", results.stats.unrealized_pnl);
            println!("Total P&L: ${}", results.stats.realized_pnl + results.stats.unrealized_pnl);
            
            println!("\n--- Risk Metrics ---");
            println!("Max drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            println!("Max portfolio value: ${}", results.stats.max_portfolio_value);
            println!("Min portfolio value: ${}", results.stats.min_portfolio_value);
            
            println!("\n--- Inventory Management ---");
            println!("Final inventory: {:.2}%", results.stats.final_inventory_pct * 100.0);
            println!("Average inventory: {:.2}%", results.stats.avg_inventory_pct * 100.0);
            
            println!("\n--- Trading Activity ---");
            println!("Total fills: {}", results.simulator_stats.total_fills);
            println!("Filled orders: {}", results.simulator_stats.filled_orders);
            println!("Partially filled orders: {}", results.simulator_stats.partially_filled_orders);
            println!("Total volume: ${}", results.stats.total_volume);
            println!("Total fees (rebates): ${}", results.stats.total_fees);
            
            // Calculate key metrics
            let avg_fill_size = if results.simulator_stats.total_fills > 0 {
                results.stats.total_volume / rust_decimal::Decimal::from(results.simulator_stats.total_fills)
            } else {
                rust_decimal::Decimal::ZERO
            };
            println!("Average fill size: ${}", avg_fill_size);
            
            let fill_rate = if results.simulator_stats.filled_orders > 0 {
                (results.simulator_stats.total_fills as f64 / results.simulator_stats.filled_orders as f64) * 100.0
            } else {
                0.0
            };
            println!("Fill rate: {:.2}%", fill_rate);
            
            println!("\n--- Performance Summary ---");
            if results.stats.return_pct > 0.0 {
                println!("✅ Positive return: {:.4}%", results.stats.return_pct * 100.0);
            } else {
                println!("⚠️  Negative return: {:.4}%", results.stats.return_pct * 100.0);
            }
            
            if results.stats.final_inventory_pct > 0.4 && results.stats.final_inventory_pct < 0.6 {
                println!("✅ Inventory well balanced: {:.2}%", results.stats.final_inventory_pct * 100.0);
            } else {
                println!("⚠️  Inventory imbalance: {:.2}%", results.stats.final_inventory_pct * 100.0);
            }
            
            if results.stats.max_drawdown < 0.01 {
                println!("✅ Low drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            } else {
                println!("⚠️  Higher drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            }
        }
        Err(e) => {
            eprintln!("❌ Backtest failed: {}", e);
            eprintln!("Error details: {:?}", e);
            eprintln!("\nMake sure you have:");
            eprintln!("1. Set CRYPTO_COM_SFTP_USERNAME environment variable (default: user080)");
            eprintln!("2. Set CRYPTO_COM_SFTP_KEY_PATH environment variable");
            eprintln!("3. Set CRYPTO_COM_SFTP_REMOTE_PATH environment variable");
            eprintln!("4. Set MAX_FILES to limit downloads (default: 14280 = ~1 week)");
            eprintln!("5. Set MAX_CONCURRENT_DOWNLOADS (default: 50)");
            eprintln!("6. Private key file has correct permissions (chmod 600)");
        }
    }
}
