// 24-hour backtest - runs full day of trading

use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::{CsvParser, MultiCsvParser};
use rust_decimal_macros::dec;
use std::time::Instant;

#[tokio::test]
async fn backtest_24h() {
    println!("\n=== Running 24-Hour Backtest ===");
    let start_time = Instant::now();
    
    // Create optimized strategy with inventory control
    let strategy = BalancedMMStrategy::new_with_config(
        dec!(0.1),  // Order size: 0.1 BTC
        5.0,        // Refresh every 5 seconds
        0.5,        // Replenish after 0.5 seconds
    );
    
    // Create backtest runner with Crypto.com fees
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000), // $1M initial capital
        dec!(92797),   // Initial BTC price (will be read from CSV)
        dec!(-0.25),   // Maker rebate: -0.25 bps = -0.0025%
        false,         // No latency simulation
        dec!(0.01),    // BTC tick size
    );

    // Load 24-hour data
    // For now, using the available CSV file
    // TODO: If you have 24-hour data files, add them to the vector below
    let data_path = "../../../hummingbot/data/backtest_crypto_com_l2_BTC-USDT_5_bid_5_ask.csv";
    
    // If you have multiple CSV files for 24 hours, use MultiCsvParser:
    // let file_paths = vec![
    //     "../../../hummingbot/data/file1.csv".to_string(),
    //     "../../../hummingbot/data/file2.csv".to_string(),
    //     // ... add all 24-hour files
    // ];
    // let loader = MultiCsvParser::new(file_paths);
    
    // For single file, use CsvParser:
    let loader = CsvParser::new(data_path.to_string());
    
    println!("Data file: {}", data_path);
    println!("Note: Currently using available CSV. For full 24h, add all hourly CSV files to MultiCsvParser.");
    println!("Strategy: Multi-layer MM (1/2/3 bps, 0.1 BTC orders)");
    println!("Config: Refresh=5s, FillDelay=0.5s, Inventory Control Enabled");
    println!("Initial capital: $1,000,000");
    println!("Maker rebate: -0.0025%\n");
    println!("Starting backtest...\n");
    
    match runner.run(loader).await {
        Ok(results) => {
            let elapsed = start_time.elapsed();
            
            println!("=== 24-Hour Backtest Results ===");
            println!("Backtest duration: {:.2}s", elapsed.as_secs_f64());
            println!("\n--- Portfolio Performance ---");
            println!("Initial portfolio value: ${}", results.stats.initial_value);
            println!("Final portfolio value: ${}", results.stats.final_value);
            println!("Total return: ${}", results.stats.total_return);
            println!("Return %: {:.4}%", results.stats.return_pct * 100.0);
            
            // Calculate annualized return (assuming 24 hours = 1 day)
            let annualized_return = results.stats.return_pct * 365.0 * 100.0;
            println!("Annualized return (if scaled): {:.2}%", annualized_return);
            
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
        }
    }
}

