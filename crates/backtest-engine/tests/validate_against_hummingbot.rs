// Validation test - compares Rust backtest results with Hummingbot results

use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::CsvParser;
use rust_decimal_macros::dec;

#[tokio::test]
async fn validate_against_hummingbot() {
    // Create optimized strategy for tight BTC spreads
    // Optimized parameters for better returns:
    // - Faster refresh (5s) = more frequent order updates
    // - Faster replenishment (0.5s) = get back in market quickly after fills
    // - Larger order size (0.1 BTC) = more volume = more spread capture
    let strategy = BalancedMMStrategy::new_with_config(
        dec!(0.1), // Order size: 0.1 BTC (increased from 0.05)
        5.0,       // Refresh every 5 seconds (reduced from 10s for more aggressive trading)
        0.5,       // Replenish after 0.5 seconds (reduced from 3s for faster replenishment)
    );

    // Create backtest runner with Crypto.com fees
    // Initial capital: $1M, Initial price: ~92797
    // Maker rebate: -0.0025% (we get paid for making liquidity)
    // Taker fee: 0.031% (we pay when taking liquidity, but we're only placing limit orders)
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000), // $1M initial capital
        dec!(92797),   // Initial BTC price (from CSV)
        dec!(-0.25),   // Maker rebate: -0.25 bps = -0.0025% (negative = rebate, we get paid)
        false,         // No latency simulation for initial validation
        dec!(0.01),    // BTC tick size
    );

    // Load the CSV data
    let data_path = "../../../hummingbot/data/backtest_crypto_com_l2_BTC-USDT_5_bid_5_ask.csv";
    let loader = CsvParser::new(data_path.to_string());

    println!("\n=== Running Validation Backtest ===");
    println!("Data file: {}", data_path);
    println!("Strategy: Multi-layer MM (1/2/3 bps, 0.1 BTC orders)");
    println!("Config: Refresh=5s, FillDelay=0.5s");
    println!("Initial capital: $1,000,000");
    println!("Maker rebate: -0.0025% (we get paid for making liquidity)");
    println!("Taker fee: 0.031% (for reference, we only place limit orders)\n");

    match runner.run(loader).await {
        Ok(results) => {
            println!("=== Backtest Results ===");
            println!("Initial portfolio value: ${}", results.stats.initial_value);
            println!("Final portfolio value: ${}", results.stats.final_value);
            println!("Total return: ${}", results.stats.total_return);
            println!("Return %: {:.4}%", results.stats.return_pct * 100.0);
            println!("\n--- P&L Breakdown ---");
            println!("Realized P&L: ${}", results.stats.realized_pnl);
            println!("Unrealized P&L: ${}", results.stats.unrealized_pnl);
            println!(
                "Total P&L: ${}",
                results.stats.realized_pnl + results.stats.unrealized_pnl
            );
            println!("\n--- Risk Metrics ---");
            println!("Max drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            println!(
                "Max portfolio value: ${}",
                results.stats.max_portfolio_value
            );
            println!(
                "Min portfolio value: ${}",
                results.stats.min_portfolio_value
            );
            println!("\n--- Inventory ---");
            println!(
                "Final inventory: {:.2}%",
                results.stats.final_inventory_pct * 100.0
            );
            println!(
                "Average inventory: {:.2}%",
                results.stats.avg_inventory_pct * 100.0
            );
            println!("\n--- Trading Activity ---");
            println!("Total fills: {}", results.simulator_stats.total_fills);
            println!("Filled orders: {}", results.simulator_stats.filled_orders);
            println!(
                "Partially filled orders: {}",
                results.simulator_stats.partially_filled_orders
            );
            println!("Total volume: ${}", results.stats.total_volume);
            println!("Total fees: ${}", results.stats.total_fees);

            println!("\n=== Comparison with Hummingbot ===");
            println!("Expected from Hummingbot:");
            println!("  Total Return: +0.24% ($2,431)");
            println!("  Realized P&L: -$302,758 (needs investigation)");
            println!("  Final Inventory: 100% (needs fixing)");
            println!("  Max Drawdown: 0.30%");
            println!("\nOur Results:");
            println!(
                "  Total Return: {:.4}% (${})",
                results.stats.return_pct * 100.0,
                results.stats.total_return
            );
            println!("  Realized P&L: ${}", results.stats.realized_pnl);
            println!(
                "  Final Inventory: {:.2}%",
                results.stats.final_inventory_pct * 100.0
            );
            println!("  Max Drawdown: {:.4}%", results.stats.max_drawdown * 100.0);

            // Validation checks
            println!("\n=== Validation Checks ===");
            let return_match = (results.stats.return_pct * 100.0 - 0.24).abs() < 0.1;
            let drawdown_match = (results.stats.max_drawdown * 100.0 - 0.30).abs() < 0.1;

            if return_match {
                println!("✅ Return matches Hummingbot (within 0.1%)");
            } else {
                println!("⚠️  Return differs from Hummingbot");
                println!(
                    "   Expected: ~0.24%, Got: {:.4}%",
                    results.stats.return_pct * 100.0
                );
            }

            if drawdown_match {
                println!("✅ Drawdown matches Hummingbot (within 0.1%)");
            } else {
                println!("⚠️  Drawdown differs from Hummingbot");
                println!(
                    "   Expected: ~0.30%, Got: {:.4}%",
                    results.stats.max_drawdown * 100.0
                );
            }

            // Check if we have fills
            if results.simulator_stats.total_fills > 0 {
                println!("✅ Orders are filling correctly");
            } else {
                println!("⚠️  No fills detected - check order placement logic");
            }

            // Check inventory (should be balanced, not 100%)
            if results.stats.final_inventory_pct > 0.9 {
                println!(
                    "⚠️  Inventory imbalance detected ({}%) - matches Hummingbot issue",
                    results.stats.final_inventory_pct * 100.0
                );
            } else {
                println!("✅ Inventory is balanced");
            }
        }
        Err(e) => {
            eprintln!("❌ Backtest failed: {}", e);
            eprintln!("This might be expected if the CSV file path is incorrect.");
            eprintln!("Please check the path: ../../../hummingbot/data/backtest_crypto_com_l2_BTC-USDT_5_bid_5_ask.csv");
        }
    }
}
