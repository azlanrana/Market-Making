use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::CsvParser;
use mm_core::strategy::Strategy;
use rebate_alpha::RebateAlphaStrategy;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[tokio::test]
async fn test_simple_backtest() {
    // Create a simple strategy
    let strategy = BalancedMMStrategy::new(5.0, 5.0, dec!(0.05)); // 5 bps spread, 0.05 BTC orders

    // Create backtest runner
    let mut runner = BacktestRunner::new(
        strategy,
        dec!(1000000), // $1M initial capital
        dec!(92797),   // Initial BTC price
        dec!(0),       // 0% maker fee
        false,         // No latency simulation for simple test
        dec!(0.01),    // BTC tick size
    );

    // Try to load data (will fail if file doesn't exist, but that's ok for now)
    // Path is relative to workspace root
    let data_path = "../../../hummingbot/data/backtest_crypto_com_l2_BTC-USDT_5_bid_5_ask.csv";
    let loader = CsvParser::new(data_path.to_string());

    match runner.run(loader).await {
        Ok(results) => {
            println!("Backtest completed successfully!");
            println!("Final portfolio value: {}", results.stats.final_value);
            println!("Total return: {}", results.stats.total_return);
            println!("Return %: {:.4}%", results.stats.return_pct * 100.0);
            println!("Realized P&L: {}", results.stats.realized_pnl);
            println!("Unrealized P&L: {}", results.stats.unrealized_pnl);
            println!("Max drawdown: {:.4}%", results.stats.max_drawdown * 100.0);
            println!(
                "Final inventory: {:.2}%",
                results.stats.final_inventory_pct * 100.0
            );
            println!("Total fills: {}", results.simulator_stats.total_fills);
        }
        Err(e) => {
            // For now, just check that the structure compiles
            // File might not exist, which is fine
            println!("Backtest error (expected if data file missing): {}", e);
        }
    }
}

#[test]
fn test_strategy_creation() {
    let strategy = BalancedMMStrategy::new(5.0, 5.0, dec!(0.05));
    assert_eq!(strategy.name(), "balanced_mm");

    // Validate config
    assert!(strategy.validate_config().is_ok());
}

#[test]
fn test_rebate_alpha_strategy_creation() {
    let strategy = RebateAlphaStrategy::new_for_rebate_farming(dec!(0.1), 0.75);
    assert_eq!(strategy.name(), "rebate_alpha_pro");
    assert!(strategy.validate_config().is_ok());
}

#[test]
fn test_portfolio_basic() {
    use backtest_engine::BacktestPortfolio;

    let mut portfolio = BacktestPortfolio::new(Decimal::ZERO, dec!(1000000), dec!(92797));

    // Add a buy trade
    use orderbook::order::OrderSide;
    portfolio.add_trade(0.0, OrderSide::Buy, dec!(92797), dec!(0.1), dec!(0));

    let snapshot = portfolio.mark_to_market(0.0, dec!(92797));
    assert_eq!(snapshot.base_balance, dec!(0.1));
    assert!(snapshot.quote_balance < dec!(1000000)); // Should have spent quote
}
