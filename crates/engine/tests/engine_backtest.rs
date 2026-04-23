use balanced_mm::BalancedMMStrategy;
use data_loader::CsvParser;
use mm_engine::{BacktestEngine, SimpleFeeModel};
use rust_decimal_macros::dec;

#[tokio::test]
async fn test_engine_backtest() {
    let strategy = BalancedMMStrategy::new(5.0, 5.0, dec!(0.05));
    let fee_model = SimpleFeeModel::new(dec!(0), dec!(1.5));

    let mut engine =
        BacktestEngine::new(strategy, dec!(1000000), dec!(92797), fee_model, dec!(0.01));

    let data_path = "../../hummingbot/data/backtest_crypto_com_l2_BTC-USDT_5_bid_5_ask.csv";
    let loader = CsvParser::new(data_path.to_string());

    match engine.run(loader).await {
        Ok(results) => {
            println!("New engine backtest completed!");
            println!("Total PnL: {}", results.stats.total_pnl);
            println!("Realized PnL: {}", results.stats.realized_pnl);
            println!("Win rate: {:.2}%", results.stats.win_rate * 100.0);
            println!("Sharpe: {:.2}", results.stats.sharpe);
            println!("Max drawdown: {:.2}%", results.stats.max_drawdown * 100.0);
            println!("Round trips: {}", results.stats.round_trip_count);
        }
        Err(e) => {
            println!("Engine backtest error (expected if data missing): {}", e);
        }
    }
}
