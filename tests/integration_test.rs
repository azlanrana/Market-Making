// Integration test - tests the full backtesting pipeline

use backtest_engine::BacktestPortfolio;
use backtest_engine::simulator::OrderBookSimulator;
use orderbook::order::{Order, OrderSide};
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[test]
fn test_order_simulator_basic() {
    let mut simulator = OrderBookSimulator::new();
    
    // Create a buy order
    let order = Order::new(
        "order1".to_string(),
        OrderSide::Buy,
        dec!(92700),
        dec!(0.1),
        0.0,
        1,
    );
    
    assert!(simulator.add_order(order));
    
    // Create a sell order
    let order2 = Order::new(
        "order2".to_string(),
        OrderSide::Sell,
        dec!(92800),
        dec!(0.1),
        0.0,
        1,
    );
    
    assert!(simulator.add_order(order2));
    
    // Check active orders
    let active = simulator.get_active_orders();
    assert_eq!(active.len(), 2);
}

#[test]
fn test_portfolio_trade() {
    let mut portfolio = BacktestPortfolio::new(
        Decimal::ZERO,
        dec!(1000000),
        dec!(92797),
    );
    
    // Buy 0.1 BTC at 92797
    portfolio.add_trade(
        0.0,
        OrderSide::Buy,
        dec!(92797),
        dec!(0.1),
        dec!(0),
    );
    
    let snapshot = portfolio.mark_to_market(0.0, dec!(92797));
    assert_eq!(snapshot.base_balance, dec!(0.1));
    assert_eq!(snapshot.quote_balance, dec!(1000000) - dec!(92797) * dec!(0.1));
    
    // Sell 0.05 BTC at 92800
    portfolio.add_trade(
        1.0,
        OrderSide::Sell,
        dec!(92800),
        dec!(0.05),
        dec!(0),
    );
    
    let snapshot2 = portfolio.mark_to_market(1.0, dec!(92800));
    assert_eq!(snapshot2.base_balance, dec!(0.05));
    // Should have realized P&L from the sale
    assert!(snapshot2.realized_pnl > Decimal::ZERO);
}

#[test]
fn test_fill_simulation() {
    let mut simulator = OrderBookSimulator::new();
    
    // Add our bid at 92700
    let order = Order::new(
        "our_bid".to_string(),
        OrderSide::Buy,
        dec!(92700),
        dec!(0.1),
        0.0,
        1,
    );
    simulator.add_order(order);
    
    // Create market snapshot where asks cross our bid
    let snapshot = OrderBookSnapshot {
        timestamp: 1.0,
        mid_price: dec!(92750),
        best_bid: dec!(92700),
        best_ask: dec!(92800),
        spread: dec!(100),
        spread_bps: 1.08,
        bids: vec![(dec!(92700), dec!(0.5))],
        asks: vec![(dec!(92700), dec!(0.2))], // Market ask at our bid price - should fill!
    };
    
    let fills = simulator.process_market_snapshot(&snapshot);
    assert!(!fills.is_empty(), "Should have fills when market crosses our order");
    
    if let Some(fill) = fills.first() {
        assert_eq!(fill.order_id, "our_bid");
        assert!(fill.filled_amount > Decimal::ZERO);
    }
}
