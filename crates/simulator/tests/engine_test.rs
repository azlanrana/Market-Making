use mm_core_types::{FillReason, Order, OrderBookSnapshot, OrderType, Side};
use mm_simulator::{MatchingEngine, QueueModelConfig};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn snapshot(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>, ts: f64) -> OrderBookSnapshot {
    let bids_d: Vec<(Decimal, Decimal)> = bids
        .into_iter()
        .map(|(p, q)| {
            (
                Decimal::from_f64_retain(p).unwrap(),
                Decimal::from_f64_retain(q).unwrap(),
            )
        })
        .collect();
    let asks_d: Vec<(Decimal, Decimal)> = asks
        .into_iter()
        .map(|(p, q)| {
            (
                Decimal::from_f64_retain(p).unwrap(),
                Decimal::from_f64_retain(q).unwrap(),
            )
        })
        .collect();
    OrderBookSnapshot::new(ts, bids_d, asks_d)
}

#[test]
fn test_limit_fill_at_maker_price() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        price_improving_queue_pct: 0.0,
        delta_trade_fraction: 1.0,
        ..QueueModelConfig::default()
    });
    let book1 = snapshot(
        vec![(1000.0, 10.0), (999.0, 5.0)],
        vec![(1001.0, 10.0)],
        1.0,
    );
    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let order = Order::new(
        "o1".to_string(),
        Side::Buy,
        dec!(1001),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let book2 = snapshot(vec![(1000.0, 10.0)], vec![(999.0, 5.0), (1000.0, 3.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price, dec!(1001));
    assert_eq!(fills[0].amount, dec!(2));
    assert_eq!(fills[0].fill_reason, Some(FillReason::CrossedBook));
}

#[test]
fn test_crossed_book_fill_uses_removed_depth_delta() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        price_improving_queue_pct: 0.0,
        delta_trade_fraction: 0.5,
        ..QueueModelConfig::default()
    });
    let book1 = snapshot(vec![(1000.0, 10.0)], vec![(1001.0, 10.0)], 1.0);
    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let order = Order::new(
        "crossdelta1".to_string(),
        Side::Buy,
        dec!(1001),
        dec!(8),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let book2 = snapshot(vec![(1000.0, 10.0)], vec![(999.0, 5.0), (1000.0, 5.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].price, dec!(1001));
    assert_eq!(fills[0].amount, dec!(5));
    assert_eq!(fills[0].fill_reason, Some(FillReason::CrossedBook));
}

#[test]
fn test_crossed_book_does_not_fill_without_removed_depth() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        price_improving_queue_pct: 0.0,
        delta_trade_fraction: 1.0,
        ..QueueModelConfig::default()
    });
    let book1 = snapshot(vec![(1000.0, 10.0)], vec![(1002.0, 10.0)], 1.0);
    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let order = Order::new(
        "crossguard1".to_string(),
        Side::Buy,
        dec!(1001),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let book2 = snapshot(vec![(1000.0, 10.0)], vec![(999.0, 5.0), (1000.0, 5.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());
}

#[test]
fn test_market_slippage() {
    let mut engine = MatchingEngine::new();
    let book = snapshot(
        vec![(100.0, 10.0)],
        vec![(101.0, 2.0), (102.0, 5.0), (103.0, 10.0)],
        1.0,
    );
    let order = Order::new(
        "m1".to_string(),
        Side::Buy,
        dec!(0),
        dec!(10),
        OrderType::Market,
        0,
        1.0,
    );
    let fills = engine.submit(order, &book);
    assert_eq!(fills.len(), 3);
    assert_eq!(fills[0].price, dec!(101));
    assert_eq!(fills[0].amount, dec!(2));
    assert_eq!(fills[1].price, dec!(102));
    assert_eq!(fills[1].amount, dec!(5));
    assert_eq!(fills[2].price, dec!(103));
    assert_eq!(fills[2].amount, dec!(3));
}

#[test]
fn test_partial_fill_then_complete() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        touch_queue_pct: 0.0,
        delta_trade_fraction: 1.0,
        ..QueueModelConfig::default()
    });
    let book1 = snapshot(
        vec![(1000.0, 5.0)],
        vec![(1000.0, 2.0), (1001.0, 10.0)],
        1.0,
    );
    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let order = Order::new(
        "o1".to_string(),
        Side::Sell,
        dec!(1000),
        dec!(5),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let book2 = snapshot(
        vec![(1000.0, 2.0)],
        vec![(1000.0, 2.0), (1001.0, 10.0)],
        2.0,
    );
    let fills1 = engine.process_book_update(&book2);
    assert_eq!(fills1.len(), 1);
    assert_eq!(fills1[0].amount, dec!(3));

    let book3 = snapshot(vec![], vec![(1000.0, 2.0)], 3.0);
    let fills2 = engine.process_book_update(&book3);
    assert_eq!(fills2.len(), 1);
    assert_eq!(fills2[0].amount, dec!(2));
}

#[test]
fn test_cancel_by_id() {
    let mut engine = MatchingEngine::new();
    let book = snapshot(vec![(1000.0, 10.0)], vec![(1001.0, 10.0)], 1.0);
    let order = Order::new(
        "o1".to_string(),
        Side::Buy,
        dec!(1000),
        dec!(5),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book);
    assert!(engine.get_order("o1").is_some());

    engine.cancel("o1");
    assert!(engine.get_order("o1").is_none());
}

#[test]
fn test_touch_depletion_clears_market_queue_only() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        queue_depletion_enabled: true,
        delta_trade_fraction: 1.0,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "o1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let book2 = snapshot(vec![(100.0, 4.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let order = engine.get_order("o1").unwrap();
    assert_eq!(order.market_queue_ahead, dec!(4));
    assert_eq!(order.internal_queue_ahead, Decimal::ZERO);
}

#[test]
fn test_internal_fifo_gates_later_orders_after_market_queue_depletion() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        touch_queue_pct: 0.5,
        queue_depletion_enabled: true,
        delta_trade_fraction: 1.0,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 20.0)], vec![(101.0, 10.0)], 1.0);
    let order1 = Order::new(
        "o1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(5),
        OrderType::Limit,
        0,
        1.0,
    );
    let order2 = Order::new(
        "o2".to_string(),
        Side::Buy,
        dec!(100),
        dec!(5),
        OrderType::Limit,
        0,
        2.0,
    );
    engine.submit(order1, &book1);
    engine.submit(order2, &book1);

    let second = engine.get_order("o2").unwrap();
    assert_eq!(second.market_queue_ahead, dec!(10));
    assert_eq!(second.internal_queue_ahead, dec!(5));

    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let book2 = snapshot(vec![(100.0, 4.0)], vec![(101.0, 10.0)], 2.0);
    let fills1 = engine.process_book_update(&book2);
    assert_eq!(fills1.len(), 1);
    assert_eq!(fills1[0].order_id, "o1");
    assert_eq!(fills1[0].fill_reason, Some(FillReason::QueueDepletion));

    let remaining = engine.get_order("o2").unwrap();
    assert_eq!(remaining.market_queue_ahead, Decimal::ZERO);
    assert_eq!(remaining.internal_queue_ahead, Decimal::ZERO);

    let book3 = snapshot(vec![(100.0, 2.0)], vec![(101.0, 10.0)], 3.0);
    let fills2 = engine.process_book_update(&book3);
    assert_eq!(fills2.len(), 1);
    assert_eq!(fills2[0].order_id, "o2");
    assert_eq!(fills2[0].fill_reason, Some(FillReason::QueueDepletion));
}

#[test]
fn test_inside_spread_buy_queue_depletion_via_touch_blend() {
    // Bid at 1001 inside spread; no L2 bid level at 1001. Best-bid depletion × blend → maker fill.
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        price_improving_queue_pct: 0.0,
        touch_queue_pct: 0.0,
        queue_depletion_enabled: true,
        crossed_book_fill_enabled: false,
        delta_trade_fraction: 1.0,
        min_delta_for_fill: 0.001,
        price_improving_bid_depletion_blend: 1.0,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(1000.0, 10.0)], vec![(1002.0, 10.0)], 1.0);
    let _ = engine.process_book_update(&book1);

    let order = Order::new(
        "inside1".to_string(),
        Side::Buy,
        dec!(1001),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let book2 = snapshot(vec![(1000.0, 5.0)], vec![(1002.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id, "inside1");
    assert_eq!(fills[0].price, dec!(1001));
    assert_eq!(fills[0].fill_reason, Some(FillReason::QueueDepletion));
}

#[test]
fn test_inside_spread_price_improver_starts_at_front_of_queue_by_default() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        queue_depletion_enabled: true,
        crossed_book_fill_enabled: false,
        delta_trade_fraction: 1.0,
        min_delta_for_fill: 0.001,
        price_improving_bid_depletion_blend: 1.0,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(1000.0, 10.0)], vec![(1002.0, 10.0)], 1.0);
    let _ = engine.process_book_update(&book1);

    let order = Order::new(
        "inside_default".to_string(),
        Side::Buy,
        dec!(1001),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let resting = engine.get_order("inside_default").unwrap();
    assert_eq!(resting.market_queue_ahead, Decimal::ZERO);

    let book2 = snapshot(vec![(1000.0, 5.0)], vec![(1002.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id, "inside_default");
    assert_eq!(fills[0].fill_reason, Some(FillReason::QueueDepletion));
}

#[test]
fn test_no_depletion_fill_when_touch_price_changes() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        touch_queue_pct: 0.5,
        queue_depletion_enabled: true,
        delta_trade_fraction: 1.0,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 20.0), (99.0, 10.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "o1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(5),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(99.0, 18.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let order = engine.get_order("o1").unwrap();
    assert_eq!(order.market_queue_ahead, dec!(10));
}

#[test]
fn test_sell_side_queue_depletion_mirror() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        touch_queue_pct: 0.5,
        queue_depletion_enabled: true,
        delta_trade_fraction: 1.0,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 20.0)], 1.0);
    let order = Order::new(
        "s1".to_string(),
        Side::Sell,
        dec!(101),
        dec!(5),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 8.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert_eq!(fills.len(), 1);
    assert_eq!(fills[0].order_id, "s1");
    assert_eq!(fills[0].fill_reason, Some(FillReason::QueueDepletion));
    assert_eq!(fills[0].price, dec!(101));
}

#[test]
fn test_unchanged_touch_churn_advances_market_queue_without_fill() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        queue_depletion_enabled: true,
        queue_churn_enabled: true,
        queue_turnover_rate_per_sec: 1.0,
        cancel_ahead_fraction: 0.5,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "churn1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);

    let prime = engine.process_book_update(&book1);
    assert!(prime.is_empty());

    let book2 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let order = engine.get_order("churn1").unwrap();
    assert_eq!(order.market_queue_ahead, dec!(5));
    assert_eq!(order.internal_queue_ahead, Decimal::ZERO);

    let stats = engine.queue_churn_stats();
    assert_eq!(stats.cancel_ahead_advance_events, 1);
    assert_eq!(stats.cancel_ahead_advance_total, dec!(5));
}

#[test]
fn test_visible_non_trade_shrink_advances_queue_without_latency_heuristic() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        queue_depletion_enabled: true,
        queue_churn_enabled: true,
        delta_trade_fraction: 0.5,
        queue_turnover_rate_per_sec: 0.0,
        cancel_ahead_fraction: 1.0,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "visible_cancel1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(100.0, 6.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let order = engine.get_order("visible_cancel1").unwrap();
    assert_eq!(order.market_queue_ahead, dec!(6));

    let stats = engine.queue_churn_stats();
    assert_eq!(stats.cancel_ahead_advance_events, 1);
    assert_eq!(stats.cancel_ahead_advance_total, dec!(2));
}

#[test]
fn test_mixed_trade_and_cancel_interval_only_fills_on_trade_component() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        touch_queue_pct: 0.3,
        queue_depletion_enabled: true,
        queue_churn_enabled: true,
        delta_trade_fraction: 0.5,
        min_delta_for_fill: 0.001,
        queue_turnover_rate_per_sec: 1.0,
        cancel_ahead_fraction: 1.0,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 20.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "mixed1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(3),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(100.0, 8.0)], vec![(101.0, 10.0)], 2.0);
    let fills1 = engine.process_book_update(&book2);
    assert!(fills1.is_empty());

    let order = engine.get_order("mixed1").unwrap();
    assert_eq!(order.market_queue_ahead, Decimal::ZERO);

    let stats = engine.queue_churn_stats();
    assert_eq!(stats.cancel_ahead_advance_events, 1);
    assert_eq!(stats.cancel_ahead_advance_total, dec!(6));

    let book3 = snapshot(vec![(100.0, 6.0)], vec![(101.0, 10.0)], 3.0);
    let fills2 = engine.process_book_update(&book3);
    assert_eq!(fills2.len(), 1);
    assert_eq!(fills2[0].order_id, "mixed1");
    assert_eq!(fills2[0].amount, dec!(1));
    assert_eq!(fills2[0].fill_reason, Some(FillReason::QueueDepletion));
}

#[test]
fn test_churn_keeps_later_orders_blocked_by_internal_fifo() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        touch_queue_pct: 0.5,
        queue_depletion_enabled: true,
        queue_churn_enabled: true,
        queue_turnover_rate_per_sec: 1.0,
        cancel_ahead_fraction: 0.5,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 1.0);
    let order1 = Order::new(
        "fifo1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(5),
        OrderType::Limit,
        0,
        1.0,
    );
    let order2 = Order::new(
        "fifo2".to_string(),
        Side::Buy,
        dec!(100),
        dec!(5),
        OrderType::Limit,
        0,
        2.0,
    );
    engine.submit(order1, &book1);
    engine.submit(order2, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let first = engine.get_order("fifo1").unwrap();
    assert_eq!(first.market_queue_ahead, Decimal::ZERO);
    assert_eq!(first.internal_queue_ahead, Decimal::ZERO);

    let second = engine.get_order("fifo2").unwrap();
    assert_eq!(second.market_queue_ahead, Decimal::ZERO);
    assert_eq!(second.internal_queue_ahead, dec!(5));
}

#[test]
fn test_no_churn_inference_when_touch_price_changes() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        queue_depletion_enabled: true,
        queue_churn_enabled: true,
        queue_turnover_rate_per_sec: 10.0,
        cancel_ahead_fraction: 1.0,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0), (99.0, 8.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "guard1".to_string(),
        Side::Buy,
        dec!(100),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(99.0, 10.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let order = engine.get_order("guard1").unwrap();
    assert_eq!(order.market_queue_ahead, dec!(10));

    let stats = engine.queue_churn_stats();
    assert_eq!(stats.cancel_ahead_advance_events, 0);
    assert_eq!(stats.cancel_ahead_advance_total, Decimal::ZERO);
}

#[test]
fn test_sell_side_churn_advances_market_queue_without_fill() {
    let mut engine = MatchingEngine::new().with_queue_config(QueueModelConfig {
        queue_depletion_enabled: true,
        queue_churn_enabled: true,
        queue_turnover_rate_per_sec: 1.0,
        cancel_ahead_fraction: 0.5,
        min_delta_for_fill: 0.001,
        ..QueueModelConfig::default()
    });

    let book1 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 1.0);
    let order = Order::new(
        "sellchurn1".to_string(),
        Side::Sell,
        dec!(101),
        dec!(2),
        OrderType::Limit,
        0,
        1.0,
    );
    engine.submit(order, &book1);
    let _ = engine.process_book_update(&book1);

    let book2 = snapshot(vec![(100.0, 10.0)], vec![(101.0, 10.0)], 2.0);
    let fills = engine.process_book_update(&book2);
    assert!(fills.is_empty());

    let order = engine.get_order("sellchurn1").unwrap();
    assert_eq!(order.market_queue_ahead, dec!(5));
    assert_eq!(order.internal_queue_ahead, Decimal::ZERO);

    let stats = engine.queue_churn_stats();
    assert_eq!(stats.cancel_ahead_advance_events, 1);
    assert_eq!(stats.cancel_ahead_advance_total, dec!(5));
}
