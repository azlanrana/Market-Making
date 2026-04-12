//! Determinism test: same data + seed => identical results.
use backtest_engine::BacktestRunner;
use balanced_mm::BalancedMMStrategy;
use data_loader::DataLoader;
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

struct VecLoader {
    snapshots: Vec<OrderBookSnapshot>,
}

impl DataLoader for VecLoader {
    fn load_snapshots(&self) -> anyhow::Result<Box<dyn Iterator<Item = anyhow::Result<OrderBookSnapshot>> + Send>> {
        let snapshots = self.snapshots.clone();
        let iter = snapshots.into_iter().map(|s| Ok(s));
        Ok(Box::new(iter))
    }
}

fn make_snapshots() -> Vec<OrderBookSnapshot> {
    (0..100)
        .map(|i| {
            let ts = 1700000000.0 + i as f64 * 5.0;
            let mid = 3000.0 + i as f64 * 0.1;
            let best_bid = mid - 0.25;
            let best_ask = mid + 0.25;
            OrderBookSnapshot::from_price_levels(
                ts,
                rust_decimal::Decimal::from_f64_retain(mid).unwrap(),
                rust_decimal::Decimal::from_f64_retain(best_bid).unwrap(),
                rust_decimal::Decimal::from_f64_retain(best_ask).unwrap(),
                vec![vec![best_bid, 10.0]],
                vec![vec![best_ask, 10.0]],
            )
            .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn test_determinism_with_latency() {
    let snapshots = make_snapshots();
    let loader1 = VecLoader {
        snapshots: snapshots.clone(),
    };
    let loader2 = VecLoader {
        snapshots,
    };

    let strategy1 = BalancedMMStrategy::new(5.0, 5.0, dec!(0.05));
    let mut runner1 = BacktestRunner::new(
        strategy1,
        dec!(1000000),
        dec!(3000),
        dec!(0),
        true,
        dec!(0.01),
    );
    let results1 = runner1.run(loader1).await.unwrap();

    let strategy2 = BalancedMMStrategy::new(5.0, 5.0, dec!(0.05));
    let mut runner2 = BacktestRunner::new(
        strategy2,
        dec!(1000000),
        dec!(3000),
        dec!(0),
        true,
        dec!(0.01),
    );
    let results2 = runner2.run(loader2).await.unwrap();

    assert_eq!(
        results1.stats.final_value,
        results2.stats.final_value,
        "Determinism: final_value must match"
    );
    assert_eq!(
        results1.simulator_stats.total_fills,
        results2.simulator_stats.total_fills,
        "Determinism: total_fills must match"
    );
}
