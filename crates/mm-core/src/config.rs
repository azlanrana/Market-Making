use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetConfig {
    pub symbol: String,
    pub base_spread_bps: f64,
    pub min_order_size: f64,
    pub tick_size: f64,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyParams {
    pub strategy_type: String,
    #[serde(flatten)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    pub data_file: String,
    pub initial_capital: f64,
    pub start_time: Option<f64>,
    pub end_time: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub asset: AssetConfig,
    pub strategy: StrategyParams,
    pub backtest: Option<BacktestConfig>,
}
