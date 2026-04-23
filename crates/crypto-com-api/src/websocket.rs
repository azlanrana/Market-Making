use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use orderbook::snapshot::OrderBookSnapshot;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[derive(Debug, Clone)]
pub struct MarketStreamConfig {
    pub instrument_name: String,
    pub depth: u32,
    pub include_trades: bool,
}

impl MarketStreamConfig {
    pub fn new(instrument_name: impl Into<String>, depth: u32) -> Self {
        Self {
            instrument_name: instrument_name.into(),
            depth,
            include_trades: false,
        }
    }

    pub fn with_trades(mut self, include_trades: bool) -> Self {
        self.include_trades = include_trades;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicTrade {
    pub instrument_name: String,
    pub side: String,
    pub price: f64,
    pub amount: f64,
    pub timestamp: f64,
    pub trade_id: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MarketEvent {
    Book(OrderBookSnapshot),
    Trade(PublicTrade),
}

#[derive(Debug, Clone)]
pub struct WebSocketClient {
    url: String,
    reconnect_delay: Duration,
}

#[derive(Debug, Default)]
struct LocalBookState {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_u: Option<i64>,
}

impl WebSocketClient {
    pub fn new() -> Self {
        Self {
            url: "wss://stream.crypto.com/exchange/v1/market".to_string(),
            reconnect_delay: Duration::from_secs(2),
        }
    }

    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn with_reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay;
        self
    }

    pub async fn stream_market_data(
        &self,
        config: MarketStreamConfig,
    ) -> Result<mpsc::UnboundedReceiver<Result<MarketEvent>>> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let url = self.url.clone();
        let reconnect_delay = self.reconnect_delay;

        tokio::spawn(async move {
            let mut subscribe_id: i64 = 1;
            let mut book_state;
            loop {
                let stream = match connect_async(&url).await {
                    Ok((stream, _)) => stream,
                    Err(err) => {
                        if sender
                            .send(Err(anyhow!("websocket connect failed: {err}")))
                            .is_err()
                        {
                            break;
                        }
                        tokio::time::sleep(reconnect_delay).await;
                        continue;
                    }
                };

                let (mut write, mut read) = stream.split();
                book_state = LocalBookState::default();
                // Crypto.com recommends waiting 1 second after establishing the socket
                // before sending subscription requests to avoid prorated rate-limit rejects.
                tokio::time::sleep(Duration::from_secs(1)).await;
                let subscribe_request = subscribe_message(
                    subscribe_id,
                    &config.instrument_name,
                    config.depth,
                    config.include_trades,
                );

                if let Err(err) = write
                    .send(Message::Text(subscribe_request.to_string()))
                    .await
                {
                    if sender
                        .send(Err(anyhow!("websocket subscribe failed: {err}")))
                        .is_err()
                    {
                        break;
                    }
                    tokio::time::sleep(reconnect_delay).await;
                    subscribe_id += 1;
                    continue;
                }

                let mut should_reconnect = false;
                while let Some(message) = read.next().await {
                    match message {
                        Ok(Message::Text(text)) => match serde_json::from_str::<Value>(&text) {
                            Ok(value) => {
                                if is_subscribe_ack(&value) {
                                    continue;
                                }
                                if let Some(heartbeat_id) = heartbeat_id(&value) {
                                    let response = heartbeat_response(heartbeat_id);
                                    if let Err(err) =
                                        write.send(Message::Text(response.to_string())).await
                                    {
                                        let _ = sender
                                            .send(Err(anyhow!("heartbeat response failed: {err}")));
                                        should_reconnect = true;
                                        break;
                                    }
                                    continue;
                                }

                                match parse_market_events(&value, &mut book_state) {
                                    Ok(events) => {
                                        for event in events {
                                            if sender.send(Ok(event)).is_err() {
                                                return;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        if sender.send(Err(err)).is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                if sender
                                    .send(Err(anyhow!("failed to parse websocket message: {err}")))
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        },
                        Ok(Message::Ping(payload)) => {
                            if let Err(err) = write.send(Message::Pong(payload)).await {
                                let _ = sender.send(Err(anyhow!("websocket pong failed: {err}")));
                                should_reconnect = true;
                                break;
                            }
                        }
                        Ok(Message::Close(_)) => {
                            should_reconnect = true;
                            break;
                        }
                        Ok(_) => {}
                        Err(err) => {
                            let _ = sender.send(Err(anyhow!("websocket stream error: {err}")));
                            should_reconnect = true;
                            break;
                        }
                    }
                }

                if !should_reconnect {
                    let _ = sender.send(Err(anyhow!("websocket stream ended unexpectedly")));
                }

                tokio::time::sleep(reconnect_delay).await;
                subscribe_id += 1;
            }
        });

        Ok(receiver)
    }
}

#[derive(Serialize)]
struct SubscribeParams {
    channels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    book_subscription_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    book_update_frequency: Option<u32>,
}

#[derive(Serialize)]
struct SubscribeRequest {
    id: i64,
    method: &'static str,
    params: SubscribeParams,
    nonce: i64,
}

#[derive(Serialize)]
struct HeartbeatResponse {
    id: i64,
    method: &'static str,
}

fn subscribe_message(id: i64, instrument_name: &str, depth: u32, include_trades: bool) -> Value {
    let mut channels = vec![format!("book.{instrument_name}.{depth}")];
    if include_trades {
        channels.push(format!("trade.{instrument_name}"));
    }
    serde_json::to_value(SubscribeRequest {
        id,
        method: "subscribe",
        params: SubscribeParams {
            channels,
            book_subscription_type: Some("SNAPSHOT_AND_UPDATE"),
            book_update_frequency: Some(100),
        },
        nonce: chrono_like_nonce(),
    })
    .expect("subscribe request serialization must succeed")
}

fn heartbeat_response(id: i64) -> Value {
    serde_json::to_value(HeartbeatResponse {
        id,
        method: "public/respond-heartbeat",
    })
    .expect("heartbeat response serialization must succeed")
}

fn chrono_like_nonce() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

fn is_subscribe_ack(value: &Value) -> bool {
    value.get("method").and_then(Value::as_str) == Some("subscribe")
        && value.get("code").and_then(Value::as_i64) == Some(0)
        && value
            .get("id")
            .and_then(Value::as_i64)
            .map(|id| id >= 0)
            .unwrap_or(false)
}

fn heartbeat_id(value: &Value) -> Option<i64> {
    match value.get("method").and_then(Value::as_str) {
        Some("public/heartbeat") | Some("heartbeat") => value.get("id").and_then(Value::as_i64),
        _ => None,
    }
}

fn parse_market_events(value: &Value, book_state: &mut LocalBookState) -> Result<Vec<MarketEvent>> {
    let payload = value
        .get("result")
        .or_else(|| value.get("params"))
        .ok_or_else(|| anyhow!("missing websocket result payload"))?;
    let channel = payload
        .get("channel")
        .or_else(|| payload.get("subscription"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing websocket channel"))?;

    if channel == "book" {
        let snapshot = parse_book_snapshot(payload, book_state)?;
        return Ok(vec![MarketEvent::Book(snapshot)]);
    }
    if channel == "book.update" {
        if let Some(snapshot) = apply_book_update(payload, book_state)? {
            return Ok(vec![MarketEvent::Book(snapshot)]);
        }
        return Ok(Vec::new());
    }
    if channel.starts_with("trade") {
        let trades = parse_trades(payload)?;
        return Ok(trades.into_iter().map(MarketEvent::Trade).collect());
    }

    Ok(Vec::new())
}

fn parse_book_snapshot(
    payload: &Value,
    book_state: &mut LocalBookState,
) -> Result<OrderBookSnapshot> {
    let data = extract_first_data_item(payload)?;
    let instrument_name = payload
        .get("instrument_name")
        .or_else(|| data.get("instrument_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();

    let timestamp_ms = data
        .get("t")
        .or_else(|| data.get("create_time"))
        .or_else(|| payload.get("t"))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("missing order book timestamp for {instrument_name}"))?;

    let bids = parse_levels(
        data.get("bids")
            .ok_or_else(|| anyhow!("missing bids for {instrument_name}"))?,
    )?;
    let asks = parse_levels(
        data.get("asks")
            .ok_or_else(|| anyhow!("missing asks for {instrument_name}"))?,
    )?;

    book_state.bids.clear();
    book_state.asks.clear();
    for level in bids {
        upsert_level(&mut book_state.bids, level.price, level.size);
    }
    for level in asks {
        upsert_level(&mut book_state.asks, level.price, level.size);
    }
    book_state.last_u = extract_sequence(payload, data, "u");

    build_snapshot_from_state(book_state, timestamp_ms, instrument_name)
}

fn apply_book_update(
    payload: &Value,
    book_state: &mut LocalBookState,
) -> Result<Option<OrderBookSnapshot>> {
    if book_state.last_u.is_none() {
        return Ok(None);
    }

    let data = extract_first_data_item(payload)?;
    let update = data
        .get("update")
        .ok_or_else(|| anyhow!("missing delta update payload"))?;
    let current_u =
        extract_sequence(payload, data, "u").ok_or_else(|| anyhow!("missing delta sequence u"))?;
    let previous_u = extract_sequence(payload, data, "pu")
        .ok_or_else(|| anyhow!("missing delta sequence pu"))?;

    if Some(previous_u) != book_state.last_u {
        let expected_previous_u = book_state.last_u;
        book_state.last_u = None;
        return Err(anyhow!(
            "book delta sequence mismatch: expected pu {:?}, got {previous_u}",
            expected_previous_u
        ));
    }

    if let Some(bids) = update.get("bids") {
        for level in parse_levels(bids)? {
            if level.size <= Decimal::ZERO {
                book_state.bids.remove(&level.price);
            } else {
                upsert_level(&mut book_state.bids, level.price, level.size);
            }
        }
    }

    if let Some(asks) = update.get("asks") {
        for level in parse_levels(asks)? {
            if level.size <= Decimal::ZERO {
                book_state.asks.remove(&level.price);
            } else {
                upsert_level(&mut book_state.asks, level.price, level.size);
            }
        }
    }

    book_state.last_u = Some(current_u);
    let timestamp_ms =
        extract_sequence(payload, data, "t").ok_or_else(|| anyhow!("missing delta timestamp t"))?;
    let instrument_name = payload
        .get("instrument_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    build_snapshot_from_state(book_state, timestamp_ms, instrument_name).map(Some)
}

fn build_snapshot_from_state(
    book_state: &LocalBookState,
    timestamp_ms: i64,
    instrument_name: &str,
) -> Result<OrderBookSnapshot> {
    let best_bid = book_state
        .bids
        .iter()
        .next_back()
        .map(|(price, _)| *price)
        .ok_or_else(|| anyhow!("missing best bid for {instrument_name}"))?;
    let best_ask = book_state
        .asks
        .iter()
        .next()
        .map(|(price, _)| *price)
        .ok_or_else(|| anyhow!("missing best ask for {instrument_name}"))?;
    let mid_price = (best_bid + best_ask) / Decimal::from(2);

    OrderBookSnapshot::from_price_levels(
        timestamp_ms as f64 / 1000.0,
        mid_price,
        best_bid,
        best_ask,
        book_state
            .bids
            .iter()
            .rev()
            .map(|(price, size)| {
                vec![
                    price.to_string().parse::<f64>().unwrap_or_default(),
                    size.to_string().parse::<f64>().unwrap_or_default(),
                ]
            })
            .collect(),
        book_state
            .asks
            .iter()
            .map(|(price, size)| {
                vec![
                    price.to_string().parse::<f64>().unwrap_or_default(),
                    size.to_string().parse::<f64>().unwrap_or_default(),
                ]
            })
            .collect(),
    )
    .map_err(|err| anyhow!("failed to build order book snapshot: {err}"))
}

fn parse_trades(payload: &Value) -> Result<Vec<PublicTrade>> {
    let instrument_name = payload
        .get("instrument_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let items = payload
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing trade data array"))?;

    let mut trades = Vec::with_capacity(items.len());
    for item in items {
        let side = item
            .get("side")
            .or_else(|| item.get("taker_side"))
            .or_else(|| item.get("s"))
            .or_else(|| item.get("takerSide"))
            .map(value_to_string)
            .map(|side| normalize_trade_side(&side))
            .unwrap_or_default();
        let price = parse_f64_field(item, &["price", "traded_price", "p"])?;
        let amount = parse_f64_field(item, &["amount", "qty", "quantity", "traded_quantity", "q"])?;
        let timestamp = item
            .get("create_time")
            .or_else(|| item.get("t"))
            .or_else(|| item.get("time"))
            .and_then(Value::as_i64)
            .map(|ts| ts as f64 / 1000.0)
            .unwrap_or_default();
        let trade_id = item
            .get("trade_id")
            .or_else(|| item.get("d"))
            .map(value_to_string);

        trades.push(PublicTrade {
            instrument_name: item
                .get("instrument_name")
                .and_then(Value::as_str)
                .unwrap_or(&instrument_name)
                .to_string(),
            side,
            price,
            amount,
            timestamp,
            trade_id,
        });
    }

    Ok(trades)
}

fn extract_first_data_item<'a>(payload: &'a Value) -> Result<&'a Value> {
    match payload.get("data") {
        Some(Value::Array(items)) => items
            .first()
            .ok_or_else(|| anyhow!("websocket data array was empty")),
        Some(value) if value.is_object() => Ok(value),
        _ => Err(anyhow!("missing websocket data payload")),
    }
}

#[derive(Debug, Clone, Copy)]
struct ParsedLevel {
    price: Decimal,
    size: Decimal,
}

fn parse_levels(value: &Value) -> Result<Vec<ParsedLevel>> {
    let levels = value
        .as_array()
        .ok_or_else(|| anyhow!("price levels must be an array"))?;
    let mut parsed = Vec::with_capacity(levels.len());
    for level in levels {
        let entry = level
            .as_array()
            .ok_or_else(|| anyhow!("price level entry must be an array"))?;
        if entry.len() < 2 {
            return Err(anyhow!("price level entry must contain price and size"));
        }
        let price = value_to_decimal(&entry[0]).context("invalid price level price")?;
        let size = value_to_decimal(&entry[1]).context("invalid price level size")?;
        parsed.push(ParsedLevel { price, size });
    }
    Ok(parsed)
}

fn parse_f64_field(value: &Value, keys: &[&str]) -> Result<f64> {
    for key in keys {
        if let Some(candidate) = value.get(*key) {
            return value_to_f64(candidate).with_context(|| format!("invalid numeric field {key}"));
        }
    }
    Err(anyhow!("missing numeric field"))
}

fn value_to_f64(value: &Value) -> Result<f64> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .ok_or_else(|| anyhow!("numeric value was not representable as f64")),
        Value::String(text) => text
            .parse::<f64>()
            .with_context(|| format!("failed to parse numeric string {text}")),
        _ => Err(anyhow!("expected number or numeric string")),
    }
}

fn value_to_decimal(value: &Value) -> Result<Decimal> {
    match value {
        Value::Number(number) => {
            let as_f64 = number
                .as_f64()
                .ok_or_else(|| anyhow!("numeric value was not representable as f64"))?;
            Decimal::from_f64_retain(as_f64)
                .ok_or_else(|| anyhow!("failed to convert numeric value to Decimal"))
        }
        Value::String(text) => text
            .parse::<Decimal>()
            .with_context(|| format!("failed to parse decimal string {text}")),
        _ => Err(anyhow!("expected number or numeric string")),
    }
}

fn extract_sequence(payload: &Value, data: &Value, field: &str) -> Option<i64> {
    data.get(field)
        .or_else(|| payload.get(field))
        .and_then(Value::as_i64)
}

fn upsert_level(book: &mut BTreeMap<Decimal, Decimal>, price: Decimal, size: Decimal) {
    if size <= Decimal::ZERO {
        book.remove(&price);
    } else {
        book.insert(price, size);
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn normalize_trade_side(side: &str) -> String {
    match side.trim().to_ascii_uppercase().as_str() {
        "BUY" | "B" => "BUY".to_string(),
        "SELL" | "S" => "SELL".to_string(),
        other => other.to_string(),
    }
}

impl Default for WebSocketClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_book_message_into_snapshot() {
        let value: Value = serde_json::from_str(
            r#"{
                "id": -1,
                "method": "subscribe",
                "result": {
                    "instrument_name": "ETH_USDT",
                    "subscription": "book.ETH_USDT.10",
                    "channel": "book",
                    "data": [{
                        "bids": [["100.0", "2.0"], ["99.5", "3.0"]],
                        "asks": [["100.5", "1.0"], ["101.0", "4.0"]],
                        "t": 1710000000123
                    }]
                }
            }"#,
        )
        .unwrap();

        let mut book_state = LocalBookState::default();
        let events = parse_market_events(&value, &mut book_state).unwrap();
        match &events[0] {
            MarketEvent::Book(snapshot) => {
                assert_eq!(snapshot.best_bid.to_string(), "100.0");
                assert_eq!(snapshot.best_ask.to_string(), "100.5");
                assert_eq!(snapshot.timestamp, 1710000000.123);
            }
            _ => panic!("expected book event"),
        }
    }

    #[test]
    fn parses_trade_message_into_events() {
        let value: Value = serde_json::from_str(
            r#"{
                "result": {
                    "instrument_name": "ETH_USDT",
                    "channel": "trade",
                    "data": [{
                        "side": "BUY",
                        "price": "100.5",
                        "amount": "1.25",
                        "trade_id": "123",
                        "create_time": 1710000000456
                    }]
                }
            }"#,
        )
        .unwrap();

        let mut book_state = LocalBookState::default();
        let events = parse_market_events(&value, &mut book_state).unwrap();
        match &events[0] {
            MarketEvent::Trade(trade) => {
                assert_eq!(trade.instrument_name, "ETH_USDT");
                assert_eq!(trade.side, "BUY");
                assert_eq!(trade.price, 100.5);
                assert_eq!(trade.amount, 1.25);
                assert_eq!(trade.trade_id.as_deref(), Some("123"));
            }
            _ => panic!("expected trade event"),
        }
    }

    #[test]
    fn parses_trade_message_with_abbreviated_side_field() {
        let value: Value = serde_json::from_str(
            r#"{
                "result": {
                    "instrument_name": "ETH_USDT",
                    "channel": "trade",
                    "data": [{
                        "s": "b",
                        "p": "100.5",
                        "q": "1.25",
                        "d": "123",
                        "t": 1710000000456
                    }]
                }
            }"#,
        )
        .unwrap();

        let mut book_state = LocalBookState::default();
        let events = parse_market_events(&value, &mut book_state).unwrap();
        match &events[0] {
            MarketEvent::Trade(trade) => {
                assert_eq!(trade.instrument_name, "ETH_USDT");
                assert_eq!(trade.side, "BUY");
                assert_eq!(trade.price, 100.5);
                assert_eq!(trade.amount, 1.25);
                assert_eq!(trade.trade_id.as_deref(), Some("123"));
            }
            _ => panic!("expected trade event"),
        }
    }

    #[test]
    fn applies_book_delta_updates() {
        let snapshot_value: Value = serde_json::from_str(
            r#"{
                "id": -1,
                "method": "subscribe",
                "code": 0,
                "result": {
                    "instrument_name": "ETH_USDT",
                    "subscription": "book.ETH_USDT.10",
                    "channel": "book",
                    "data": [{
                        "bids": [["100.0", "2.0", "1"]],
                        "asks": [["100.5", "1.0", "1"]],
                        "t": 1710000000123,
                        "u": 10
                    }]
                }
            }"#,
        )
        .unwrap();

        let delta_value: Value = serde_json::from_str(
            r#"{
                "id": -1,
                "method": "subscribe",
                "code": 0,
                "result": {
                    "instrument_name": "ETH_USDT",
                    "subscription": "book.ETH_USDT.10",
                    "channel": "book.update",
                    "data": [{
                        "update": {
                            "bids": [["100.0", "3.5", "1"]],
                            "asks": [["100.5", "0", "0"], ["101.0", "2.0", "1"]]
                        },
                        "t": 1710000001123,
                        "u": 11,
                        "pu": 10
                    }]
                }
            }"#,
        )
        .unwrap();

        let mut book_state = LocalBookState::default();
        let snapshot_events = parse_market_events(&snapshot_value, &mut book_state).unwrap();
        assert_eq!(snapshot_events.len(), 1);

        let delta_events = parse_market_events(&delta_value, &mut book_state).unwrap();
        match &delta_events[0] {
            MarketEvent::Book(snapshot) => {
                assert_eq!(snapshot.best_bid.to_string(), "100.0");
                assert_eq!(snapshot.best_ask.to_string(), "101.0");
            }
            _ => panic!("expected updated book event"),
        }
    }
}
