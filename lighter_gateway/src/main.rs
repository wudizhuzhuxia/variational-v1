use anyhow::{anyhow, Context, Result};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use signer::KeyManager;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::{
    accept_async,
    connect_async,
    tungstenite::Message,
    MaybeTlsStream,
    WebSocketStream,
};

const ORDER_TYPE_LIMIT: u8 = 0;
const TIF_GOOD_TILL_TIME: u8 = 1;
const TX_TYPE_CREATE_ORDER: u32 = 14;

#[derive(Clone)]
struct GatewayConfig {
    bind: String,
    base_url: String,
    ws_url: String,
    account_index: i64,
    api_key_index: u8,
    chain_id: u32,
}

#[derive(Clone)]
struct GatewayState {
    config: GatewayConfig,
    client: Client,
    key_manager: Arc<KeyManager>,
    nonce: Arc<AtomicI64>,
    lighter_ws: Arc<Mutex<Option<LighterWs>>>,
}

type LighterWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Deserialize)]
struct PlaceOrderRequest {
    #[serde(alias = "requestId")]
    request_id: String,
    signal_id: Option<String>,
    market_index: u8,
    client_order_index: Option<u64>,
    base_amount: i64,
    price: i64,
    is_ask: bool,
    reduce_only: Option<bool>,
    order_expiry_ms: Option<i64>,
    dry_run: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PlaceOrderResponse {
    #[serde(rename = "type")]
    msg_type: &'static str,
    #[serde(rename = "requestId")]
    request_id: String,
    request_id_snake: String,
    signal_id: Option<String>,
    ok: bool,
    dry_run: bool,
    error: Option<String>,
    tx_type: u32,
    tx_info: Option<Value>,
    send_response: Option<Value>,
    client_order_index: u64,
    nonce: i64,
    timings: TimingReport,
    timestamp: String,
}

#[derive(Debug, Default, Serialize)]
struct TimingReport {
    gateway_received_ns: u128,
    nonce_ms: f64,
    build_ms: f64,
    sign_ms: f64,
    ws_connect_ms: f64,
    ws_send_ms: f64,
    ws_wait_ms: f64,
    total_ms: f64,
}

fn now_ms() -> Result<i64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

fn now_iso() -> String {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => format!("{}.{:03}Z", d.as_secs(), d.subsec_millis()),
        Err(_) => "0.000Z".to_string(),
    }
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).unwrap_or_default();
    if value.trim().is_empty() {
        return Err(anyhow!("{name} is not set"));
    }
    Ok(value.trim().to_string())
}

impl GatewayState {
    async fn refresh_nonce(&self) -> Result<i64> {
        let url = format!(
            "{}/api/v1/nextNonce?account_index={}&api_key_index={}",
            self.config.base_url, self.config.account_index, self.config.api_key_index
        );
        let value: Value = self.client.get(url).send().await?.json().await?;
        let nonce = value["nonce"]
            .as_i64()
            .ok_or_else(|| anyhow!("Invalid nextNonce response: {value}"))?;
        self.nonce.store(nonce - 1, Ordering::SeqCst);
        Ok(nonce)
    }

    async fn next_nonce(&self) -> Result<i64> {
        let current = self.nonce.load(Ordering::SeqCst);
        if current >= 0 {
            return Ok(self.nonce.fetch_add(1, Ordering::SeqCst) + 1);
        }
        self.refresh_nonce().await?;
        Ok(self.nonce.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn sign_transaction(&self, tx_json: &str) -> Result<[u8; 80]> {
        let tx_value: Value = serde_json::from_str(tx_json)?;
        let nonce = tx_value["Nonce"].as_i64().unwrap_or(0);
        let expired_at = tx_value["ExpiredAt"].as_i64().unwrap_or(0);
        let account_index = tx_value["AccountIndex"].as_i64().unwrap_or(0);
        let api_key_index = tx_value["ApiKeyIndex"].as_u64().unwrap_or(0) as u32;
        let market_index = tx_value["MarketIndex"].as_u64().unwrap_or(0) as u32;
        let client_order_index = tx_value["ClientOrderIndex"].as_i64().unwrap_or(0);
        let base_amount = tx_value["BaseAmount"].as_i64().unwrap_or(0);
        let price = tx_value["Price"].as_i64().unwrap_or(0) as u32;
        let is_ask = tx_value["IsAsk"].as_i64().unwrap_or(0) as u32;
        let order_type = tx_value["Type"].as_i64().unwrap_or(0) as u32;
        let time_in_force = tx_value["TimeInForce"].as_i64().unwrap_or(0) as u32;
        let reduce_only = tx_value["ReduceOnly"].as_i64().unwrap_or(0) as u32;
        let trigger_price = tx_value["TriggerPrice"].as_i64().unwrap_or(0) as u32;
        let order_expiry = tx_value["OrderExpiry"].as_i64().unwrap_or(0);

        use poseidon_hash::Goldilocks;
        let to_goldi_i64 = |val: i64| Goldilocks::from_i64(val);
        let elements = vec![
            Goldilocks::from_canonical_u64(self.config.chain_id as u64),
            Goldilocks::from_canonical_u64(TX_TYPE_CREATE_ORDER as u64),
            to_goldi_i64(nonce),
            to_goldi_i64(expired_at),
            to_goldi_i64(account_index),
            Goldilocks::from_canonical_u64(api_key_index as u64),
            Goldilocks::from_canonical_u64(market_index as u64),
            to_goldi_i64(client_order_index),
            to_goldi_i64(base_amount),
            Goldilocks::from_canonical_u64(price as u64),
            Goldilocks::from_canonical_u64(is_ask as u64),
            Goldilocks::from_canonical_u64(order_type as u64),
            Goldilocks::from_canonical_u64(time_in_force as u64),
            Goldilocks::from_canonical_u64(reduce_only as u64),
            Goldilocks::from_canonical_u64(trigger_price as u64),
            to_goldi_i64(order_expiry),
        ];
        let hash = poseidon_hash::hash_to_quintic_extension(&elements);
        let message = hash.to_bytes_le();
        self.key_manager.sign(&message).map_err(Into::into)
    }

    async fn ensure_lighter_ws(&self) -> Result<f64> {
        let start = Instant::now();
        let mut guard = self.lighter_ws.lock().await;
        if guard.is_some() {
            return Ok(0.0);
        }
        let (stream, _) = connect_async(&self.config.ws_url).await?;
        *guard = Some(stream);
        Ok(elapsed_ms(start))
    }

    async fn send_lighter_ws(&self, request_id: &str, tx_info: &Value) -> Result<(f64, f64, Option<Value>)> {
        let connect_ms = self.ensure_lighter_ws().await?;
        let payload = json!({
            "type": "jsonapi/sendtx",
            "data": {
                "tx_type": TX_TYPE_CREATE_ORDER,
                "tx_info": serde_json::to_string(tx_info)?,
                "price_protection": true
            }
        });
        let start = Instant::now();
        let mut guard = self.lighter_ws.lock().await;
        let Some(ws) = guard.as_mut() else {
            return Err(anyhow!("Lighter websocket is not connected"));
        };
        if let Err(err) = ws.send(Message::Text(payload.to_string())).await {
            *guard = None;
            return Err(err.into());
        }
        let send_ms = elapsed_ms(start);

        let wait_start = Instant::now();
        let ack = match tokio::time::timeout(Duration::from_millis(750), ws.next()).await {
            Ok(Some(Ok(message))) => parse_ws_message(message).ok(),
            Ok(Some(Err(err))) => {
                *guard = None;
                return Err(err.into());
            }
            _ => None,
        };
        Ok((
            connect_ms,
            send_ms,
            Some(json!({
                "request_id": request_id,
                "ack": ack,
                "ack_wait_ms": elapsed_ms(wait_start)
            })),
        ))
    }
}

fn parse_ws_message(message: Message) -> Result<Value> {
    match message {
        Message::Text(text) => serde_json::from_str(&text).map_err(Into::into),
        Message::Binary(bytes) => serde_json::from_slice(&bytes).map_err(Into::into),
        Message::Ping(_) | Message::Pong(_) => Ok(json!({"control": true})),
        Message::Close(frame) => Ok(json!({"closed": true, "frame": format!("{frame:?}")})),
        Message::Frame(_) => Ok(json!({"frame": true})),
    }
}

async fn handle_order(state: GatewayState, request: PlaceOrderRequest) -> PlaceOrderResponse {
    let total_start = Instant::now();
    let received_ns = total_start.elapsed().as_nanos();
    let dry_run = request.dry_run.unwrap_or(true);
    let mut timings = TimingReport {
        gateway_received_ns: received_ns,
        ..TimingReport::default()
    };
    let client_order_index = request.client_order_index.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    });

    let nonce_start = Instant::now();
    let nonce = match state.next_nonce().await {
        Ok(value) => value,
        Err(err) => {
            timings.nonce_ms = elapsed_ms(nonce_start);
            timings.total_ms = elapsed_ms(total_start);
            return error_response(request, client_order_index, -1, timings, err);
        }
    };
    timings.nonce_ms = elapsed_ms(nonce_start);

    let build_start = Instant::now();
    let now = match now_ms() {
        Ok(value) => value,
        Err(err) => {
            timings.total_ms = elapsed_ms(total_start);
            return error_response(request, client_order_index, nonce, timings, err.into());
        }
    };
    let order_expiry = request.order_expiry_ms.unwrap_or(now + (28 * 24 * 60 * 60 * 1000));
    let mut tx_info = json!({
        "AccountIndex": state.config.account_index,
        "ApiKeyIndex": state.config.api_key_index,
        "MarketIndex": request.market_index,
        "ClientOrderIndex": client_order_index,
        "BaseAmount": request.base_amount,
        "Price": request.price,
        "IsAsk": if request.is_ask { 1 } else { 0 },
        "Type": ORDER_TYPE_LIMIT,
        "TimeInForce": TIF_GOOD_TILL_TIME,
        "ReduceOnly": if request.reduce_only.unwrap_or(false) { 1 } else { 0 },
        "TriggerPrice": 0,
        "OrderExpiry": order_expiry,
        "ExpiredAt": now + 599_000,
        "Nonce": nonce,
        "Sig": ""
    });
    timings.build_ms = elapsed_ms(build_start);

    let sign_start = Instant::now();
    let signature = match state.sign_transaction(&tx_info.to_string()) {
        Ok(value) => value,
        Err(err) => {
            timings.sign_ms = elapsed_ms(sign_start);
            timings.total_ms = elapsed_ms(total_start);
            return error_response(request, client_order_index, nonce, timings, err);
        }
    };
    tx_info["Sig"] = json!(base64::engine::general_purpose::STANDARD.encode(signature));
    timings.sign_ms = elapsed_ms(sign_start);

    let (send_response, error) = if dry_run {
        (Some(json!({"dry_run": true, "code": 200})), None)
    } else {
        match state.send_lighter_ws(&request.request_id, &tx_info).await {
            Ok((connect_ms, send_ms, response)) => {
                timings.ws_connect_ms = connect_ms;
                timings.ws_send_ms = send_ms;
                if let Some(wait_ms) = response
                    .as_ref()
                    .and_then(|value| value.get("ack_wait_ms"))
                    .and_then(Value::as_f64)
                {
                    timings.ws_wait_ms = wait_ms;
                }
                (response, None)
            }
            Err(err) => (None, Some(err.to_string())),
        }
    };
    timings.total_ms = elapsed_ms(total_start);

    PlaceOrderResponse {
        msg_type: "ORDER_RESULT",
        request_id_snake: request.request_id.clone(),
        request_id: request.request_id,
        signal_id: request.signal_id,
        ok: error.is_none(),
        dry_run,
        error,
        tx_type: TX_TYPE_CREATE_ORDER,
        tx_info: Some(tx_info),
        send_response,
        client_order_index,
        nonce,
        timings,
        timestamp: now_iso(),
    }
}

fn error_response(
    request: PlaceOrderRequest,
    client_order_index: u64,
    nonce: i64,
    timings: TimingReport,
    err: anyhow::Error,
) -> PlaceOrderResponse {
    PlaceOrderResponse {
        msg_type: "ORDER_RESULT",
        request_id_snake: request.request_id.clone(),
        request_id: request.request_id,
        signal_id: request.signal_id,
        ok: false,
        dry_run: request.dry_run.unwrap_or(true),
        error: Some(err.to_string()),
        tx_type: TX_TYPE_CREATE_ORDER,
        tx_info: None,
        send_response: None,
        client_order_index,
        nonce,
        timings,
        timestamp: now_iso(),
    }
}

async fn handle_client(stream: TcpStream, state: GatewayState) -> Result<()> {
    let mut ws = accept_async(stream).await?;
    while let Some(message) = ws.next().await {
        let message = message?;
        if !message.is_text() {
            continue;
        }
        let payload: Value = serde_json::from_str(message.to_text()?)?;
        let msg_type = payload["type"].as_str().unwrap_or("").to_uppercase();
        if msg_type == "REGISTER" {
            ws.send(Message::Text(json!({
                "type": "REGISTER_ACK",
                "ok": true,
                "role": payload["role"].as_str().unwrap_or("unknown"),
                "timestamp": now_iso(),
            }).to_string())).await?;
            continue;
        }
        if msg_type == "PING" {
            ws.send(Message::Text(json!({"type": "PONG", "timestamp": now_iso()}).to_string())).await?;
            continue;
        }
        if msg_type != "PLACE_ORDER" {
            ws.send(Message::Text(json!({
                "type": "ERROR",
                "ok": false,
                "error": format!("Unsupported message type: {msg_type}"),
                "timestamp": now_iso(),
            }).to_string())).await?;
            continue;
        }
        let request: PlaceOrderRequest = serde_json::from_value(payload)?;
        let response = handle_order(state.clone(), request).await;
        ws.send(Message::Text(serde_json::to_string(&response)?)).await?;
    }
    Ok(())
}

fn load_config() -> Result<GatewayConfig> {
    let base_url = env::var("LIGHTER_BASE_URL")
        .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".to_string());
    let ws_url = env::var("LIGHTER_WS_URL")
        .unwrap_or_else(|_| "wss://mainnet.zklighter.elliot.ai/stream".to_string());
    Ok(GatewayConfig {
        bind: env::var("LIGHTER_GATEWAY_BIND").unwrap_or_else(|_| "127.0.0.1:8771".to_string()),
        base_url,
        ws_url,
        account_index: required_env("LIGHTER_ACCOUNT_INDEX")?.parse()?,
        api_key_index: required_env("LIGHTER_API_KEY_INDEX")?.parse()?,
        chain_id: env::var("LIGHTER_CHAIN_ID").ok().and_then(|v| v.parse().ok()).unwrap_or(304),
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let private_key = env::var("API_KEY_PRIVATE_KEY")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or(required_env("LIGHTER_PRIVATE_KEY")?);
    let key_manager = Arc::new(KeyManager::from_hex(private_key.trim()).context("invalid Lighter private key")?);
    let state = GatewayState {
        config: config.clone(),
        client: Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .tcp_keepalive(Duration::from_secs(60))
            .build()?,
        key_manager,
        nonce: Arc::new(AtomicI64::new(-1)),
        lighter_ws: Arc::new(Mutex::new(None)),
    };
    let listener = TcpListener::bind(&config.bind).await?;
    println!("lighter_gateway listening on ws://{}", config.bind);
    loop {
        let (stream, _) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, state).await {
                eprintln!("gateway client error: {err:#}");
            }
        });
    }
}
