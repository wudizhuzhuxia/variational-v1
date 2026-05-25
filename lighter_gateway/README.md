# Lighter Gateway

Rust sidecar for the low-latency Lighter order path.

The Python strategy connects to this gateway over a local WebSocket. The gateway:

- keeps Lighter WebSocket submission hot,
- signs create-order transactions with Rust signer code,
- submits `jsonapi/sendtx`,
- returns timing fields for replay and latency analysis.

## Signer Dependency

The gateway uses vendored Rust signer crates copied from:

https://github.com/your-quantguy/lighter-rust

They live in `vendor/lighter-rust` so `cargo check` and `cargo run` do not depend on GitHub availability during trading.

## Run

```bash
cargo run --manifest-path lighter_gateway/Cargo.toml --release
```

Required environment variables:

- `LIGHTER_PRIVATE_KEY` or `API_KEY_PRIVATE_KEY`
- `LIGHTER_ACCOUNT_INDEX`
- `LIGHTER_API_KEY_INDEX`

Optional:

- `LIGHTER_BASE_URL`, default `https://mainnet.zklighter.elliot.ai`
- `LIGHTER_WS_URL`, default derived from base URL as `wss://.../stream`
- `LIGHTER_GATEWAY_BIND`, default `127.0.0.1:8771`
- `LIGHTER_CHAIN_ID`, default `304`
