import argparse
import asyncio
import contextlib
import csv
import json
import logging
import os
import signal
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

import requests
import websockets
from dotenv import load_dotenv
from lighter.signer_client import SignerClient
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from variational.listener import (
    HEARTBEAT_STALE_SECONDS,
    CommandBroker,
    EventSink,
    VariationalMonitor,
    run_command_server,
    run_receiver_server,
)
from variational.executors import LocalOrderRequest, RustLighterGatewayClient, VariationalCommandClient

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
FORWARDER_COMMAND_PORT = 8768
LIGHTER_GATEWAY_URL = "ws://127.0.0.1:8771"
LOG_DIR = Path("./log")
OUTPUT_DIR = LOG_DIR
APP_LOG_FILE = LOG_DIR / "runtime.log"
TRADE_RECORDS_CSV_FILE = LOG_DIR / "trade_records.csv"
SIGNAL_SAMPLES_JSONL_FILE = LOG_DIR / "signal_samples.jsonl"
SIGNAL_SNAPSHOTS_JSONL_FILE = LOG_DIR / "signal_snapshots.jsonl"
EXECUTION_EVENTS_JSONL_FILE = LOG_DIR / "execution_events.jsonl"
READY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05
HEDGE_SLIPPAGE_BPS = Decimal("0.3")
ENTRY_OFFSET_PCT_DEFAULT = Decimal("0.008")
MAX_LEVERAGE_DEFAULT = Decimal("2")
DASHBOARD_REFRESH_SECONDS = 1.0
DASHBOARD_ORDERS = 8
SPREAD_HISTORY_SECONDS = 3600.0
ASSET_SWITCH_CONFIRM_TICKS = 3
LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_WS_PING_INTERVAL_SECONDS = 30
LIGHTER_WS_PING_TIMEOUT_SECONDS = 30


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def decimal_to_str(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def monotonic_ns() -> int:
    return time.perf_counter_ns()


def parse_decimal_arg(value: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"Invalid decimal: {value}") from exc


def resolve_variational_ticker(ticker: str) -> str:
    return VARIATIONAL_TICKER_OVERRIDES.get(ticker.upper(), ticker.upper())


def resolve_lighter_ticker(variational_asset: str) -> str:
    asset = variational_asset.upper()
    return VARIATIONAL_ASSET_TO_LIGHTER_TICKER.get(asset, asset)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def required_int_env(name: str) -> int:
    value = required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def spread_value(aggressive_buy_ask: Decimal | None, aggressive_sell_bid: Decimal | None) -> Decimal | None:
    if aggressive_buy_ask is None or aggressive_sell_bid is None:
        return None
    return aggressive_sell_bid - aggressive_buy_ask


def spread_percent(diff: Decimal | None, denominator: Decimal | None) -> Decimal | None:
    if diff is None or denominator is None or denominator == 0:
        return None
    return (diff / denominator) * Decimal("100")


def book_spread_percent(bid: Decimal | None, ask: Decimal | None) -> Decimal | None:
    if bid is None or ask is None:
        return None
    mid = (bid + ask) / Decimal("2")
    if mid == 0:
        return None
    return ((ask - bid) / mid) * Decimal("100")


def normalize_variational_status(status: str) -> str:
    lowered = status.strip().lower()
    if lowered == "confirmed":
        return "filled"
    return lowered


@dataclass(slots=True)
class OrderLifecycle:
    trade_key: str
    trade_id: str
    side: str
    qty: Decimal
    asset: str
    auto_hedge_enabled: bool
    last_variational_status: str
    notional_usd: Decimal | None = None

    var_fill_price: Decimal | None = None
    var_fill_ts_iso: str | None = None

    lighter_side: str | None = None
    lighter_client_order_id: int | None = None
    lighter_fill_price: Decimal | None = None
    lighter_fill_ts_iso: str | None = None
    lighter_tx_hash: str | None = None
    hedge_error: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "trade_key": self.trade_key,
            "trade_id": self.trade_id,
            "side": self.side,
            "qty": decimal_to_str(self.qty),
            "asset": self.asset,
            "notional_usd": decimal_to_str(self.notional_usd),
            "variational_filled_price": decimal_to_str(self.var_fill_price),
            "variational_filled_at": self.var_fill_ts_iso,
            "lighter_order_side": self.lighter_side,
            "lighter_client_order_id": self.lighter_client_order_id,
            "lighter_filled_price": decimal_to_str(self.lighter_fill_price),
            "lighter_filled_at": self.lighter_fill_ts_iso,
            "auto_hedge_enabled": self.auto_hedge_enabled,
            "hedge_error": self.hedge_error,
            "last_variational_status": self.last_variational_status,
        }


@dataclass(slots=True)
class SignalSnapshot:
    signal_id: str
    direction: str
    var_side: str
    lighter_side: str
    notional_usd: Decimal | None
    base_qty: Decimal
    var_bid: Decimal
    var_ask: Decimal
    lighter_bid: Decimal
    lighter_ask: Decimal
    var_book_spread_pct: Decimal | None
    lighter_book_spread_pct: Decimal | None
    long_current_pct: Decimal | None
    short_current_pct: Decimal | None
    long_median_30m_pct: float | None
    long_median_1h_pct: float | None
    short_median_30m_pct: float | None
    short_median_1h_pct: float | None
    entry_offset_pct: Decimal
    open_single_side_notional_usd: Decimal
    max_single_side_notional_usd: Decimal | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "direction": self.direction,
            "var_side": self.var_side,
            "lighter_side": self.lighter_side,
            "notional_usd": decimal_to_str(self.notional_usd),
            "base_qty": decimal_to_str(self.base_qty),
            "var_bid": decimal_to_str(self.var_bid),
            "var_ask": decimal_to_str(self.var_ask),
            "lighter_bid": decimal_to_str(self.lighter_bid),
            "lighter_ask": decimal_to_str(self.lighter_ask),
            "var_book_spread_pct": decimal_to_str(self.var_book_spread_pct),
            "lighter_book_spread_pct": decimal_to_str(self.lighter_book_spread_pct),
            "long_current_pct": decimal_to_str(self.long_current_pct),
            "short_current_pct": decimal_to_str(self.short_current_pct),
            "long_median_30m_pct": self.long_median_30m_pct,
            "long_median_1h_pct": self.long_median_1h_pct,
            "short_median_30m_pct": self.short_median_30m_pct,
            "short_median_1h_pct": self.short_median_1h_pct,
            "entry_offset_pct": decimal_to_str(self.entry_offset_pct),
            "open_single_side_notional_usd": decimal_to_str(self.open_single_side_notional_usd),
            "max_single_side_notional_usd": decimal_to_str(self.max_single_side_notional_usd),
        }


class VariationalRuntime:
    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        command_port: int,
        output_dir: Path | None,
        quiet: bool,
    ) -> None:
        self.monitor = VariationalMonitor(trade_limit=500, snapshot_file=None)
        self.sink = EventSink(output_dir=output_dir, quiet=quiet, monitor=self.monitor)
        self.command_broker = CommandBroker(quiet=quiet)
        self.host = host
        self.ws_port = ws_port
        self.rest_port = rest_port
        self.command_port = command_port
        self.ws_server = None
        self.rest_server = None
        self.command_server = None

    async def start(self) -> None:
        self.ws_server = await run_receiver_server("ws", self.host, self.ws_port, self.sink)
        self.rest_server = await run_receiver_server("rest", self.host, self.rest_port, self.sink)
        self.command_server = await run_command_server(self.host, self.command_port, self.command_broker)

    async def stop(self) -> None:
        if self.ws_server is not None:
            self.ws_server.close()
            await self.ws_server.wait_closed()
        if self.rest_server is not None:
            self.rest_server.close()
            await self.rest_server.wait_closed()
        if self.command_server is not None:
            self.command_server.close()
            await self.command_server.wait_closed()


class VariationalToLighterRuntime:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.ticker: str | None = None
        self.variational_ticker: str | None = None
        self.accepted_assets: set[str] = set()

        self.stop_flag = False
        self.logger = logging.getLogger("var_lighter_runtime")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.propagate = False

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(APP_LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        self.logger.addHandler(file_handler)
        self.dashboard_console = Console()

        output_dir = OUTPUT_DIR.expanduser().resolve()
        self.runtime = VariationalRuntime(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            command_port=self.args.command_port,
            output_dir=None,
            quiet=True,
        )

        self.orders_file = output_dir / "order_metrics.jsonl" if output_dir else None
        self.trade_records_csv_file = output_dir / TRADE_RECORDS_CSV_FILE.name if output_dir else None
        self.signal_samples_file = output_dir / SIGNAL_SAMPLES_JSONL_FILE.name if output_dir else None
        self.signal_snapshots_file = output_dir / SIGNAL_SNAPSHOTS_JSONL_FILE.name if output_dir else None
        self.execution_events_file = output_dir / EXECUTION_EVENTS_JSONL_FILE.name if output_dir else None
        self._order_write_lock = asyncio.Lock()
        self._trade_csv_write_lock = asyncio.Lock()
        self._signal_sample_write_lock = asyncio.Lock()
        self._signal_snapshot_write_lock = asyncio.Lock()
        self._execution_event_write_lock = asyncio.Lock()
        self._trade_records_snapshot_sig: str | None = None

        self.records: dict[str, OrderLifecycle] = {}
        self.record_order: deque[str] = deque(maxlen=500)
        self.lighter_client_order_to_trade_key: dict[int, str] = {}
        self.signal_to_record_key: dict[str, str] = {}
        self.signal_snapshots: dict[str, SignalSnapshot] = {}
        self._record_lock = asyncio.Lock()
        self.cross_spread_history: deque[tuple[float, float | None, float | None]] = deque()
        self._asset_switch_lock = asyncio.Lock()
        self._asset_switch_candidate: str | None = None
        self._asset_switch_candidate_hits = 0
        self._last_signal_monotonic = 0.0

        self.trade_event_cursor = 0

        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
        self.api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")
        self.lighter_client: SignerClient | None = None
        self._lighter_signer_lock = asyncio.Lock()
        self.lighter_gateway = RustLighterGatewayClient(self.args.lighter_gateway_url, timeout_ms=self.args.lighter_timeout_ms)
        self.variational_command = VariationalCommandClient(
            f"ws://{FORWARDER_HOST}:{self.args.command_port}",
            timeout_ms=self.args.var_timeout_ms,
        )

        self.lighter_market_index = 0
        self.base_amount_multiplier = 0
        self.price_multiplier = 0

        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid: Decimal | None = None
        self.lighter_best_ask: Decimal | None = None
        self.lighter_order_book_offset = 0
        self.lighter_order_book_ready = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_sequence_gap = False
        self.lighter_order_book_lock = asyncio.Lock()

        self.lighter_ws_task: asyncio.Task[None] | None = None
        self.trade_task: asyncio.Task[None] | None = None
        self.signal_task: asyncio.Task[None] | None = None
        self.dashboard_task: asyncio.Task[None] | None = None

    def print_startup_next_steps(self) -> None:
        is_zh = self.args.lang == "zh"
        if is_zh:
            lines = [
                "Python 脚本已就位，请回到 Chrome 加载并启动扩展。若 Chrome 插件已启动，请刷新网页。",
                f"命令通道：ws://{FORWARDER_HOST}:{self.args.command_port}；自动交易默认 dry-run，实盘需显式加 --live-trading。",
                "Use `python main.py --lang en` for the English dashboard.",
            ]
            title = "启动指引"
        else:
            lines = [
                "Python runtime is ready. Go back to Chrome and load/start the extension.",
                f"Command broker: ws://{FORWARDER_HOST}:{self.args.command_port}; auto-trade is dry-run unless --live-trading is set.",
                "If the Chrome extension has already started, please refresh the webpage."
            ]
            title = "Startup Guide"
        self.dashboard_console.print(Panel("\n".join(lines), title=title, border_style="yellow"))

    def setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum=None, frame=None) -> None:
        self.stop_flag = True

    def initialize_lighter_client(self) -> SignerClient:
        if self.lighter_client is None:
            api_key_private_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip() or required_env("LIGHTER_PRIVATE_KEY")
            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key},
            )
            err = self.lighter_client.check_client()
            if err is not None:
                raise RuntimeError(f"CheckClient error: {err}")
        return self.lighter_client

    def get_lighter_market_config(self) -> tuple[int, int, int]:
        if not self.ticker:
            raise RuntimeError("Ticker is not resolved yet")
        response = requests.get(
            f"{self.lighter_base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        for market in data.get("order_books", []):
            if market.get("symbol") == self.ticker:
                price_decimals = int(market["supported_price_decimals"])
                size_decimals = int(market["supported_size_decimals"])
                return int(market["market_id"]), pow(10, size_decimals), pow(10, price_decimals)

        raise RuntimeError(f"Ticker {self.ticker} not found in Lighter order books")

    async def detect_current_variational_asset(self) -> str | None:
        async with self.runtime.monitor._lock:
            if self.runtime.monitor.current_quote_asset:
                asset = str(self.runtime.monitor.current_quote_asset).strip().upper()
                quote = self.runtime.monitor.quotes.get(asset)
                if (
                    asset
                    and asset != "UNKNOWN"
                    and isinstance(quote, dict)
                    and to_decimal(quote.get("bid")) is not None
                    and to_decimal(quote.get("ask")) is not None
                ):
                    if not self.target_asset_allowed(asset):
                        return None
                    return asset

        return None

    def target_asset_allowed(self, asset: str) -> bool:
        target = str(self.args.target_ticker or "auto").strip().upper()
        if target in {"", "AUTO", "*"}:
            return True
        normalized_asset = asset.strip().upper()
        return normalized_asset in {
            target,
            resolve_variational_ticker(target),
            resolve_lighter_ticker(target),
        }

    async def wait_for_ticker_resolution(self) -> str:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            asset = await self.detect_current_variational_asset()
            if asset:
                return asset
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

        raise RuntimeError("Timed out deriving ticker from Variational quote/trade messages")

    async def _reset_state_for_asset_switch(self) -> None:
        async with self._record_lock:
            self.records.clear()
            self.record_order.clear()
            self.lighter_client_order_to_trade_key.clear()
            self.signal_to_record_key.clear()
            self.signal_snapshots.clear()
        self.cross_spread_history.clear()
        async with self._trade_csv_write_lock:
            self._trade_records_snapshot_sig = None

    async def activate_asset(self, variational_asset: str, reason: str) -> None:
        asset = variational_asset.strip().upper()
        if not asset or asset == "UNKNOWN":
            return

        async with self._asset_switch_lock:
            next_ticker = resolve_lighter_ticker(asset)
            if self.variational_ticker == asset and self.ticker == next_ticker:
                return

            self.variational_ticker = asset
            self.ticker = next_ticker
            self.accepted_assets = {
                asset,
                next_ticker,
                resolve_variational_ticker(next_ticker),
            }

            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier = self.get_lighter_market_config()
            await self.reset_lighter_order_book()
            await self._reset_state_for_asset_switch()

            if self.lighter_ws_task and not self.lighter_ws_task.done():
                self.lighter_ws_task.cancel()
                await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            await self.wait_for_lighter_order_book_ready()
            self.logger.info(
                "Switched market (%s): variational_asset=%s -> lighter_ticker=%s market_id=%s",
                reason,
                self.variational_ticker,
                self.ticker,
                self.lighter_market_index,
            )

    async def wait_for_variational_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            state = await self.runtime.monitor.get_trading_state()
            hb_age = state.get("heartbeat_age")
            if hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS:
                return
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        raise RuntimeError("Timed out waiting for Variational events stream heartbeat")

    async def wait_for_lighter_order_book_ready(self) -> None:
        deadline = time.time() + READY_TIMEOUT_SECONDS
        while not self.stop_flag and time.time() < deadline:
            if self.lighter_order_book_ready:
                return
            await asyncio.sleep(0.2)
        raise RuntimeError("Timed out waiting for Lighter order book")

    async def reset_lighter_order_book(self) -> None:
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_ready = False
            self.lighter_snapshot_loaded = False
            self.lighter_order_book_sequence_gap = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list[Any]) -> None:
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(str(level[0]))
                size = Decimal(str(level[1]))
            elif isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                size = Decimal(str(level.get("size", 0)))
            else:
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        return new_offset > self.lighter_order_book_offset

    async def request_fresh_snapshot(self, ws: Any) -> None:
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_fill_update(self, order: dict[str, Any]) -> None:
        if order.get("status") != "filled":
            return

        client_order_id_raw = order.get("client_order_id")
        try:
            client_order_id = int(client_order_id_raw)
        except Exception:
            return

        fill_price: Decimal | None = None
        filled_quote = to_decimal(order.get("filled_quote_amount"))
        filled_base = to_decimal(order.get("filled_base_amount"))
        if filled_quote is not None and filled_base is not None and filled_base != 0:
            fill_price = filled_quote / filled_base

        now_iso = utc_now()

        async with self._record_lock:
            trade_key = self.lighter_client_order_to_trade_key.get(client_order_id)
            if not trade_key:
                return
            record = self.records.get(trade_key)
            if record is None:
                return
            if record.lighter_fill_ts_iso is not None:
                return

            record.lighter_fill_ts_iso = now_iso
            record.lighter_fill_price = fill_price
            payload = record.to_payload()
            signal_id = trade_key.removeprefix("signal:") if trade_key.startswith("signal:") else None
            snapshot = self.signal_snapshots.get(signal_id) if signal_id else None

        await self.append_order_log("lighter_fill", payload)
        expected_price = None
        slippage_pct = None
        if snapshot is not None and fill_price is not None:
            if snapshot.lighter_side == "BUY":
                expected_price = snapshot.lighter_ask
                if expected_price != 0:
                    slippage_pct = ((fill_price - expected_price) / expected_price) * Decimal("100")
            elif snapshot.lighter_side == "SELL":
                expected_price = snapshot.lighter_bid
                if expected_price != 0:
                    slippage_pct = ((expected_price - fill_price) / expected_price) * Decimal("100")
        await self.append_execution_event(
            "lighter_fill",
            {
                "trade_key": trade_key,
                "signal_id": signal_id,
                "client_order_id": client_order_id,
                "fill_price": decimal_to_str(fill_price),
                "expected_price": decimal_to_str(expected_price),
                "slippage_pct": decimal_to_str(slippage_pct),
                "slippage_bps": decimal_to_str(slippage_pct * Decimal("100") if slippage_pct is not None else None),
                "raw": order,
            },
        )

    def build_lighter_ws_url(self) -> str:
        if env_flag("LIGHTER_WS_SERVER_PINGS"):
            return f"{LIGHTER_WS_URL}?server_pings=true"
        return LIGHTER_WS_URL

    async def handle_lighter_ws(self) -> None:
        while not self.stop_flag:
            try:
                await self.reset_lighter_order_book()
                url = self.build_lighter_ws_url()
                async with websockets.connect(
                    url,
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"
                    try:
                        async with self._lighter_signer_lock:
                            if not self.lighter_client:
                                self.initialize_lighter_client()
                            auth_token, err = self.lighter_client.create_auth_token_with_expiry(
                                api_key_index=self.api_key_index
                            )
                        if err is None:
                            await ws.send(
                                json.dumps(
                                    {
                                        "type": "subscribe",
                                        "channel": account_orders_channel,
                                        "auth": auth_token,
                                    }
                                )
                            )
                        else:
                            self.logger.warning("Failed to create Lighter WS auth token: %s", err)
                    except Exception as exc:
                        self.logger.warning("Error creating Lighter WS auth token: %s", exc)

                    while not self.stop_flag:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        msg_type = data.get("type")

                        if msg_type == "subscribed/order_book":
                            async with self.lighter_order_book_lock:
                                self.lighter_order_book["bids"].clear()
                                self.lighter_order_book["asks"].clear()
                                order_book = data.get("order_book", {})
                                self.lighter_order_book_offset = int(order_book.get("offset", 0) or 0)
                                self.update_lighter_order_book("bids", order_book.get("bids", []))
                                self.update_lighter_order_book("asks", order_book.get("asks", []))
                                self.lighter_snapshot_loaded = True
                                self.lighter_order_book_ready = True
                                self.lighter_best_bid = (
                                    max(self.lighter_order_book["bids"].keys())
                                    if self.lighter_order_book["bids"]
                                    else None
                                )
                                self.lighter_best_ask = (
                                    min(self.lighter_order_book["asks"].keys())
                                    if self.lighter_order_book["asks"]
                                    else None
                                )

                        elif msg_type == "update/order_book" and self.lighter_snapshot_loaded:
                            order_book = data.get("order_book", {})
                            if "offset" not in order_book:
                                continue
                            new_offset = int(order_book["offset"])
                            async with self.lighter_order_book_lock:
                                if not self.validate_order_book_offset(new_offset):
                                    self.lighter_order_book_sequence_gap = True
                                else:
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))
                                    self.lighter_order_book_offset = new_offset
                                    self.lighter_best_bid = (
                                        max(self.lighter_order_book["bids"].keys())
                                        if self.lighter_order_book["bids"]
                                        else None
                                    )
                                    self.lighter_best_ask = (
                                        min(self.lighter_order_book["asks"].keys())
                                        if self.lighter_order_book["asks"]
                                        else None
                                    )

                        elif msg_type == "update/account_orders":
                            orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                            for order in orders:
                                await self.handle_lighter_fill_update(order)

                        if self.lighter_order_book_sequence_gap:
                            await self.request_fresh_snapshot(ws)
                            self.lighter_order_book_sequence_gap = False

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong"}))

            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.logger.warning(
                    "Lighter websocket reconnect after error: %s (url=%s)",
                    exc,
                    self.build_lighter_ws_url(),
                )
                await asyncio.sleep(1)

    async def get_lighter_best_bid_ask(self) -> tuple[Decimal | None, Decimal | None]:
        async with self.lighter_order_book_lock:
            return self.lighter_best_bid, self.lighter_best_ask

    async def get_variational_best_bid_ask(self, preferred_asset: str | None):
        async with self.runtime.monitor._lock:
            quote = None
            if preferred_asset:
                quote = self.runtime.monitor.quotes.get(preferred_asset)
            if quote is None and self.variational_ticker:
                quote = self.runtime.monitor.quotes.get(self.variational_ticker)
            if quote is None and self.runtime.monitor.current_quote_asset:
                quote = self.runtime.monitor.quotes.get(self.runtime.monitor.current_quote_asset)

            if quote is None:
                return None, None, None
            return to_decimal(quote.get("bid")), to_decimal(quote.get("ask")), str(quote.get("asset", ""))

    @staticmethod
    def trade_key(event: dict[str, Any]) -> str:
        trade_id = str(event.get("trade_id", "")).strip()
        if trade_id:
            return f"id:{trade_id}"
        event_seq = str(event.get("event_seq", "")).strip()
        return f"seq:{event_seq}"

    def find_signal_record_for_var_fill(self, side: str, qty: Decimal) -> str | None:
        for key in reversed(self.record_order):
            if not key.startswith("signal:"):
                continue
            record = self.records.get(key)
            if record is None:
                continue
            if record.var_fill_ts_iso is not None:
                continue
            if record.side != side:
                continue
            if abs(record.qty - qty) <= Decimal("0.00000001"):
                return key
        return None

    async def append_signal_snapshot_log(self, payload: dict[str, Any]) -> None:
        if self.signal_snapshots_file is None:
            return
        row = {
            "event": "signal_detected",
            "logged_at": utc_now(),
            "monotonic_ns": monotonic_ns(),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._signal_snapshot_write_lock:
            await asyncio.to_thread(self._append_line, self.signal_snapshots_file, line)

    async def append_execution_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.execution_events_file is None:
            return
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            "monotonic_ns": monotonic_ns(),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._execution_event_write_lock:
            await asyncio.to_thread(self._append_line, self.execution_events_file, line)

    async def append_order_log(self, event_type: str, payload: dict[str, Any]) -> None:
        if self.orders_file is None:
            return
        row = {
            "event": event_type,
            "logged_at": utc_now(),
            **payload,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._order_write_lock:
            await asyncio.to_thread(self.orders_file.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(self._append_line, self.orders_file, line)

    @staticmethod
    def _append_line(path: Path, line: str) -> None:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    async def place_lighter_order(self, record: OrderLifecycle) -> None:
        if not self.args.auto_hedge:
            return

        side = "SELL" if record.side == "buy" else "BUY"

        best_bid, best_ask = await self.get_lighter_best_bid_ask()
        if best_bid is None or best_ask is None:
            async with self._record_lock:
                record.hedge_error = "Lighter order book not ready"
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return

        slippage = HEDGE_SLIPPAGE_BPS / Decimal("10000")
        if side == "BUY":
            is_ask = False
            limit_price = best_ask * (Decimal("1") + slippage)
        else:
            is_ask = True
            limit_price = best_bid * (Decimal("1") - slippage)

        base_amount = int(record.qty * self.base_amount_multiplier)
        if base_amount <= 0:
            async with self._record_lock:
                record.hedge_error = f"Hedge base amount rounds to zero ({record.qty})"
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)
            return

        price_i = int(limit_price * self.price_multiplier)
        async with self._record_lock:
            client_order_id = int(time.time() * 1000)
            while client_order_id in self.lighter_client_order_to_trade_key:
                client_order_id += 1

        try:
            async with self._lighter_signer_lock:
                if not self.lighter_client:
                    self.initialize_lighter_client()
                _, tx_hash, error = await self.lighter_client.create_order(
                    market_index=self.lighter_market_index,
                    client_order_index=client_order_id,
                    base_amount=base_amount,
                    price=price_i,
                    is_ask=is_ask,
                    order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                    time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                    reduce_only=False,
                    trigger_price=0,
                )

            if error is not None:
                raise RuntimeError(f"Sign error: {error}")

            async with self._record_lock:
                record.lighter_side = side
                record.lighter_client_order_id = client_order_id
                record.lighter_tx_hash = tx_hash
                record.hedge_error = None
                self.lighter_client_order_to_trade_key[client_order_id] = record.trade_key
        except Exception as exc:
            async with self._record_lock:
                record.lighter_side = side
                record.hedge_error = str(exc)
                payload = record.to_payload()
            await self.append_order_log("lighter_error", payload)

    def compute_base_qty(self, reference_price: Decimal) -> tuple[Decimal, Decimal | None]:
        amount = self.args.order_amount
        if self.args.order_amount_mode == "base":
            base_qty = amount
            notional = base_qty * reference_price
            return base_qty, notional
        notional = amount
        if reference_price <= 0:
            return Decimal("0"), notional
        return notional / reference_price, notional

    def active_single_side_notional(self) -> Decimal:
        total = Decimal("0")
        for record in self.records.values():
            if record.lighter_fill_ts_iso is not None and record.var_fill_ts_iso is not None:
                continue
            if record.notional_usd is not None:
                total += record.notional_usd
            elif record.qty is not None and record.var_fill_price is not None:
                total += record.qty * record.var_fill_price
        return total

    def max_single_side_notional(self) -> Decimal | None:
        capital = self.args.capital_usd
        if capital is None or capital <= 0:
            return None
        return capital * self.args.max_leverage

    def make_signal_snapshot(
        self,
        *,
        direction: str,
        var_bid: Decimal,
        var_ask: Decimal,
        lighter_bid: Decimal,
        lighter_ask: Decimal,
        var_book_spread_pct: Decimal | None,
        lighter_book_spread_pct: Decimal | None,
        long_current_pct: Decimal | None,
        short_current_pct: Decimal | None,
        long_median_30m_pct: float | None,
        long_median_1h_pct: float | None,
        short_median_30m_pct: float | None,
        short_median_1h_pct: float | None,
    ) -> SignalSnapshot | None:
        if direction == "long_var_short_lighter":
            reference_price = var_ask
            var_side = "BUY"
            lighter_side = "SELL"
        else:
            reference_price = var_bid
            var_side = "SELL"
            lighter_side = "BUY"

        base_qty, notional = self.compute_base_qty(reference_price)
        if base_qty <= 0:
            return None

        open_notional = self.active_single_side_notional()
        max_notional = self.max_single_side_notional()
        if max_notional is not None and notional is not None and open_notional + notional > max_notional:
            self.logger.info(
                "Signal skipped by leverage cap: direction=%s open=%s order=%s max=%s",
                direction,
                open_notional,
                notional,
                max_notional,
            )
            return None

        return SignalSnapshot(
            signal_id=f"sig-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
            direction=direction,
            var_side=var_side,
            lighter_side=lighter_side,
            notional_usd=notional,
            base_qty=base_qty,
            var_bid=var_bid,
            var_ask=var_ask,
            lighter_bid=lighter_bid,
            lighter_ask=lighter_ask,
            var_book_spread_pct=var_book_spread_pct,
            lighter_book_spread_pct=lighter_book_spread_pct,
            long_current_pct=long_current_pct,
            short_current_pct=short_current_pct,
            long_median_30m_pct=long_median_30m_pct,
            long_median_1h_pct=long_median_1h_pct,
            short_median_30m_pct=short_median_30m_pct,
            short_median_1h_pct=short_median_1h_pct,
            entry_offset_pct=self.args.entry_offset_pct,
            open_single_side_notional_usd=open_notional,
            max_single_side_notional_usd=max_notional,
        )

    def signal_is_ready(
        self,
        *,
        current_pct: Decimal | None,
        median_30m: float | None,
        median_1h: float | None,
    ) -> bool:
        if current_pct is None or median_30m is None or median_1h is None:
            return False
        current = Decimal(str(current_pct))
        return (
            current - Decimal(str(median_30m)) >= self.args.entry_offset_pct
            and current - Decimal(str(median_1h)) >= self.args.entry_offset_pct
        )

    async def build_current_market_snapshot(self) -> dict[str, Any] | None:
        var_bid, var_ask, quote_asset = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        if None in (var_bid, var_ask, lighter_bid, lighter_ask):
            return None

        assert var_bid is not None and var_ask is not None and lighter_bid is not None and lighter_ask is not None
        var_book_spread_pct = book_spread_percent(var_bid, var_ask)
        lighter_book_spread_pct = book_spread_percent(lighter_bid, lighter_ask)
        long_current_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
        short_current_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)
        return {
            "quote_asset": quote_asset,
            "var_bid": var_bid,
            "var_ask": var_ask,
            "lighter_bid": lighter_bid,
            "lighter_ask": lighter_ask,
            "var_book_spread_pct": var_book_spread_pct,
            "lighter_book_spread_pct": lighter_book_spread_pct,
            "long_current_pct": long_current_pct,
            "short_current_pct": short_current_pct,
            "long_median_30m_pct": self._median_cross_spread(30 * 60, long_side=True),
            "long_median_1h_pct": self._median_cross_spread(60 * 60, long_side=True),
            "short_median_30m_pct": self._median_cross_spread(30 * 60, long_side=False),
            "short_median_1h_pct": self._median_cross_spread(60 * 60, long_side=False),
        }

    async def place_variational_signal_order(self, snapshot: SignalSnapshot) -> dict[str, Any]:
        start_ns = monotonic_ns()
        await self.append_execution_event(
            "var_build_start",
            {
                "signal_id": snapshot.signal_id,
                "side": snapshot.var_side,
                "amount": decimal_to_str(snapshot.notional_usd if self.args.var_amount_mode == "quote" else snapshot.base_qty),
                "amount_mode": self.args.var_amount_mode,
            },
        )
        order = LocalOrderRequest(
            request_id=f"var-{snapshot.signal_id}",
            signal_id=snapshot.signal_id,
            side=snapshot.var_side,
            amount=decimal_to_str(snapshot.notional_usd if self.args.var_amount_mode == "quote" else snapshot.base_qty) or "0",
            amount_mode=self.args.var_amount_mode,
            market=self.variational_ticker,
            account=None,
            dry_run=not self.args.live_trading,
            timeout_ms=self.args.var_timeout_ms,
            extra={
                "referencePrice": decimal_to_str(snapshot.var_ask if snapshot.var_side == "BUY" else snapshot.var_bid),
                "baseQty": decimal_to_str(snapshot.base_qty),
                "notionalUsd": decimal_to_str(snapshot.notional_usd),
            },
        )
        await self.append_execution_event("var_dispatch", {"signal_id": snapshot.signal_id, "request_id": order.request_id})
        try:
            result = await self.variational_command.place_order(order)
            await self.append_execution_event(
                "var_result",
                {
                    "signal_id": snapshot.signal_id,
                    "request_id": order.request_id,
                    "ok": result.get("ok"),
                    "latency_ms": (monotonic_ns() - start_ns) / 1_000_000,
                    "result": result,
                },
            )
            return result
        except Exception as exc:
            await self.append_execution_event(
                "var_error",
                {
                    "signal_id": snapshot.signal_id,
                    "request_id": order.request_id,
                    "latency_ms": (monotonic_ns() - start_ns) / 1_000_000,
                    "error": str(exc),
                },
            )
            return {"ok": False, "error": str(exc)}

    async def place_lighter_signal_order(self, snapshot: SignalSnapshot) -> dict[str, Any]:
        start_ns = monotonic_ns()
        side = snapshot.lighter_side
        slippage = self.args.lighter_max_slippage_bps / Decimal("10000")
        if side == "BUY":
            is_ask = False
            limit_price = snapshot.lighter_ask * (Decimal("1") + slippage)
            reference_price = snapshot.lighter_ask
        else:
            is_ask = True
            limit_price = snapshot.lighter_bid * (Decimal("1") - slippage)
            reference_price = snapshot.lighter_bid

        base_amount = int(snapshot.base_qty * self.base_amount_multiplier)
        price_i = int(limit_price * self.price_multiplier)
        client_order_id = int(time.time() * 1000)
        while True:
            async with self._record_lock:
                if client_order_id not in self.lighter_client_order_to_trade_key:
                    self.lighter_client_order_to_trade_key[client_order_id] = f"signal:{snapshot.signal_id}"
                    break
            client_order_id += 1
        await self.append_execution_event(
            "lighter_build",
            {
                "signal_id": snapshot.signal_id,
                "side": side,
                "is_ask": is_ask,
                "base_amount": base_amount,
                "base_qty": decimal_to_str(snapshot.base_qty),
                "reference_price": decimal_to_str(reference_price),
                "max_slippage_bps": decimal_to_str(self.args.lighter_max_slippage_bps),
                "limit_price": decimal_to_str(limit_price),
                "price_i": price_i,
                "executor": self.args.lighter_executor,
            },
        )

        if base_amount <= 0 or price_i <= 0:
            error = f"Invalid Lighter order integers base_amount={base_amount} price={price_i}"
            async with self._record_lock:
                if self.lighter_client_order_to_trade_key.get(client_order_id) == f"signal:{snapshot.signal_id}":
                    self.lighter_client_order_to_trade_key.pop(client_order_id, None)
            await self.append_execution_event("lighter_error", {"signal_id": snapshot.signal_id, "error": error})
            return {"ok": False, "error": error}

        if self.args.lighter_executor == "sdk":
            async with self._record_lock:
                if self.lighter_client_order_to_trade_key.get(client_order_id) == f"signal:{snapshot.signal_id}":
                    self.lighter_client_order_to_trade_key.pop(client_order_id, None)
            if not self.args.live_trading:
                await self.append_execution_event(
                    "lighter_result",
                    {
                        "signal_id": snapshot.signal_id,
                        "ok": True,
                        "dry_run": True,
                        "executor": "sdk",
                        "latency_ms": (monotonic_ns() - start_ns) / 1_000_000,
                    },
                )
                return {"ok": True, "dry_run": True, "executor": "sdk"}
            record = OrderLifecycle(
                trade_key=f"signal:{snapshot.signal_id}",
                trade_id=snapshot.signal_id,
                side="buy" if snapshot.var_side == "BUY" else "sell",
                qty=snapshot.base_qty,
                asset=self.variational_ticker or "UNKNOWN",
                auto_hedge_enabled=True,
                last_variational_status="signal",
                notional_usd=snapshot.notional_usd,
            )
            await self.place_lighter_order(record)
            return {"ok": record.hedge_error is None, "error": record.hedge_error}

        payload = {
            "request_id": f"lighter-{snapshot.signal_id}",
            "signal_id": snapshot.signal_id,
            "market_index": self.lighter_market_index,
            "client_order_index": client_order_id,
            "base_amount": base_amount,
            "price": price_i,
            "is_ask": is_ask,
            "reduce_only": False,
            "dry_run": not self.args.live_trading,
        }
        await self.append_execution_event("lighter_dispatch", {"signal_id": snapshot.signal_id, **payload})
        try:
            result = await self.lighter_gateway.place_order(payload, timeout_ms=self.args.lighter_timeout_ms)
            client_id = result.get("client_order_index") or client_order_id
            async with self._record_lock:
                self.lighter_client_order_to_trade_key[int(client_id)] = f"signal:{snapshot.signal_id}"
            await self.append_execution_event(
                "lighter_result",
                {
                    "signal_id": snapshot.signal_id,
                    "request_id": payload["request_id"],
                    "ok": result.get("ok"),
                    "latency_ms": (monotonic_ns() - start_ns) / 1_000_000,
                    "result": result,
                },
            )
            return result
        except Exception as exc:
            async with self._record_lock:
                if self.lighter_client_order_to_trade_key.get(client_order_id) == f"signal:{snapshot.signal_id}":
                    self.lighter_client_order_to_trade_key.pop(client_order_id, None)
            await self.append_execution_event(
                "lighter_error",
                {
                    "signal_id": snapshot.signal_id,
                    "request_id": payload["request_id"],
                    "latency_ms": (monotonic_ns() - start_ns) / 1_000_000,
                    "error": str(exc),
                },
            )
            return {"ok": False, "error": str(exc)}

    async def execute_signal(self, snapshot: SignalSnapshot) -> None:
        await self.append_signal_snapshot_log(snapshot.to_payload())
        record_key = f"signal:{snapshot.signal_id}"
        async with self._record_lock:
            self.records[record_key] = OrderLifecycle(
                trade_key=record_key,
                trade_id=snapshot.signal_id,
                side="buy" if snapshot.var_side == "BUY" else "sell",
                qty=snapshot.base_qty,
                asset=self.variational_ticker or "UNKNOWN",
                auto_hedge_enabled=True,
                last_variational_status="signal",
                notional_usd=snapshot.notional_usd,
            )
            self.record_order.append(record_key)
            self.signal_to_record_key[snapshot.signal_id] = record_key
            self.signal_snapshots[snapshot.signal_id] = snapshot

        start_ns = monotonic_ns()
        await self.append_execution_event("signal_execute_start", snapshot.to_payload())
        var_task = asyncio.create_task(self.place_variational_signal_order(snapshot))
        lighter_task = asyncio.create_task(self.place_lighter_signal_order(snapshot))
        var_result, lighter_result = await asyncio.gather(var_task, lighter_task, return_exceptions=True)
        await self.append_execution_event(
            "signal_execute_done",
            {
                "signal_id": snapshot.signal_id,
                "latency_ms": (monotonic_ns() - start_ns) / 1_000_000,
                "var_result": str(var_result) if isinstance(var_result, Exception) else var_result,
                "lighter_result": str(lighter_result) if isinstance(lighter_result, Exception) else lighter_result,
            },
        )

    async def signal_loop(self) -> None:
        while not self.stop_flag:
            await asyncio.sleep(self.args.signal_interval_seconds)
            if not self.args.auto_trade:
                continue
            now = time.monotonic()
            if now - self._last_signal_monotonic < self.args.signal_cooldown_seconds:
                continue
            market = await self.build_current_market_snapshot()
            if market is None:
                continue
            market.pop("quote_asset", None)
            for direction, current_key, median30_key, median1h_key in (
                ("long_var_short_lighter", "long_current_pct", "long_median_30m_pct", "long_median_1h_pct"),
                ("short_var_long_lighter", "short_current_pct", "short_median_30m_pct", "short_median_1h_pct"),
            ):
                if not self.signal_is_ready(
                    current_pct=market[current_key],
                    median_30m=market[median30_key],
                    median_1h=market[median1h_key],
                ):
                    continue
                snapshot = self.make_signal_snapshot(direction=direction, **market)
                if snapshot is None:
                    continue
                self._last_signal_monotonic = time.monotonic()
                asyncio.create_task(self.execute_signal(snapshot))
                break

    def should_track_variational_event(self, event: dict[str, Any]) -> bool:
        side = str(event.get("side", "")).strip().lower()
        if side not in {"buy", "sell"}:
            return False

        qty = to_decimal(event.get("qty"))
        if qty is None or qty <= 0:
            return False

        asset = str(event.get("asset", "")).strip().upper()
        if not asset:
            return False
        return asset in self.accepted_assets

    async def process_variational_trade_event(self, event: dict[str, Any]) -> None:
        if not self.should_track_variational_event(event):
            return

        key = self.trade_key(event)
        side = str(event.get("side", "")).strip().lower()
        qty = to_decimal(event.get("qty"))
        if qty is None:
            return

        status = normalize_variational_status(str(event.get("status", "")))
        asset = str(event.get("asset", "")).strip().upper() or self.variational_ticker
        trade_id = str(event.get("trade_id", "")).strip()

        now_iso = utc_now()
        fill_iso = str(event.get("timestamp") or now_iso)

        created = False
        created_record: OrderLifecycle | None = None
        matched_signal_key: str | None = None
        var_expected_price: Decimal | None = None
        var_slippage_pct: Decimal | None = None

        async with self._record_lock:
            record = self.records.get(key)
            if record is None and self.args.auto_trade and status == "filled":
                matched_signal_key = self.find_signal_record_for_var_fill(side, qty)
                if matched_signal_key is not None:
                    record = self.records.get(matched_signal_key)
            if record is None:
                record = OrderLifecycle(
                    trade_key=key,
                    trade_id=trade_id,
                    side=side,
                    qty=qty,
                    asset=asset if asset else "UNKNOWN",
                    auto_hedge_enabled=self.args.auto_hedge,
                    last_variational_status=status,
                    notional_usd=None,
                )
                price = to_decimal(event.get("price"))
                if price is not None:
                    record.notional_usd = qty * price
                self.records[key] = record
                self.record_order.append(key)
                created = True
                created_record = record
            else:
                previous_status = record.last_variational_status
                record.last_variational_status = status

            if created:
                previous_status = ""

            should_set_fill = False
            if status == "filled":
                if record.var_fill_ts_iso is None:
                    should_set_fill = True
                elif previous_status != "filled":
                    should_set_fill = True

            if should_set_fill:
                record.var_fill_ts_iso = fill_iso
                record.var_fill_price = to_decimal(event.get("price"))
                filled_payload = record.to_payload()
                if matched_signal_key:
                    snapshot = self.signal_snapshots.get(matched_signal_key.removeprefix("signal:"))
                    if snapshot is not None and record.var_fill_price is not None:
                        if snapshot.var_side == "BUY":
                            var_expected_price = snapshot.var_ask
                            if var_expected_price != 0:
                                var_slippage_pct = ((record.var_fill_price - var_expected_price) / var_expected_price) * Decimal("100")
                        elif snapshot.var_side == "SELL":
                            var_expected_price = snapshot.var_bid
                            if var_expected_price != 0:
                                var_slippage_pct = ((var_expected_price - record.var_fill_price) / var_expected_price) * Decimal("100")
            else:
                filled_payload = None

        if filled_payload is not None:
            await self.append_order_log("variational_fill", filled_payload)
            await self.append_execution_event(
                "var_fill",
                {
                    "trade_key": matched_signal_key or key,
                    "source_trade_key": key,
                    "trade_id": trade_id,
                    "side": side,
                    "asset": asset,
                    "qty": decimal_to_str(qty),
                    "price": decimal_to_str(to_decimal(event.get("price"))),
                    "expected_price": decimal_to_str(var_expected_price),
                    "slippage_pct": decimal_to_str(var_slippage_pct),
                    "slippage_bps": decimal_to_str(var_slippage_pct * Decimal("100") if var_slippage_pct is not None else None),
                    "status": status,
                    "received_at": event.get("received_at"),
                    "timestamp": event.get("timestamp"),
                    "raw": event,
                },
            )

        if created and created_record is not None and self.args.auto_hedge and not self.args.auto_trade:
            await self.place_lighter_order(created_record)

    async def trade_loop(self) -> None:
        while not self.stop_flag:
            current_asset = await self.detect_current_variational_asset()
            if current_asset:
                if current_asset == self.variational_ticker:
                    self._asset_switch_candidate = None
                    self._asset_switch_candidate_hits = 0
                else:
                    if current_asset == self._asset_switch_candidate:
                        self._asset_switch_candidate_hits += 1
                    else:
                        self._asset_switch_candidate = current_asset
                        self._asset_switch_candidate_hits = 1

                    if self._asset_switch_candidate_hits >= ASSET_SWITCH_CONFIRM_TICKS:
                        await self.activate_asset(current_asset, reason="quote_stream_debounced")
                        self._asset_switch_candidate = None
                        self._asset_switch_candidate_hits = 0
            else:
                self._asset_switch_candidate = None
                self._asset_switch_candidate_hits = 0

            events = await self.runtime.monitor.get_trade_events_since(self.trade_event_cursor, limit=500)
            for event in events:
                self.trade_event_cursor = max(self.trade_event_cursor, int(event.get("event_seq", 0) or 0))
                await self.process_variational_trade_event(event)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    def _fmt_price(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return format(value, "f")

    @staticmethod
    def _direction_labels(side: str) -> tuple[str, str]:
        side_n = side.strip().lower()
        if side_n == "buy":
            return "做多 Var / 做空 Lighter", "Long Var / Short Lighter"
        if side_n == "sell":
            return "做空 Var / 做多 Lighter", "Short Var / Long Lighter"
        side_u = side_n.upper() if side_n else "-"
        return side_u, side_u

    def _fmt_pct(self, value: Decimal | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _fmt_signal_pct(
        self,
        current: Decimal | None,
        book_spread_baseline: Decimal | None,
        median_5m: float | None,
        median_30m: float | None,
        median_1h: float | None,
    ) -> str:
        if current is None:
            return "-"
        if book_spread_baseline is None:
            color = "red"
            return f"[{color}]{self._fmt_pct(current)}[/{color}]"

        adjusted = current - book_spread_baseline
        adjusted_f = float(adjusted)
        thresholds = [v for v in (median_5m, median_30m, median_1h) if v is not None]
        is_green = any(adjusted_f > threshold for threshold in thresholds)
        color = "green" if is_green else "red"
        return f"[{color}]{self._fmt_pct(current)}[/{color}]"

    @staticmethod
    def _adjust_signal_pct(current: Decimal | None, book_spread_baseline: Decimal | None) -> Decimal | None:
        if current is None:
            return None
        if book_spread_baseline is None:
            return current
        return current - book_spread_baseline

    @staticmethod
    def _fill_diff_by_direction(
        side: str,
        var_fill_price: Decimal | None,
        lighter_fill_price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal | None]:
        side_n = side.strip().lower()
        if side_n == "buy":
            # Long Var / Short Lighter: lighter_fill - var_fill
            diff = spread_value(var_fill_price, lighter_fill_price)
            pct = spread_percent(diff, var_fill_price)
            return diff, pct
        if side_n == "sell":
            # Short Var / Long Lighter: var_fill - lighter_fill
            diff = spread_value(lighter_fill_price, var_fill_price)
            pct = spread_percent(diff, lighter_fill_price)
            return diff, pct
        diff = spread_value(lighter_fill_price, var_fill_price)
        pct = spread_percent(diff, var_fill_price)
        return diff, pct

    @staticmethod
    def _decimal_as_float(value: Decimal | None) -> float | None:
        if value is None:
            return None
        return float(value)

    @staticmethod
    def _fmt_median_pct(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.4f}%"

    def _record_cross_spreads(
        self,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
    ) -> None:
        now = time.monotonic()
        self.cross_spread_history.append(
            (
                now,
                self._decimal_as_float(long_var_short_lighter_pct),
                self._decimal_as_float(short_var_long_lighter_pct),
            )
        )
        cutoff = now - SPREAD_HISTORY_SECONDS
        while self.cross_spread_history and self.cross_spread_history[0][0] < cutoff:
            self.cross_spread_history.popleft()

    def _median_cross_spread(self, window_seconds: float, long_side: bool) -> float | None:
        now = time.monotonic()
        cutoff = now - window_seconds
        value_index = 1 if long_side else 2
        values = [
            row[value_index]
            for row in self.cross_spread_history
            if row[0] >= cutoff and row[value_index] is not None
        ]
        if not values:
            return None
        return float(median(values))

    async def append_signal_sample(
        self,
        *,
        quote_asset: str | None,
        var_bid: Decimal | None,
        var_ask: Decimal | None,
        lighter_bid: Decimal | None,
        lighter_ask: Decimal | None,
        var_book_spread_pct: Decimal | None,
        lighter_book_spread_pct: Decimal | None,
        spread_color_baseline: Decimal | None,
        long_var_short_lighter_pct: Decimal | None,
        short_var_long_lighter_pct: Decimal | None,
        long_pct_median_5m: float | None,
        long_pct_median_30m: float | None,
        long_pct_median_1h: float | None,
        short_pct_median_5m: float | None,
        short_pct_median_30m: float | None,
        short_pct_median_1h: float | None,
    ) -> None:
        if self.signal_samples_file is None:
            return

        long_adjusted_pct = self._adjust_signal_pct(long_var_short_lighter_pct, spread_color_baseline)
        short_adjusted_pct = self._adjust_signal_pct(short_var_long_lighter_pct, spread_color_baseline)
        row = {
            "logged_at": utc_now(),
            "ticker": self.ticker,
            "variational_ticker": self.variational_ticker,
            "quote_asset": quote_asset,
            "var_bid": decimal_to_str(var_bid),
            "var_ask": decimal_to_str(var_ask),
            "lighter_bid": decimal_to_str(lighter_bid),
            "lighter_ask": decimal_to_str(lighter_ask),
            "var_book_spread_pct": decimal_to_str(var_book_spread_pct),
            "lighter_book_spread_pct": decimal_to_str(lighter_book_spread_pct),
            "spread_baseline_pct": decimal_to_str(spread_color_baseline),
            "long_current_pct": decimal_to_str(long_var_short_lighter_pct),
            "long_adjusted_pct": decimal_to_str(long_adjusted_pct),
            "long_median_5m_pct": long_pct_median_5m,
            "long_median_30m_pct": long_pct_median_30m,
            "long_median_1h_pct": long_pct_median_1h,
            "short_current_pct": decimal_to_str(short_var_long_lighter_pct),
            "short_adjusted_pct": decimal_to_str(short_adjusted_pct),
            "short_median_5m_pct": short_pct_median_5m,
            "short_median_30m_pct": short_pct_median_30m,
            "short_median_1h_pct": short_pct_median_1h,
        }
        line = json.dumps(row, ensure_ascii=True) + "\n"
        async with self._signal_sample_write_lock:
            await asyncio.to_thread(self._append_line, self.signal_samples_file, line)

    async def render_dashboard(self) -> Group:
        var_bid, var_ask, quote_asset = await self.get_variational_best_bid_ask(self.variational_ticker)
        lighter_bid, lighter_ask = await self.get_lighter_best_bid_ask()
        var_book_spread = spread_value(var_bid, var_ask)
        lighter_book_spread = spread_value(lighter_bid, lighter_ask)
        var_book_spread_pct = book_spread_percent(var_bid, var_ask)
        lighter_book_spread_pct = book_spread_percent(lighter_bid, lighter_ask)
        spread_color_baseline: Decimal | None = None
        if var_book_spread_pct is not None and lighter_book_spread_pct is not None:
            spread_color_baseline = (var_book_spread_pct + lighter_book_spread_pct) / Decimal("2")

        long_var_short_lighter_pct = spread_percent(spread_value(var_ask, lighter_bid), var_ask)
        short_var_long_lighter_pct = spread_percent(spread_value(lighter_ask, var_bid), lighter_ask)
        self._record_cross_spreads(
            long_var_short_lighter_pct,
            short_var_long_lighter_pct,
        )

        long_pct_median_5m = self._median_cross_spread(5 * 60, long_side=True)
        long_pct_median_30m = self._median_cross_spread(30 * 60, long_side=True)
        long_pct_median_1h = self._median_cross_spread(60 * 60, long_side=True)
        short_pct_median_5m = self._median_cross_spread(5 * 60, long_side=False)
        short_pct_median_30m = self._median_cross_spread(30 * 60, long_side=False)
        short_pct_median_1h = self._median_cross_spread(60 * 60, long_side=False)

        await self.append_signal_sample(
            quote_asset=quote_asset,
            var_bid=var_bid,
            var_ask=var_ask,
            lighter_bid=lighter_bid,
            lighter_ask=lighter_ask,
            var_book_spread_pct=var_book_spread_pct,
            lighter_book_spread_pct=lighter_book_spread_pct,
            spread_color_baseline=spread_color_baseline,
            long_var_short_lighter_pct=long_var_short_lighter_pct,
            short_var_long_lighter_pct=short_var_long_lighter_pct,
            long_pct_median_5m=long_pct_median_5m,
            long_pct_median_30m=long_pct_median_30m,
            long_pct_median_1h=long_pct_median_1h,
            short_pct_median_5m=short_pct_median_5m,
            short_pct_median_30m=short_pct_median_30m,
            short_pct_median_1h=short_pct_median_1h,
        )

        async with self._record_lock:
            recent_keys = list(self.record_order)[-DASHBOARD_ORDERS:]
            rows = [self.records[key] for key in reversed(recent_keys) if key in self.records]

        is_zh = self.args.lang == "zh"
        header_title = "Variational <-> Lighter"
        auto_hedge_label = "自动对冲" if is_zh else "auto_hedge"
        auto_trade_label = "自动交易" if is_zh else "auto_trade"
        live_label = "实盘" if is_zh else "live"
        auto_hedge_on = "开" if is_zh else "ON"
        auto_hedge_off = "关" if is_zh else "OFF"
        quote_title = "最优买一 / 卖一" if is_zh else "Best Bid / Ask"
        col_exchange = "交易所" if is_zh else "Exchange"
        col_bid = "买一" if is_zh else "Bid"
        col_ask = "卖一" if is_zh else "Ask"
        col_book_spread = "买卖价差" if is_zh else "Bid/Ask Spread"
        col_book_spread_pct = "买卖价差%" if is_zh else "Bid/Ask Spread %"
        spread_title = "价差" if is_zh else "Spreads"
        col_metric = "指标" if is_zh else "Metric"
        col_formula = "公式" if is_zh else "Formula"
        col_value_pct = "当前值%" if is_zh else "Value %"
        col_median_5m_pct = "5分钟中位数%" if is_zh else "Median 5m %"
        col_median_30m_pct = "30分钟中位数%" if is_zh else "Median 30m %"
        col_median_1h_pct = "1小时中位数%" if is_zh else "Median 1h %"
        metric_long_short = "做多 Var / 做空 Lighter" if is_zh else "Long Var / Short Lighter"
        metric_short_long = "做空 Var / 做多 Lighter" if is_zh else "Short Var / Long Lighter"
        orders_title = "最近订单（最新在前）" if is_zh else "Recent Orders (latest first)"
        col_trade_id = "订单ID" if is_zh else "Trade ID"
        col_side = "方向" if is_zh else "Side"
        col_qty = "数量" if is_zh else "Qty"
        col_var_fill_px = "Var 成交价" if is_zh else "Var Fill Px"
        col_lighter_fill_px = "Lighter 成交价" if is_zh else "Lighter Fill Px"
        col_fill_diff = "成交价差(按方向)" if is_zh else "Fill Diff (Directional)"
        col_fill_diff_pct = "成交价差%(按方向)" if is_zh else "Fill Diff % (Directional)"
        no_orders_text = "（暂无订单）" if is_zh else "(no tracked orders yet)"
        variational_label = "Variational"
        lighter_label = "Lighter"
        hedge_color = "green" if self.args.auto_hedge else "red"
        hedge_text = auto_hedge_on if self.args.auto_hedge else auto_hedge_off
        trade_color = "green" if self.args.auto_trade else "red"
        trade_text = auto_hedge_on if self.args.auto_trade else auto_hedge_off
        live_color = "red" if self.args.live_trading else "yellow"
        live_text = auto_hedge_on if self.args.live_trading else "dry-run"

        header = Panel(
            f"[bold]{header_title}[/bold] | [bold]{self.ticker}[/bold] | "
            f"[bold {hedge_color}]{auto_hedge_label}={hedge_text}[/] | "
            f"[bold {trade_color}]{auto_trade_label}={trade_text}[/] | "
            f"[bold {live_color}]{live_label}={live_text}[/] | {utc_now()}",
            border_style="cyan",
        )

        quote_table = Table(title=quote_title, show_header=True, expand=True)
        quote_table.add_column(col_exchange, style="bold")
        quote_table.add_column(col_bid, justify="right")
        quote_table.add_column(col_ask, justify="right")
        quote_table.add_column(col_book_spread, justify="right")
        quote_table.add_column(col_book_spread_pct, justify="right")
        quote_table.add_row(
            f"{variational_label} ({quote_asset or self.variational_ticker})",
            self._fmt_price(var_bid),
            self._fmt_price(var_ask),
            self._fmt_price(var_book_spread),
            self._fmt_pct(var_book_spread_pct),
        )
        quote_table.add_row(
            lighter_label,
            self._fmt_price(lighter_bid),
            self._fmt_price(lighter_ask),
            self._fmt_price(lighter_book_spread),
            self._fmt_pct(lighter_book_spread_pct),
        )

        spread_table = Table(title=spread_title, show_header=True, expand=True)
        spread_table.add_column(col_metric, style="bold")
        spread_table.add_column(col_formula)
        spread_table.add_column(col_value_pct, justify="right")
        spread_table.add_column(col_median_5m_pct, justify="right")
        spread_table.add_column(col_median_30m_pct, justify="right")
        spread_table.add_column(col_median_1h_pct, justify="right")
        spread_table.add_row(
            metric_long_short,
            "lighter_bid - var_ask",
            self._fmt_signal_pct(
                long_var_short_lighter_pct,
                spread_color_baseline,
                long_pct_median_5m,
                long_pct_median_30m,
                long_pct_median_1h,
            ),
            self._fmt_median_pct(long_pct_median_5m),
            self._fmt_median_pct(long_pct_median_30m),
            self._fmt_median_pct(long_pct_median_1h),
        )
        spread_table.add_row(
            metric_short_long,
            "var_bid - lighter_ask",
            self._fmt_signal_pct(
                short_var_long_lighter_pct,
                spread_color_baseline,
                short_pct_median_5m,
                short_pct_median_30m,
                short_pct_median_1h,
            ),
            self._fmt_median_pct(short_pct_median_5m),
            self._fmt_median_pct(short_pct_median_30m),
            self._fmt_median_pct(short_pct_median_1h),
        )

        orders_table = Table(title=orders_title, show_header=True, expand=True)
        orders_table.add_column(col_trade_id)
        orders_table.add_column(col_side)
        orders_table.add_column(col_qty, justify="right")
        orders_table.add_column(col_var_fill_px, justify="right")
        orders_table.add_column(col_lighter_fill_px, justify="right")
        orders_table.add_column(col_fill_diff, justify="right")
        orders_table.add_column(col_fill_diff_pct, justify="right")

        if not rows:
            orders_table.add_row(
                no_orders_text,
                "-",
                "-",
                "-",
                "-",
                "-",
                "-",
            )
        else:
            for row in rows:
                payload = row.to_payload()
                trade_display = row.trade_id[:10] if row.trade_id else row.trade_key[:10]
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    row.side,
                    row.var_fill_price,
                    row.lighter_fill_price,
                )
                side_zh, side_en = self._direction_labels(row.side)
                side_display = side_zh if is_zh else side_en
                orders_table.add_row(
                    trade_display,
                    side_display,
                    self._fmt_price(row.qty),
                    payload["variational_filled_price"] or "-",
                    payload["lighter_filled_price"] or "-",
                    self._fmt_price(fill_diff),
                    self._fmt_pct(fill_diff_pct),
                )

        return Group(header, quote_table, spread_table, orders_table)

    async def export_trade_records_csv(self) -> None:
        if self.trade_records_csv_file is None:
            return

        async with self._record_lock:
            keys = list(self.record_order)
            rows: list[dict[str, Any]] = []
            for key in keys:
                record = self.records.get(key)
                if record is None:
                    continue
                payload = record.to_payload()
                fill_diff, fill_diff_pct = self._fill_diff_by_direction(
                    record.side,
                    record.var_fill_price,
                    record.lighter_fill_price,
                )
                side_zh, side_en = self._direction_labels(record.side)
                rows.append(
                    {
                        "trade_key": record.trade_key,
                        "trade_id": record.trade_id,
                        "asset": record.asset,
                        "side_raw": record.side,
                        "direction_zh": side_zh,
                        "direction_en": side_en,
                        "qty": decimal_to_str(record.qty),
                        "notional_usd": payload["notional_usd"],
                        "variational_filled_price": payload["variational_filled_price"],
                        "variational_filled_at": payload["variational_filled_at"],
                        "lighter_order_side": payload["lighter_order_side"],
                        "lighter_client_order_id": payload["lighter_client_order_id"],
                        "lighter_filled_price": payload["lighter_filled_price"],
                        "lighter_filled_at": payload["lighter_filled_at"],
                        "fill_diff_var_minus_lighter": decimal_to_str(fill_diff),
                        "fill_diff_pct_vs_var": decimal_to_str(fill_diff_pct),
                        "auto_hedge_enabled": payload["auto_hedge_enabled"],
                        "hedge_error": payload["hedge_error"],
                        "last_variational_status": payload["last_variational_status"],
                    }
                )

        snapshot_sig = json.dumps(rows, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        if snapshot_sig == self._trade_records_snapshot_sig:
            return

        fieldnames = [
            "trade_key",
            "trade_id",
            "asset",
            "side_raw",
            "direction_zh",
            "direction_en",
            "qty",
            "notional_usd",
            "variational_filled_price",
            "variational_filled_at",
            "lighter_order_side",
            "lighter_client_order_id",
            "lighter_filled_price",
            "lighter_filled_at",
            "fill_diff_var_minus_lighter",
            "fill_diff_pct_vs_var",
            "auto_hedge_enabled",
            "hedge_error",
            "last_variational_status",
        ]
        async with self._trade_csv_write_lock:
            if snapshot_sig == self._trade_records_snapshot_sig:
                return
            await asyncio.to_thread(self._write_csv_rows, self.trade_records_csv_file, fieldnames, rows)
            self._trade_records_snapshot_sig = snapshot_sig

    @staticmethod
    def _write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)

    async def dashboard_loop(self) -> None:
        refresh_interval = DASHBOARD_REFRESH_SECONDS
        refresh_per_second = max(1, int(round(1.0 / refresh_interval)))
        initial_render = await self.render_dashboard()
        await self.export_trade_records_csv()
        with Live(
            initial_render,
            console=self.dashboard_console,
            refresh_per_second=refresh_per_second,
            screen=True,
        ) as live:
            while not self.stop_flag:
                await asyncio.sleep(refresh_interval)
                live.update(await self.render_dashboard())
                await self.export_trade_records_csv()

    async def run(self) -> None:
        self.setup_signal_handlers()
        await self.runtime.start()
        self.print_startup_next_steps()
        self.logger.info(
            "Listening for Variational forwarder events on ws://%s:%s and ws://%s:%s; command broker ws://%s:%s",
            FORWARDER_HOST,
            FORWARDER_WS_PORT,
            FORWARDER_HOST,
            FORWARDER_REST_PORT,
            FORWARDER_HOST,
            self.args.command_port,
        )
        self.logger.info(
            "Auto trade=%s live=%s lighter_executor=%s order_amount=%s mode=%s entry_offset_pct=%s max_leverage=%s",
            self.args.auto_trade,
            self.args.live_trading,
            self.args.lighter_executor,
            self.args.order_amount,
            self.args.order_amount_mode,
            self.args.entry_offset_pct,
            self.args.max_leverage,
        )

        await self.wait_for_variational_ready()
        self.logger.info("Variational heartbeat is live")
        self.initialize_lighter_client()
        initial_asset = await self.wait_for_ticker_resolution()
        await self.activate_asset(initial_asset, reason="startup")

        self.trade_event_cursor = await self.runtime.monitor.get_latest_trade_event_seq()
        self.logger.info("Tracking new Variational trade events from seq>%s", self.trade_event_cursor)

        self.trade_task = asyncio.create_task(self.trade_loop())
        if self.args.auto_trade:
            self.signal_task = asyncio.create_task(self.signal_loop())
        self.dashboard_task = asyncio.create_task(self.dashboard_loop())

        while not self.stop_flag:
            await asyncio.sleep(0.25)

    async def close(self) -> None:
        self.stop_flag = True

        if self.dashboard_task and not self.dashboard_task.done():
            self.dashboard_task.cancel()
            await asyncio.gather(self.dashboard_task, return_exceptions=True)

        if self.trade_task and not self.trade_task.done():
            self.trade_task.cancel()
            await asyncio.gather(self.trade_task, return_exceptions=True)

        if self.signal_task and not self.signal_task.done():
            self.signal_task.cancel()
            await asyncio.gather(self.signal_task, return_exceptions=True)

        if self.lighter_ws_task and not self.lighter_ws_task.done():
            self.lighter_ws_task.cancel()
            await asyncio.gather(self.lighter_ws_task, return_exceptions=True)

        await asyncio.gather(
            self.variational_command.close(),
            self.lighter_gateway.close(),
            return_exceptions=True,
        )

        if self.lighter_client is not None:
            close_method = getattr(self.lighter_client, "close", None)
            if callable(close_method):
                with contextlib.suppress(Exception):
                    close_result = close_method()
                    if asyncio.iscoroutine(close_result):
                        await close_result

        await self.runtime.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Track Variational order lifecycle and optionally auto-hedge on Lighter (ticker auto-detected)."
    )
    parser.add_argument(
        "--lang",
        choices=["zh", "en"],
        default="zh",
        help="Dashboard language: zh (Chinese) or en (English). Default: zh",
    )
    parser.add_argument(
        "--no-hedge",
        action="store_false",
        dest="auto_hedge",
        help="Disable automatic Lighter hedge placement (default: enabled)",
    )
    parser.add_argument(
        "--auto-trade",
        action="store_true",
        help="Enable signal-driven simultaneous Variational + Lighter order placement. Defaults to off.",
    )
    parser.add_argument(
        "--live-trading",
        action="store_true",
        help="Send real orders. Without this flag, auto-trade only writes dry-run execution logs.",
    )
    parser.add_argument(
        "--target-ticker",
        default="BTC",
        help="Only trade this ticker/asset; use auto to follow the active Variational page. Default: BTC",
    )
    parser.add_argument(
        "--order-amount",
        type=parse_decimal_arg,
        default=Decimal("20"),
        help="Per-leg order amount. Default: 20.",
    )
    parser.add_argument(
        "--order-amount-mode",
        choices=["quote", "base"],
        default="quote",
        help="Interpret --order-amount as quote notional (USDC) or base quantity. Default: quote",
    )
    parser.add_argument(
        "--var-amount-mode",
        choices=["quote", "base"],
        default="quote",
        help="Amount mode sent to the Variational Chrome command executor. Default: quote",
    )
    parser.add_argument(
        "--capital-usd",
        type=parse_decimal_arg,
        default=None,
        help="Capital used for max leverage cap. If omitted, no notional cap is applied.",
    )
    parser.add_argument(
        "--max-leverage",
        type=parse_decimal_arg,
        default=MAX_LEVERAGE_DEFAULT,
        help="Max total single-side notional as capital * leverage. Default: 2",
    )
    parser.add_argument(
        "--entry-offset-pct",
        type=parse_decimal_arg,
        default=ENTRY_OFFSET_PCT_DEFAULT,
        help="Entry condition: current spread must exceed both 30m and 1h medians by this percent value. Default: 0.008",
    )
    parser.add_argument(
        "--signal-interval-seconds",
        type=float,
        default=0.2,
        help="How often to check entry signals when --auto-trade is enabled. Default: 0.2",
    )
    parser.add_argument(
        "--signal-cooldown-seconds",
        type=float,
        default=1.0,
        help="Minimum seconds between signal-triggered orders. Default: 1.0",
    )
    parser.add_argument(
        "--command-port",
        type=int,
        default=FORWARDER_COMMAND_PORT,
        help="Local WebSocket command broker port for Chrome extension Var orders. Default: 8768",
    )
    parser.add_argument(
        "--var-timeout-ms",
        type=int,
        default=5000,
        help="Timeout for Variational Chrome command order result. Default: 5000",
    )
    parser.add_argument(
        "--lighter-timeout-ms",
        type=int,
        default=5000,
        help="Timeout for Lighter gateway order result. Default: 5000",
    )
    parser.add_argument(
        "--lighter-executor",
        choices=["rust-ws", "sdk"],
        default="rust-ws",
        help="Lighter order path. rust-ws uses the Rust sidecar, sdk keeps the legacy rollback path.",
    )
    parser.add_argument(
        "--lighter-gateway-url",
        default=LIGHTER_GATEWAY_URL,
        help="Local Rust Lighter gateway WebSocket URL. Default: ws://127.0.0.1:8771",
    )
    parser.add_argument(
        "--lighter-max-slippage-bps",
        type=parse_decimal_arg,
        default=Decimal("0.3"),
        help="Limit price protection around Lighter best bid/ask in bps. Default: 0.3",
    )
    parser.set_defaults(auto_hedge=True, auto_trade=False, live_trading=False)
    return parser.parse_args()


async def _amain() -> None:
    load_dotenv()
    args = parse_args()
    runtime = VariationalToLighterRuntime(args)
    try:
        await runtime.run()
    finally:
        await runtime.close()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
