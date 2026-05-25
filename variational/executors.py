from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import websockets


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class LocalOrderRequest:
    request_id: str
    signal_id: str
    side: str
    amount: str
    amount_mode: str
    market: str | None
    account: str | None
    dry_run: bool
    timeout_ms: int
    extra: dict[str, Any]

    def to_variational_payload(self) -> dict[str, Any]:
        return {
            "type": "PLACE_ORDER",
            "requestId": self.request_id,
            "signalId": self.signal_id,
            "side": self.side,
            "amount": self.amount,
            "amountMode": self.amount_mode,
            "market": self.market,
            "account": self.account,
            "dryRun": self.dry_run,
            "timeoutMs": self.timeout_ms,
            **self.extra,
        }


class LocalWsRpcClient:
    def __init__(self, url: str, role: str, timeout_ms: int = 5000) -> None:
        self.url = url
        self.role = role
        self.timeout_ms = timeout_ms
        self._ws: Any | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._connect_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()

    async def connect(self) -> None:
        async with self._connect_lock:
            if self._ws is not None:
                return
            self._ws = await websockets.connect(
                self.url,
                ping_interval=20,
                ping_timeout=20,
                max_size=None,
            )
            await self._ws.send(json.dumps({"type": "REGISTER", "role": self.role, "timestamp": utc_now()}))
            self._reader_task = asyncio.create_task(self._reader_loop())

    async def close(self) -> None:
        ws = self._ws
        self._ws = None
        if ws is not None:
            await ws.close()
        if self._reader_task is not None and not self._reader_task.done():
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
        for future in self._pending.values():
            if not future.done():
                future.set_exception(RuntimeError(f"{self.role} websocket closed"))
        self._pending.clear()

    async def request(self, payload: dict[str, Any], *, timeout_ms: int | None = None) -> dict[str, Any]:
        await self.connect()
        request_id = str(payload.get("requestId") or payload.get("request_id") or uuid.uuid4())
        payload["requestId"] = request_id
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future

        try:
            async with self._send_lock:
                if self._ws is None:
                    raise RuntimeError(f"{self.role} websocket is not connected")
                await self._ws.send(json.dumps(payload, ensure_ascii=True))
            timeout = (timeout_ms if timeout_ms is not None else self.timeout_ms) / 1000
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)

    async def _reader_loop(self) -> None:
        try:
            assert self._ws is not None
            async for raw in self._ws:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(payload, dict):
                    continue

                request_id = str(payload.get("requestId") or payload.get("request_id") or "")
                if not request_id:
                    continue
                if payload.get("type") == "ORDER_DISPATCHED":
                    continue
                future = self._pending.get(request_id)
                if future is not None and not future.done():
                    future.set_result(payload)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(exc)
            self._pending.clear()
            self._ws = None


class VariationalCommandClient(LocalWsRpcClient):
    def __init__(self, url: str, timeout_ms: int = 5000) -> None:
        super().__init__(url, role="python", timeout_ms=timeout_ms)

    async def place_order(self, order: LocalOrderRequest) -> dict[str, Any]:
        return await self.request(order.to_variational_payload(), timeout_ms=order.timeout_ms)


class RustLighterGatewayClient(LocalWsRpcClient):
    def __init__(self, url: str, timeout_ms: int = 5000) -> None:
        super().__init__(url, role="python", timeout_ms=timeout_ms)

    async def place_order(self, payload: dict[str, Any], *, timeout_ms: int = 5000) -> dict[str, Any]:
        request_id = str(payload.get("request_id") or payload.get("requestId") or uuid.uuid4())
        gateway_payload = {key: value for key, value in payload.items() if key not in {"request_id", "requestId"}}
        message = {
            "type": "PLACE_ORDER",
            "requestId": request_id,
            **gateway_payload,
        }
        return await self.request(message, timeout_ms=timeout_ms)
