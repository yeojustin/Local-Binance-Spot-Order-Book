"""Binance local order book + OBI, streamed to browsers over WebSocket."""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from websockets import connect

ROOT = Path(__file__).parent
BINANCE_REST = "https://api.binance.com/api/v3/depth"
SNAPSHOT_LIMIT = int(os.getenv("SNAPSHOT_LIMIT", "100"))

POPULAR_SYMBOLS: dict[str, str] = {
    "btcusdt": "BTC",
    "ethusdt": "ETH",
    "bnbusdt": "BNB",
    "solusdt": "SOL",
    "xrpusdt": "XRP",
    "dogeusdt": "DOGE",
    "adausdt": "ADA",
    "avaxusdt": "AVAX",
    "linkusdt": "LINK",
}

_default = os.getenv("BINANCE_SYMBOL", "btcusdt").lower()
DEFAULT_SYMBOL = _default if _default in POPULAR_SYMBOLS else "btcusdt"


def depth_ws_url(symbol: str) -> str:
    return f"wss://stream.binance.com:9443/ws/{symbol}@depth@100ms"


def pair_label(symbol: str) -> str:
    base = POPULAR_SYMBOLS.get(symbol, symbol.replace("usdt", "").upper())
    return f"{base}/USDT"


def symbol_list() -> list[dict]:
    return [{"id": s, "base": b, "pair": f"{b}/USDT"} for s, b in POPULAR_SYMBOLS.items()]


def fmt_level(price: float, qty: float) -> list[str]:
    p = f"{price:.8f}".rstrip("0").rstrip(".")
    q = f"{qty:.8f}".rstrip("0").rstrip(".")
    return [p, q]


class LimitOrderBook:
    def __init__(self, symbol: str = DEFAULT_SYMBOL) -> None:
        self.symbol = symbol
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None
        self.synced = False

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.synced = False

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}
        self.last_update_id = snapshot["lastUpdateId"]
        self.synced = False

    @staticmethod
    def _patch_side(book: dict[float, float], levels: list) -> None:
        for price_str, qty_str in levels:
            price, qty = float(price_str), float(qty_str)
            if qty == 0:
                book.pop(price, None)
            else:
                book[price] = qty

    def apply_diff(self, bids: list, asks: list) -> None:
        self._patch_side(self.bids, bids)
        self._patch_side(self.asks, asks)

    def to_payload(self, msg_type: str = "book") -> dict:
        top_bids = sorted(self.bids.items(), reverse=True)[:10]
        top_asks = sorted(self.asks.items())[:10]
        bid_vol = sum(self.bids.values())
        ask_vol = sum(self.asks.values())
        total = bid_vol + ask_vol
        obi = (bid_vol - ask_vol) / total if total else 0.0

        return {
            "type": msg_type,
            "source": "rest_snapshot" if msg_type == "snapshot" else "local_book",
            "symbol": self.symbol,
            "pair": pair_label(self.symbol),
            "synced": self.synced,
            "lastUpdateId": self.last_update_id,
            "bid_levels": len(self.bids),
            "ask_levels": len(self.asks),
            "bid_volume": bid_vol,
            "ask_volume": ask_vol,
            "best_bid": top_bids[0][0] if top_bids else None,
            "best_ask": top_asks[0][0] if top_asks else None,
            "obi": obi,
            "spread": top_asks[0][0] - top_bids[0][0] if top_asks and top_bids else 0,
            "bids": [fmt_level(p, q) for p, q in top_bids],
            "asks": [fmt_level(p, q) for p, q in top_asks][::-1],
        }


class Hub:
    """Shared app state: one book, many UI clients, one Binance feed task."""

    def __init__(self) -> None:
        self.book = LimitOrderBook()
        self.clients: list[WebSocket] = []
        self.feed_task: asyncio.Task | None = None
        self.feed_lock = asyncio.Lock()

    async def broadcast(self, payload: dict) -> None:
        msg = json.dumps(payload)
        for ws in self.clients[:]:
            try:
                await ws.send_text(msg)
            except Exception:
                self.clients.remove(ws)

    async def send_json(self, ws: WebSocket, payload: dict) -> None:
        await ws.send_text(json.dumps(payload))

    async def status(self, message: str, **fields) -> None:
        await self.broadcast({"type": "status", "symbol": self.book.symbol, "message": message, **fields})


hub = Hub()


async def fetch_depth_snapshot(symbol: str) -> dict:
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(
            BINANCE_REST,
            params={"symbol": symbol.upper(), "limit": SNAPSHOT_LIMIT},
        )
        r.raise_for_status()
        return r.json()


def apply_depth_event(event: dict) -> str:
    """Returns: ignored | applied | restart."""
    book = hub.book
    local_id = book.last_update_id
    u, U = event["u"], event["U"]

    if u <= local_id:
        return "ignored"
    if U > local_id + 1:
        return "restart"

    book.apply_diff(event["b"], event["a"])
    book.last_update_id = u
    return "applied"


async def process_event(event: dict) -> bool:
    """Apply one diff; broadcast book on success. False = caller should resync."""
    result = apply_depth_event(event)
    if result == "ignored":
        return True
    if result == "restart":
        hub.book.synced = False
        await hub.status(
            "missed events — restart",
            local_id=hub.book.last_update_id,
            event_U=event["U"],
            event_u=event["u"],
        )
        return False
    await hub.broadcast(hub.book.to_payload())
    return True


def bridged_to_snapshot(local_id: int, event: dict) -> bool:
    return event["U"] <= local_id + 1 <= event["u"]


async def sync_order_book(symbol: str) -> None:
    buffer: list[dict] = []
    url = depth_ws_url(symbol)

    await hub.status("step 1: open depth websocket", stream=url)

    async with connect(url) as ws:

        async def reader() -> None:
            async for raw in ws:
                if hub.book.symbol != symbol:
                    return
                buffer.append(json.loads(raw))

        reader_task = asyncio.create_task(reader())

        try:
            await hub.status("step 2: buffering events")
            while not buffer:
                if hub.book.symbol != symbol:
                    return
                await asyncio.sleep(0.01)
            first_U = buffer[0]["U"]
            await hub.status("step 2: first event seen", first_U=first_U)

            while True:
                await hub.status("step 3: fetch depth snapshot", limit=SNAPSHOT_LIMIT)
                snapshot = await fetch_depth_snapshot(symbol)
                if snapshot["lastUpdateId"] >= first_U:
                    break
                await hub.status(
                    "step 4: snapshot too old, refetch",
                    snapshot_lastUpdateId=snapshot["lastUpdateId"],
                    first_U=first_U,
                )

            local_id = snapshot["lastUpdateId"]
            while buffer and buffer[0]["u"] <= local_id:
                buffer.pop(0)

            while not buffer:
                if hub.book.symbol != symbol:
                    return
                await hub.status("waiting for event after snapshot")
                await asyncio.sleep(0.05)

            if not bridged_to_snapshot(local_id, buffer[0]):
                await hub.status(
                    "step 6: snapshot not bridged — restart",
                    U=buffer[0]["U"],
                    u=buffer[0]["u"],
                    lastUpdateId=local_id,
                )
                return await sync_order_book(symbol)

            hub.book.load_snapshot(snapshot)
            await hub.broadcast(hub.book.to_payload(msg_type="snapshot"))
            await hub.status(
                "step 7: local book = snapshot",
                lastUpdateId=local_id,
                bid_levels=len(hub.book.bids),
                ask_levels=len(hub.book.asks),
            )

            while buffer:
                if hub.book.symbol != symbol:
                    return
                if not await process_event(buffer.pop(0)):
                    return await sync_order_book(symbol)

            hub.book.synced = True
            await hub.status("step 8: synced, applying live events", lastUpdateId=hub.book.last_update_id)
            await hub.broadcast(hub.book.to_payload())

            while True:
                if hub.book.symbol != symbol:
                    return
                if not buffer:
                    await asyncio.sleep(0.001)
                    continue
                if not await process_event(buffer.pop(0)):
                    return await sync_order_book(symbol)
        finally:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass


async def run_feed_loop() -> None:
    while True:
        symbol = hub.book.symbol
        try:
            await sync_order_book(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if hub.book.symbol != symbol:
                return
            hub.book.synced = False
            await hub.status("feed error — retrying", error=str(exc))
            await asyncio.sleep(5)


async def restart_feed() -> None:
    async with hub.feed_lock:
        if hub.feed_task and not hub.feed_task.done():
            hub.feed_task.cancel()
            try:
                await hub.feed_task
            except asyncio.CancelledError:
                pass
        hub.book.clear()
        hub.feed_task = asyncio.create_task(run_feed_loop())


async def set_symbol(symbol: str) -> bool:
    symbol = symbol.lower().strip()
    if symbol not in POPULAR_SYMBOLS:
        return False
    if symbol == hub.book.symbol:
        return True
    hub.book.symbol = symbol
    await restart_feed()
    return True


@asynccontextmanager
async def lifespan(_: FastAPI):
    await restart_feed()
    yield
    async with hub.feed_lock:
        if hub.feed_task and not hub.feed_task.done():
            hub.feed_task.cancel()
            try:
                await hub.feed_task
            except asyncio.CancelledError:
                pass


app = FastAPI(title="OBI Imbalance", lifespan=lifespan)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(ROOT / "index.html")


@app.get("/api/symbols")
def symbols() -> dict:
    return {"default": hub.book.symbol, "symbols": symbol_list()}


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    hub.clients.append(websocket)

    await hub.send_json(
        websocket,
        {
            "type": "hello",
            "symbol": hub.book.symbol,
            "pair": pair_label(hub.book.symbol),
            "symbols": symbol_list(),
            "pipeline": "snapshot + depth diffs → local book → OBI",
        },
    )
    if hub.book.bids or hub.book.asks:
        await hub.send_json(websocket, hub.book.to_payload())

    try:
        while True:
            msg = json.loads(await websocket.receive_text())
            if msg.get("action") != "set_symbol":
                continue
            ok = await set_symbol(msg.get("symbol", ""))
            await hub.send_json(
                websocket,
                {
                    "type": "symbol_ack",
                    "symbol": hub.book.symbol,
                    "pair": pair_label(hub.book.symbol),
                    "ok": ok,
                },
            )
    except WebSocketDisconnect:
        if websocket in hub.clients:
            hub.clients.remove(websocket)
