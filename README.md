# OBI from a live Binance order book

Learning project: keep a local Binance Spot order book (REST snapshot + websocket diffs), compute **order book imbalance (OBI)**, view it in a browser dashboard.

No trading. No API keys — public market data only.

---

## Table of contents

- [Run the app](#run-the-app)
- [What this does](#what-this-does)
- [The dashboard](#the-dashboard)
- [Terms](#terms)
- [Why snapshot + diffs](#why-snapshot--diffs)
- [Files](#files)
- [How the sync works](#how-the-sync-works)
- [Endpoints and env vars](#endpoints-and-env-vars)
- [WebSocket messages](#websocket-messages)
- [When something looks wrong](#when-something-looks-wrong)
- [Binance links](#binance-links)

---

## Run the app

Python 3.12+ and [uv](https://github.com/astral-sh/uv) (or pip).

```bash
cd Arbi-rag-crypto-agents
uv sync
uv run uvicorn obi_imbalance:app --reload
```

Open **http://127.0.0.1:8000/** (serve via uvicorn — don't open `index.html` as a file).

**Working looks like:**

1. Sync log shows `step 1` … `step 8: synced, applying live events`
2. **Sync: SYNCED** in the header
3. Metrics table filling in; charts updating; book ladder showing asks/bids

**Env vars:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `BINANCE_SYMBOL` | `btcusdt` | Pair at server startup |
| `SNAPSHOT_LIMIT` | `100` | REST depth levels per side (max `5000`) |

```bash
export BINANCE_SYMBOL=ethusdt
export SNAPSHOT_LIMIT=5000
uv run uvicorn obi_imbalance:app --reload
```

Changing the pair in the UI triggers a full resync — book/charts clear briefly.

`Ctrl+C` to stop.

---

## What this does

```
Binance REST + WS  →  obi_imbalance.py  →  WebSocket /ws  →  index.html
   (snapshot+diffs)      merge + OBI            JSON              display only
```

- **Backend** talks to Binance, maintains the local book, computes OBI, broadcasts JSON.
- **Frontend** only renders server messages. No Binance connection, no OBI math in the browser.

If OBI is wrong, debug `obi_imbalance.py`, not the HTML.

---

## The dashboard

| Section | What you see |
|---------|----------------|
| **Header** | Pair dropdown, `Source` (snapshot vs local book), `Sync`, `WS`, pipeline `①→②→③` |
| **Summary metrics** | Table: Metric \| Value \| What it means (OBI, spread, volumes, level counts, `lastUpdateId`) |
| **OBI over time** | Line chart of OBI from live `book` messages (after merge) |
| **Volume: snapshot vs local** | Orange bars = REST snapshot only; black = merged local book |
| **Depth (top 10)** | Bar chart of size at each displayed price |
| **Book ladder** | Two tables: **Side \| Price \| Size** — asks (left), bids (right), spread row between |
| **Server state** | Last message: type, source, symbol, synced |
| **Sync log** | Backend steps (`step 1`…`step 8`, resyncs, errors) — Time \| Message \| Extra |

**Message types from server:**

- `snapshot` — book right after REST load (step 7), before diffs catch up
- `book` — local book after each applied diff (live)
- `status` — sync log lines only

---

## Terms

| Term | Meaning |
|------|---------|
| **Order book** | Resting bids (buy) and asks (sell) at price levels |
| **Snapshot** | One `GET /depth` response + `lastUpdateId` |
| **Diff** | WS patch: price level → new size (`0` = remove) |
| **Local book** | Snapshot with diffs applied in order |
| **Spread** | Best ask − best bid |
| **OBI** | `(bid_volume − ask_volume) / (bid_volume + ask_volume)` |

OBI uses **all levels** in the local book (default 100 per side). The ladder shows **top 10** only.

---

## Why snapshot + diffs

Binance sends patches, not the full book every tick. You need:

1. REST snapshot (starting picture)
2. Ordered websocket diffs (updates)

Skip or mis-order a diff → local book drifts → OBI lies.

This follows [Binance’s local order book guide](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly). The sync log mirrors those steps.

Not using `@depth10@100ms` (pre-built top 10 refresh) — that skips the merge lesson.

---

## Files

```
obi_imbalance.py   # FastAPI, Binance sync, OBI, /ws
index.html         # Dashboard (Chart.js), display only
pyproject.toml     # fastapi, httpx, uvicorn, websockets
```

**Backend:**

| Piece | Role |
|-------|------|
| `LimitOrderBook` | Bids/asks dicts, snapshot load, apply diffs, build payload |
| `Hub` | One book, client list, `status()` / `broadcast()` to all `/ws` clients |
| `sync_order_book()` | Binance steps 1–8 |
| `apply_depth_event()` | Per-diff rules (ignore / apply / restart) |

---

## How the sync works

| Step | Binance | Code |
|------|---------|------|
| 1 | Open `{symbol}@depth` WS | `@depth@100ms` |
| 2 | Buffer events; note first `U` | `first_U` |
| 3 | `GET /api/v3/depth?limit=N` | `fetch_depth_snapshot` |
| 4 | If `lastUpdateId < first_U`, refetch | loop step 3 |
| 5 | Drop events with `u <= lastUpdateId` | pop buffer |
| 6 | First event: `U <= lastUpdateId + 1 <= u` | bridge check |
| 7 | Load snapshot → emit `type: snapshot` | `load_snapshot` + broadcast |
| 8 | Apply buffer + live diffs → emit `type: book` | `apply_depth_event` |

**Per diff after merge:**

- `u <= local id` → ignore  
- `U > local id + 1` → missed events → full restart  
- else update `b` / `a`, set local id to `u`

Diff fields: `U`, `u`, `b`, `a` (and `pu` on the wire).

---

## Endpoints and env vars

| | URL |
|---|-----|
| Snapshot | `GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=100` |
| Diffs | `wss://stream.binance.com:9443/ws/btcusdt@depth@100ms` |

| Route | Purpose |
|-------|---------|
| `GET /` | Dashboard (`index.html`) |
| `GET /api/symbols` | Allowed pairs (JSON) |
| `WS /ws` | Live stream |

Pairs: `POPULAR_SYMBOLS` in `obi_imbalance.py`.

---

## WebSocket messages

**Browser → server**

```json
{ "action": "set_symbol", "symbol": "ethusdt" }
```

**Server → browser**

| `type` | When | Main fields |
|--------|------|-------------|
| `hello` | On connect | `symbols`, `symbol`, `pair`, `pipeline` |
| `status` | Sync step | `message` (+ optional debug fields) |
| `snapshot` | After REST load | `obi`, `bid_volume`, `ask_volume`, `bids`, `asks`, `source: rest_snapshot` |
| `book` | Each applied diff | Same shape, `source: local_book`, `synced: true` when caught up |
| `symbol_ack` | After pair change | `ok`, `symbol`, `pair` |

Example `book` payload:

```json
{
  "type": "book",
  "source": "local_book",
  "pair": "BTC/USDT",
  "synced": true,
  "lastUpdateId": 94177039150,
  "obi": 0.034,
  "spread": 0.01,
  "bid_volume": 500.84,
  "ask_volume": 340.12,
  "best_bid": 96840.5,
  "best_ask": 96840.51,
  "bid_levels": 100,
  "ask_levels": 100,
  "bids": [["96840.5", "1.234"]],
  "asks": [["96840.51", "0.891"]]
}
```

---

## When something looks wrong

| Symptom | Likely cause |
|---------|----------------|
| `WS: OFFLINE` | Server not running or wrong URL |
| Book empty, **SYNCING** | Still on steps 1–8 |
| `step 4: snapshot too old` | Refetching REST |
| `step 6: snapshot not bridged` | Resyncing |
| `missed events — restart` | Gap in update ids |
| Orange volume bar but flat black | Waiting for first `book` after snapshot |
| OBI flat at 0 | Book empty / not synced |
| `Address already in use` on start | Old uvicorn still on port 8000 (e.g. suspended with Ctrl+Z). `Ctrl+C` to stop, or `lsof -ti :8000 \| xargs kill -9` |

---

## Binance links

- [REST depth](https://developers.binance.com/docs/binance-spot-api-docs/rest-api#order-book)
- [Diff depth stream](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream)
- [Local order book how-to](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#how-to-manage-a-local-order-book-correctly)
