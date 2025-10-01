#!/usr/bin/env python3
"""
Resilient WebSocket client for Upstox Market Data Feed.
"""
import os, time, json, logging, asyncio, ssl, threading, random
import requests
import websockets

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

USE_PROTO = os.getenv("USE_PROTO", "False").lower() in ("1","true","yes")
if USE_PROTO:
    try:
        import marketdata_pb2
    except Exception as e:
        logging.warning("Proto import failed: %s. Binary payloads will be logged raw. Set USE_PROTO=False to silence.", e)
        USE_PROTO = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN") or ""
MARKET_FEED_AUTHORIZE_URL = os.getenv("MARKET_FEED_AUTHORIZE_URL") or "https://api.upstox.com/v3/market-data-feed/authorize"

RECONNECT_BASE_DELAY = float(os.getenv("RECONNECT_BASE_DELAY") or 1.0)
RECONNECT_MAX_DELAY = float(os.getenv("RECONNECT_MAX_DELAY") or 60.0)
DEFAULT_SUBSCRIBE_BATCH = int(os.getenv("DEFAULT_SUBSCRIBE_BATCH") or 100)
WS_READ_TIMEOUT = int(os.getenv("WS_READ_TIMEOUT") or 60)
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL") or 25)

if not UPSTOX_ACCESS_TOKEN:
    logging.error("Set UPSTOX_ACCESS_TOKEN in environment and re-run.")
    raise SystemExit(1)

def get_authorized_ws_url():
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    env_url = os.getenv("MARKET_FEED_AUTHORIZE_URL")
    candidates = []
    if env_url:
        candidates.append(env_url)
    candidates += [
        "https://api.upstox.com/v3/market-data-feed/authorize",
        "https://api.upstox.com/v2/market-data-feed/authorize",
        "https://api.upstox.com/v2/market-data/authorize",
        "https://api.upstox.com/v1/market-data/authorize",
    ]
    tried = set()
    for url in candidates:
        if not url or url in tried:
            continue
        tried.add(url)
        try:
            logging.info("Trying authorize URL: %s", url)
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 404:
                logging.warning("Authorize URL %s returned 404 — trying next.", url)
                continue
            r.raise_for_status()
            j = r.json()
            d = j.get("data") or j
            url_ws = d.get("socket_url") or d.get("socketUrl") or d.get("endpoint") or d.get("ws_url") or d.get("websocket")
            if url_ws:
                logging.info("Authorized websocket URL obtained from %s", url)
                return url_ws
            else:
                logging.warning("Authorize response from %s didn't include socket URL: %s", url, j)
        except Exception as e:
            logging.warning("Authorize attempt failed for %s: %s", url, e)
    logging.error("Failed to obtain authorized websocket URL from candidates.")
    return None

class UpstoxWSClient:
    def __init__(self, on_tick_callback=None):
        self._loop = None
        self._thread = None
        self.ws = None
        self._closing = False
        self.on_tick = on_tick_callback or (lambda msg: logging.info("Tick: %s", msg))
        self.subscriptions = set()
        self._pending_subscribe = set()
        self._pending_unsubscribe = set()
        self._lock = threading.Lock()
        self._connected_event = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._closing = False
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logging.info("UpstoxWSClient started background thread.")

    def stop(self):
        self._closing = True
        if self._loop:
            fut = asyncio.run_coroutine_threadsafe(self._close_ws(), self._loop)
            try:
                fut.result(timeout=5)
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=5)
        logging.info("UpstoxWSClient stopped.")

    def add_instruments(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        with self._lock:
            for k in keys:
                if k not in self.subscriptions:
                    self._pending_subscribe.add(k)

    def remove_instruments(self, keys):
        if isinstance(keys, str):
            keys = [keys]
        with self._lock:
            for k in keys:
                if k in self.subscriptions:
                    self._pending_unsubscribe.add(k)

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._main_loop())

    async def _close_ws(self):
        if self.ws:
            try:
                await self.ws.close()
            except:
                pass

    async def _main_loop(self):
        backoff = RECONNECT_BASE_DELAY
        while not self._closing:
            ws_url = get_authorized_ws_url()
            if not ws_url:
                logging.warning("No ws url; backing off %.1fs", backoff)
                await asyncio.sleep(backoff + random.random()*0.5)
                backoff = min(backoff * 2, RECONNECT_MAX_DELAY)
                continue
            try:
                ssl_ctx = ssl.create_default_context()
                logging.info("Connecting to WS: %s ...", ws_url[:80])
                async with websockets.connect(ws_url, ssl=ssl_ctx, max_size=None, ping_interval=None) as ws:
                    self.ws = ws
                    backoff = RECONNECT_BASE_DELAY
                    self._connected_event.set()
                    logging.info("WebSocket connected.")
                    consumer_task = asyncio.create_task(self._consumer_loop(ws))
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
                    await self._flush_pending_subscriptions(ws)
                    done, pending = await asyncio.wait([consumer_task, heartbeat_task], return_when=asyncio.FIRST_EXCEPTION)
                    for t in pending:
                        t.cancel()
            except Exception as e:
                logging.exception("WebSocket error: %s", e)
            self._connected_event.clear()
            self.ws = None
            if self._closing:
                break
            sleep_for = backoff + random.random() * 0.5
            logging.info("Reconnecting after %.1fs...", sleep_for)
            await asyncio.sleep(sleep_for)
            backoff = min(backoff * 2, RECONNECT_MAX_DELAY)

    async def _heartbeat_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                try:
                    await ws.ping()
                except Exception as e:
                    logging.debug("Heartbeat ping failed: %s", e)
        except asyncio.CancelledError:
            return

    async def _consumer_loop(self, ws):
        try:
            while True:
                await self._flush_pending_subscriptions(ws)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_READ_TIMEOUT)
                except asyncio.TimeoutError:
                    logging.warning("WS read timeout (%ds). Reconnecting...", WS_READ_TIMEOUT)
                    raise websockets.ConnectionClosed(1006, "read timeout")
                if isinstance(raw, (bytes, bytearray)):
                    await self._handle_binary_message(raw)
                else:
                    await self._handle_text_message(raw)
        except asyncio.CancelledError:
            return

    async def _flush_pending_subscriptions(self, ws):
        with self._lock:
            subs = list(self._pending_subscribe)
            unsubs = list(self._pending_unsubscribe)
            self._pending_subscribe.clear()
            self._pending_unsubscribe.clear()
        if unsubs:
            msg = {"action": "unsubscribe", "instruments": unsubs}
            try:
                await ws.send(json.dumps(msg))
                logging.info("Sent unsubscribe for %d instruments.", len(unsubs))
                with self._lock:
                    for k in unsubs:
                        self.subscriptions.discard(k)
            except Exception as e:
                logging.warning("Failed to send unsubscribe: %s", e)
        if subs:
            for i in range(0, len(subs), DEFAULT_SUBSCRIBE_BATCH):
                batch = subs[i:i+DEFAULT_SUBSCRIBE_BATCH]
                msg = {"action": "subscribe", "instruments": batch}
                try:
                    await ws.send(json.dumps(msg))
                    logging.info("Sent subscribe for %d instruments.", len(batch))
                    with self._lock:
                        for k in batch:
                            self.subscriptions.add(k)
                except Exception as e:
                    logging.warning("Failed to send subscribe: %s", e)
                    with self._lock:
                        for k in batch:
                            self._pending_subscribe.add(k)

    async def _handle_text_message(self, raw):
        logging.debug("WS text: %s", raw)
        try:
            j = json.loads(raw)
            if j.get("type") == "subscription_ack":
                logging.info("Subscription ack: %s", j)
            elif j.get("type") == "error":
                logging.warning("Server error: %s", j)
            else:
                self.on_tick(j)
        except Exception:
            logging.debug("WS text not JSON: %s", raw)

    async def _handle_binary_message(self, raw):
        logging.debug("WS binary message len=%d", len(raw))
        if USE_PROTO:
            try:
                fb = marketdata_pb2.FeedResponse()
                fb.ParseFromString(raw)
                msg = _proto_to_dict(fb)
                self.on_tick(msg)
            except Exception as e:
                logging.exception("Proto parse failed: %s", e)
                self.on_tick({"binary_len": len(raw)})
        else:
            snippet = raw[:64].hex()
            self.on_tick({"binary_len": len(raw), "snippet": snippet})

def _proto_to_dict(fb):
    out = {}
    try:
        if hasattr(fb, "message_type"):
            out["message_type"] = fb.message_type
        if hasattr(fb, "ticks"):
            out["ticks"] = []
            for t in fb.ticks:
                tick = {}
                if hasattr(t, "instrument_key"):
                    tick["instrument_key"] = getattr(t, "instrument_key")
                if hasattr(t, "ltp"):
                    tick["ltp"] = getattr(t, "ltp")
                if hasattr(t, "open_interest"):
                    tick["oi"] = getattr(t, "open_interest")
                out["ticks"].append(tick)
    except Exception as e:
        logging.exception("proto->dict convert error: %s", e)
    return out

if __name__ == "__main__":
    def on_tick(msg):
        logging.info("ON_TICK: %s", msg)
    client = UpstoxWSClient(on_tick_callback=on_tick)
    client.start()
    if not client._connected_event.wait(timeout=10):
        logging.warning("Didn't connect within 10s - continuing, client will keep trying in background.")
    sample_keys = ["NSE_FO|50858", "NSE_FO|52539"]
    client.add_instruments(sample_keys)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Stopping client...")
        client.stop()
