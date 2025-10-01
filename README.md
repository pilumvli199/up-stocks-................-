# Upstox Option Chain Poller & WebSocket Client

This bundle contains:
- `option_chain_poller.py` — REST-based option chain poller with robust batching and rotation to avoid rate-limits.
- `upstox_ws_feed.py` — Resilient WebSocket client to subscribe to Upstox market-data feed and receive real-time ticks.
- `requirements.txt` — Python dependencies.
- `.env.example` — example environment variables.
- `MarketData.proto.sample` — placeholder proto (you must obtain real proto from Upstox docs).

## Quick start

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and fill your credentials:
   ```bash
   cp .env.example .env
   # edit .env and fill UPSTOX_ACCESS_TOKEN, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (optional)
   ```

3. If you want protobuf decoding:
   - Download `MarketData.proto` from Upstox docs and run:
     ```bash
     protoc --python_out=. MarketData.proto
     ```
     This will create `marketdata_pb2.py` used by `upstox_ws_feed.py`.

4. Run REST poller:
   ```bash
   python3 option_chain_poller.py
   ```

5. Run WebSocket client:
   ```bash
   python3 upstox_ws_feed.py
   ```

## Notes
- Adjust `MAX_KEYS_PER_POLL`, `BATCH_SIZE`, and `POLL_INTERVAL` in `.env` to tune for rate-limits.
- WebSocket authorize endpoint can differ by account/region; if you get 404, check Upstox docs or set `MARKET_FEED_AUTHORIZE_URL` in `.env`.
- Keep your tokens secret.
