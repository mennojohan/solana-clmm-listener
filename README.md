# Solana CLMM Listener

An open-source, real-time WebSocket listener and data indexer for **Orca Whirlpool (CLMM)** pools on Solana. 

Built for researchers, quantitative analysts, and liquidity providers who want to stream raw pool state updates (`sqrt_price`, `tick_current_index`, `liquidity`, `fee_growth`) and exact swap vault deltas directly into PostgreSQL without relying on paid third-party indexers.

---

## What It Collects (Current MVP)

* **Real-Time Pool State Updates:** Streams account changes for a target Whirlpool, decoding binary state data into human-readable prices, ticks, active liquidity, and global fee growth.
* **Exact Swap Deltas:** Monitors program logs for `Swap` instructions and resolves exact `amount_in`, `amount_out`, `token_in`, and `token_out` by tracking pool vault balance changes (avoiding multi-hop wallet noise).
* **LP Activity Detection:** Detects `OpenPosition`, `IncreaseLiquidity`, and `DecreaseLiquidity` events in real time.
* **Resilient Connection Handling:** Includes dual WebSocket subscriptions (`accountSubscribe` + `logsSubscribe`), configurable RPC rate-limiting, initial state synchronization, connection pooling, and exponential backoff with jitter.
* **Built-in Data Inspector (Streamlit):** Lightweight UI to verify the live price curve and inspect raw decoded swap records at [http://localhost:8501](http://localhost:8501).

---

## Quickstart (Up & Running in < 5 Minutes)

### 1. Prerequisites
* [Docker Desktop](https://docker.com) installed and running
* A Solana RPC endpoint with WebSocket support (e.g., Helius, QuickNode, or Alchemy free tier)

### 2. Clone the Repository
```bash
git clone https://github.com/mennojohan/solana-clmm-listener.git
cd solana-clmm-listener
```

### 3. Configuration
Copy the example environment file and add your RPC endpoint:
```bash
cp .env.example .env
```

Example `.env` configuration:
```env
RPC_ENDPOINT=wss://helius-rpc.com/?api-key=YOUR_HELIUS_API_KEY_HERE
WHIRLPOOL_ADDRESS=HJPjoWUrhoZzkNfRpHuieeFk9WcZWjwy6PBjZ81ngndJ
MAX_CONCURRENT_RPC=3
RPC_DELAY_SECONDS=0.1

DB_HOST=db
DB_PORT=5432
DB_NAME=solana_clmm
DB_USER=clmm_user
DB_PASSWORD=your_secure_password
```

### 4. Build & Run with Docker
Start the PostgreSQL database, Python listener, and lightweight Streamlit data inspector in the background. The database schema and sample dataset are initialized automatically on startup:
```bash
docker compose up -d --build
```

* **Data Inspector UI:** Open [http://localhost:8501](http://localhost:8501) in your browser to inspect the price curve and decoded swap records.
* **Live Listener Logs:** View the stream in your terminal with `docker compose logs -f listener`.
* **Stop Services:** Run `docker compose down` to stop all containers while preserving your database volume.

---

## Architecture & Known MVP Trade-offs

To keep this MVP free to run on standard RPC tiers and easy to deploy locally, it intentionally makes a few architectural trade-offs:
* **WebSocket + HTTP RPC Fetching:** Swap logs trigger a rate-limited async `get_transaction` call (`MAX_CONCURRENT_RPC` + `RPC_DELAY_SECONDS`) to extract vault balance deltas. This works reliably on free RPC tiers while introducing ~0.5–1.5s latency compared to raw gRPC streams.
* **In-Memory Pool State Correlation:** Swaps are correlated with the latest cached pool state. Under extreme single-block congestion, rapid consecutive swaps may share the post-block state.
* **LP Position Bounds:** LP events are currently detected and logged, while full binary decoding of individual position tick ranges (`tick_lower`, `tick_upper`) is scheduled for Phase 2.

---

## Phase 2 Roadmap

The data pipeline in this repository serves as the core ingestion layer for an upcoming open-source CLMM analytics suite:

* **Full LP Position Decoding:** Binary decoding of `tick_arrays` and LP position instructions (`OpenPosition`, `IncreaseLiquidity`, `DecreaseLiquidity`) to track exact position bounds and liquidity distribution.
* **Quantitative CLMM Analytics Modules:** Open-source Python modules for evaluating capital efficiency, fee performance, and execution costs across concentrated liquidity pools.
* **gRPC & High-Throughput Scaling:** Optional Yellowstone gRPC ingestion and Redis buffering for multi-pool indexing without RPC rate-limit bottlenecks.

---

## License

MIT License. Free to use, modify, and build upon for your own research and tools.
