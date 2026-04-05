# DGB Bloom Seeder — Design Spec

## Goal

Standalone service that crawls the DigiByte P2P network, identifies nodes with bloom filter support (`NODE_BLOOM`), and serves them via a lightweight JSON API for SPV wallets.

## Problem

DigiByte v8.26 nodes default to `peerbloomfilters=0`, rejecting SPV wallet connections. The Android wallet currently depends on a single bloom-capable node (`digiscope.me`). If that node goes down, all SPV wallets stop syncing. A discovery service that maintains a list of bloom-capable peers across the network provides redundancy and decentralization.

## Architecture

Single Python process with two async loops:

1. **Crawler loop** — connects to peers, performs P2P version handshake, checks `NODE_BLOOM` flag, discovers more peers via `getaddr`
2. **API server** — serves discovered bloom peers as JSON over HTTP

Both run concurrently via `asyncio`. Data stored in SQLite.

## Crawler

### Peer Discovery

Initial peers come from DigiByte DNS seeds:
- `seed.digibyte.io`
- `seed2.digibyte.io`
- `seed.digibyteprojects.com`
- `digibyteblockexplorer.com`
- `dgbseed.org`

DNS resolution returns IP addresses. These are added to the crawl queue.

### P2P Handshake

For each peer in the queue:

1. Open TCP connection to `ip:12024` (mainnet P2P port) with 5-second timeout
2. Send a `version` message:
   - Protocol version: `70019`
   - Services: `0x00` (we offer nothing)
   - Timestamp: current Unix time
   - Receiver/sender address: zeroed
   - Nonce: random 8 bytes
   - User agent: `/DGB-Bloom-Seeder:1.0/`
   - Start height: `0`
   - Relay: `false`
3. Read the peer's `version` message back
4. Extract `services` field (uint64_t LE at byte offset 4 of the version payload)
5. If `services & 0x04` (NODE_BLOOM bit): store as bloom-capable
6. Send `verack`
7. Send `getaddr` to request the peer's known addresses
8. Read `addr` response — add new peers to the crawl queue
9. Disconnect

### DigiByte P2P Message Format

```
[4 bytes] magic: 0xFAC3B6DA (DigiByte mainnet)
[12 bytes] command: null-padded ASCII (e.g., "version\x00\x00\x00\x00\x00")
[4 bytes] payload length: uint32 LE
[4 bytes] checksum: first 4 bytes of SHA256(SHA256(payload))
[N bytes] payload
```

### Version Message Payload

```
[4 bytes] protocol version: int32 LE (70019)
[8 bytes] services: uint64 LE (0x00 for seeder)
[8 bytes] timestamp: int64 LE
[26 bytes] addr_recv: (services + ip + port)
[26 bytes] addr_from: (services + ip + port)
[8 bytes] nonce: uint64 LE (random)
[varint + string] user_agent
[4 bytes] start_height: int32 LE
[1 byte] relay: bool
```

### Service Flags

```
NODE_NETWORK = 0x01
NODE_BLOOM   = 0x04
NODE_WITNESS = 0x08
```

A peer with `services & 0x04 != 0` supports bloom filters.

### Crawl Schedule

- Full crawl every 30 minutes (configurable via `crawl_interval`)
- Each crawl processes up to 500 peers from the queue
- 10 concurrent connections (configurable via `crawl_concurrency`)
- 5-second timeout per connection
- Peers not seen in 24 hours are pruned (configurable via `prune_hours`)

## Storage

SQLite database (`bloom_seeder.db`), single table:

```sql
CREATE TABLE bloom_peers (
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    services INTEGER NOT NULL,
    protocol_version INTEGER,
    user_agent TEXT,
    last_seen INTEGER NOT NULL,
    first_seen INTEGER NOT NULL,
    PRIMARY KEY (ip, port)
);

CREATE INDEX idx_bloom_last_seen ON bloom_peers(last_seen);
```

Only peers with `NODE_BLOOM` set are stored. Non-bloom peers are checked but not persisted.

The `all_peers` table tracks all discovered peers for the crawl queue:

```sql
CREATE TABLE all_peers (
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    last_crawled INTEGER DEFAULT 0,
    PRIMARY KEY (ip, port)
);
```

## API

Built-in HTTP server using `aiohttp` (lightweight, async, no framework overhead).

### `GET /peers`

Returns bloom-capable peers seen within the last 6 hours, sorted by most recently seen.

**Response:**
```json
{
    "peers": [
        {
            "ip": "134.199.198.90",
            "port": 12024,
            "last_seen": 1743900000,
            "protocol_version": 70019,
            "user_agent": "/DigiByte:8.26.0/"
        }
    ],
    "count": 42,
    "crawl_age_seconds": 120
}
```

- `count`: total bloom peers in response
- `crawl_age_seconds`: seconds since last crawl completed (so consumers know freshness)
- Max results: 25 (configurable via `api_max_results`)
- No authentication required — public data

### `GET /stats`

Lightweight health check / stats endpoint.

```json
{
    "bloom_peers_total": 42,
    "bloom_peers_6h": 38,
    "all_peers_known": 1250,
    "last_crawl": 1743900000,
    "uptime_seconds": 86400
}
```

### Server Config

- Default port: `8025`
- Listens on `0.0.0.0` (configurable)
- No TLS — expected to sit behind nginx reverse proxy for HTTPS

## Configuration

`config.yaml`:

```yaml
# Network
dgb_port: 12024              # DigiByte mainnet P2P port
dgb_magic: "fac3b6da"        # Mainnet magic bytes

# Crawler
crawl_interval: 1800          # Seconds between crawls (30 min)
crawl_concurrency: 10         # Max simultaneous connections
crawl_timeout: 5              # Seconds per connection
crawl_max_peers: 500          # Max peers to check per crawl cycle
prune_hours: 24               # Remove peers not seen in this many hours

# API
api_port: 8025
api_host: "0.0.0.0"
api_max_results: 25
api_max_age_hours: 6          # Only return peers seen within this window

# Database
db_path: "bloom_seeder.db"

# Logging
log_level: "INFO"
```

## Wallet Integration

The Android wallet adds a periodic peer list refresh:

1. Every 60 minutes (configurable), `SyncService` fetches `GET /peers` from a configured URL (default: `https://api.digiscope.me/api/peers/bloom` proxied to the seeder)
2. Response is cached in SharedPreferences as JSON
3. On each `startSync`, the cached peer list is injected alongside `digiscope.me` via `_injectPriorityPeer`
4. If the API is unreachable, the wallet falls back to `digiscope.me` only (current behavior)
5. Cached peers are used until the next successful refresh

This is a separate change to the Android wallet, implemented after the seeder is deployed and confirmed working.

## File Structure

```
dgb-bloom-seeder/
├── seeder.py           # Entry point — starts crawler + API
├── crawler.py          # P2P handshake, peer discovery, crawl loop
├── protocol.py         # DigiByte P2P message encoding/decoding
├── storage.py          # SQLite operations
├── api.py              # HTTP API server
├── config.yaml         # Default configuration
├── requirements.txt    # aiohttp, aiosqlite, pyyaml
├── README.md           # Setup, usage, deployment instructions
└── tests/
    ├── test_protocol.py    # Message encoding/decoding tests
    └── test_crawler.py     # Handshake parsing tests
```

## Deployment

```bash
git clone https://github.com/JohnnyLawDGB/dgb-bloom-seeder.git
cd dgb-bloom-seeder
pip install -r requirements.txt
python3 seeder.py
```

Or with PM2:
```bash
pm2 start seeder.py --name bloom-seeder --interpreter python3
```

On `digiscope.me`, nginx proxies `api.digiscope.me/api/peers/bloom` → `localhost:8025/peers`.

## Dependencies

- Python 3.10+
- `aiohttp` — async HTTP server
- `aiosqlite` — async SQLite
- `pyyaml` — config parsing
- No DigiByte Core RPC dependency — pure P2P protocol

## What's NOT in Scope

- Testnet support (could be added later via config)
- Peer reputation scoring
- Geographic distribution analysis
- Web dashboard
- Authentication / rate limiting (nginx handles this)
- Wallet integration changes (separate project after seeder is deployed)
