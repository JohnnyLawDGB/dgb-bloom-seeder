# DGB Bloom Seeder

Crawls the DigiByte P2P network, discovers nodes that serve **compact block filters (BIP157/158, `NODE_COMPACT_FILTERS`)**, and serves that peer list via a lightweight JSON API. Legacy BIP37 bloom (`NODE_BLOOM`) support has been retired — the seeder is now compact-filter-only. Built to support the [DigiByte Android Wallet](https://github.com/JohnnyLawDGB/digibytewallet-android) and any other light client that needs filter-capable peers.

## The Problem

Light wallets — like mobile wallets — can't download the full blockchain, so they sync from peers that serve **compact block filters (BIP157/158)**: the node offers one compact filter per block, the wallet decides locally which blocks to fetch, and it never has to reveal its addresses. But a node only advertises `NODE_COMPACT_FILTERS` when its operator has explicitly enabled the filter index (`blockfilterindex=basic` + `peerblockfilters=1`), and most haven't. The older mechanism — BIP37 **bloom filters** (`NODE_BLOOM`) — has been retired: it's disabled by default on modern nodes and is being phased out for privacy reasons. A wallet booting from generic DNS seeds mostly lands on nodes it can't sync from.

This seeder solves that by continuously crawling the network, verifying which nodes actually serve compact block filters, and exposing that short, ranked list via a simple API — so wallets always have filter-capable peers to reach.

## How It Helps Mobile Wallet Development

The [DigiByte Android Wallet](https://github.com/JohnnyLawDGB/digibytewallet-android) is moving to **BIP157/158 compact block filters** for private, bandwidth-light sync. Today it leans on a single hardcoded filter-serving node (`digiscope.me`). If that node goes down, wallets lose their sync peer.

The seeder provides:
- **Redundancy** — multiple filter-serving peers instead of a single point of failure
- **Decentralization** — peers discovered across the network, not hardcoded
- **Auto-discovery** — the wallet periodically fetches fresh filter peers from the API
- **Community participation** — anyone can run a seeder, or just enable filters on their node, to contribute peers

### Wallet Integration (Planned)

The Android wallet will integrate with this seeder by:
1. Fetching `GET /peers` from the seeder API once per hour (cached locally) — compact-filter peers
2. Injecting the returned filter-capable peers into the SPV peer manager on each sync start
3. Falling back to `digiscope.me` if the API is unreachable

## Live Instance

A seeder runs at `digiscope.me` and is available at:

```
https://api.digiscope.me/api/peers          # Filter peer list
https://api.digiscope.me/api/peers/stats    # Crawl statistics
```

`/api/peers/bloom` and `/api/peers/all` still resolve (nginx aliases kept for backwards
compatibility) but now transparently serve the same filter peer list — see the API
section below.

## Quick Start

```bash
git clone https://github.com/JohnnyLawDGB/dgb-bloom-seeder.git
cd dgb-bloom-seeder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 seeder.py
```

The seeder will:
1. Resolve peers from DigiByte DNS seeds
2. Connect to each peer, perform a P2P version handshake
3. Check the `NODE_COMPACT_FILTERS` bit (0x40) in the service flags, and confirm it with a `getcfheaders` round-trip
4. Store filter-validated peers in a local SQLite database
5. Serve discovered peers at `http://localhost:8025/peers`
6. Re-crawl every 30 minutes, pruning stale peers after 24 hours

## API

The seeder serves a single capability: **compact block filters**. The `?capability=`
parameter no longer selects a protocol — any value passed (empty, `filter`, or a legacy
value like `bloom`/`dandelion`) is accepted and ignored (every request returns the filter
list). Nothing 400s or 404s.

### `GET /peers`

Returns the seeder's filter-validated peers above the inclusion threshold, ranked
highest-confidence first by a composite score that blends Bayesian-smoothed 7-day
reliability with a longevity bonus.

```json
{
    "peers": [
        {
            "ip": "129.212.182.152",
            "port": 12024,
            "services": 1101,
            "services_hex": "0x44d",
            "capabilities": ["NETWORK", "BLOOM", "WITNESS", "COMPACT_FILTERS", "NETWORK_LIMITED"],
            "user_agent": "/DigiByte:8.26.2/",
            "last_seen": 1746876472,
            "first_seen": 1741000000,
            "protocol_version": 70019,
            "bloom_validated_at": 1746876472,
            "filter_validated_at": 1746876472,
            "peer_capability": "filter",
            "uptime_score": 0.94,
            "composite_score": 1.18,
            "attempts_7d": 312,
            "successes_7d": 298,
            "tenure_days": 33.6
        }
    ],
    "count": 1,
    "capability": "filter",
    "crawl_age_seconds": 120
}
```

The response-level `capability` field is always `"filter"` — kept for backwards
compatibility with clients that already read it. Per-peer `peer_capability` is likewise
always `"filter"`. The `capabilities` array is an honest readout of the peer's advertised
service bits, so it still lists `BLOOM` for nodes that happen to advertise that bit —
that's just describing the wire, not a second capability the seeder validates.

`/api/peers/bloom` and `/api/peers/all` (and any other legacy nginx alias) hit this same
endpoint and return the identical filter peer list — they never error, they just no
longer carry bloom-specific meaning.

### `GET /stats`

Health check and crawl statistics.

```json
{
    "peers_total": 42,
    "peers_filter_validated": 6,
    "peers_filter_above_threshold": 5,
    "all_peers_known": 12000,
    "attempts_7d_total": 8342,
    "last_crawl": 1743900000,
    "uptime_seconds": 86400
}
```

- `peers_filter_validated` — number of peers ever confirmed to serve compact filters
- `peers_filter_above_threshold` — number that would currently appear in `/peers`
- `attempts_7d_total` — total crawl-attempt rows recorded in the rolling 7-day window

There are no `peers_bloom_*` keys — bloom counts were dropped along with bloom support.

## Running Your Own Seeder

Anyone can run their own seeder to help decentralize the wallet's compact-filter (BIP158) infrastructure. It speaks the DigiByte P2P protocol directly and confirms each candidate actually serves filters with a BIP157 `getcfheaders` round-trip before listing it.

### Requirements

- Python 3.10+
- A server with a static IP and outbound TCP access to port 12024
- No DigiByte Core node required — the seeder speaks the P2P protocol directly

### Configuration

Edit `config.yaml`:

```yaml
crawl_interval: 1800     # Seconds between crawls (30 min)
crawl_concurrency: 10    # Simultaneous peer connections
crawl_max_peers: 500     # Peers to check per cycle
api_port: 8025           # HTTP API port
prune_hours: 24          # Remove peers not seen in this window
```

### Upgrading from a bloom-aware seeder

If you previously ran a bloom-only or capability-aware (filter+bloom) version of this
seeder, upgrading to this compact-filter-only version is a plain code deploy:

1. **Schema migration is automatic and already behind you.** `Storage.init()` still
   migrates any legacy `bloom_peers` → `peers` tables on first start, but the bloom
   columns and index are now dormant — nothing new to run.
2. **No nginx changes needed.** The API ignores any `?capability=` query parameter and
   always returns the filter peer list, so `/api/peers/bloom` and `/api/peers/all` keep
   working exactly as configured — they just now serve filter peers instead of bloom
   peers.
3. **`/stats` no longer reports `peers_bloom_*` keys.** Update any dashboards or
   monitoring that reads those fields.

Back up `bloom_seeder.db` before deploying as a precaution, even though the DB schema
itself is unchanged.

### Deployment

With PM2:
```bash
pm2 start seeder.py --name bloom-seeder --interpreter /path/to/venv/bin/python3
pm2 save
```

With systemd:
```ini
[Unit]
Description=DGB Bloom Seeder
After=network.target

[Service]
ExecStart=/opt/dgb-bloom-seeder/.venv/bin/python3 /opt/dgb-bloom-seeder/seeder.py
WorkingDirectory=/opt/dgb-bloom-seeder
Restart=always

[Install]
WantedBy=multi-user.target
```

### Getting Your Seeder Included in the Mobile Wallet

The Android wallet hardcodes seeder API URLs for peer discovery. To get your seeder included:

1. **Deploy your seeder** on a server with a static IP and stable uptime
2. **Put it behind HTTPS** (the wallet requires TLS for API calls)
3. **Verify it's working** — your `/peers` endpoint should return filter-capable peers
4. **Open a PR** to [digibytewallet-android](https://github.com/JohnnyLawDGB/digibytewallet-android) adding your seeder URL to the peer discovery configuration
5. Include in your PR:
   - Your seeder's HTTPS URL
   - Server location / uptime commitment
   - Whether you also run a filter-enabled DigiByte node (`peerblockfilters=1`)

The more seeders in the wallet's configuration, the more resilient the mobile wallet infrastructure becomes.

## For Node Operators

You don't need to run a seeder to help — the most valuable thing you can do is serve **compact block filters (BIP157/158)** on your existing DigiByte Core node. That makes it discoverable by seeders and directly usable by modern light wallets.

Add this to your `digibyte.conf`:

```
blockfilterindex=basic    # build the BIP158 block-filter index
peerblockfilters=1        # serve those filters to light-client peers
```

Then restart your node. The index builds once (a few minutes to ~an hour, depending on hardware and disk), after which your node advertises `NODE_COMPACT_FILTERS` and seeders will validate it with a BIP157 `getcfheaders` round-trip.

**Verify it's working:**
```bash
digibyte-cli getindexinfo                                    # "basic block filter index" → synced: true
digibyte-cli getnetworkinfo | grep -A8 localservicesnames    # should list COMPACT_FILTERS
```

**Config file locations:**
- **Linux:** `~/.digibyte/digibyte.conf`
- **macOS:** `~/Library/Application Support/DigiByte/digibyte.conf`
- **Windows:** `%APPDATA%\DigiByte\digibyte.conf`

> **Do not set `peerbloomfilters=1`.** BIP37 bloom filters are retired; this seeder no
> longer discovers or serves bloom peers, so there's no benefit to enabling it.

### Trade-offs

**Benefits:**
- Serves modern light/mobile wallets across the DigiByte network over BIP158
- Modest, bounded cost — the filter index is compact and built once, then grows only with new blocks
- Helps decentralize light-client infrastructure so it no longer hangs on a handful of nodes

**Considerations:**
- Disk: the BIP158 filter index is **~4 GB — roughly +7%** more than a node without it (measured on a full DigiByte node; same on 8.26 and 9.26), plus a one-time CPU build. **RAM is effectively unchanged** — the index is disk-backed and read on demand
- Slightly increased bandwidth serving filter headers and filters to light clients
- Compact filters are **more private than the retired bloom filters**: the client downloads filters and decides locally what to request, instead of handing the node a bloom filter of its own addresses.

## Architecture

```
DNS Seeds → Crawler → P2P Handshake → Verify NODE_COMPACT_FILTERS (getcfheaders) → SQLite → HTTP API
                ↑                                                                      |
                └──────────────────────── getaddr (discover more peers) ───────────────┘
```

- **`seeder/protocol.py`** — DigiByte P2P message encoding/decoding (version, verack, getaddr, addr)
- **`seeder/crawler.py`** — Async TCP crawler with configurable concurrency
- **`seeder/storage.py`** — SQLite storage for filter peers and crawl queue
- **`seeder/api.py`** — aiohttp HTTP server (`/peers`, `/stats`)
- **`seeder/config.py`** — YAML config loader
- **`seeder.py`** — Entry point

## Tests

```bash
source .venv/bin/activate
python3 -m pytest tests/ -v
```

57 tests covering P2P protocol encoding/decoding, SQLite storage, schema migration, filter-peer ranking, crawler attempt logging, and HTTP API endpoints.

## Dependencies

- `aiohttp` — async HTTP server
- `aiosqlite` — async SQLite
- `pyyaml` — config parsing
- No DigiByte Core RPC dependency — pure P2P protocol implementation

## License

MIT
