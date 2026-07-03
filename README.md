# DGB Bloom Seeder

Crawls the DigiByte P2P network, discovers nodes that serve **compact block filters (BIP157/158, `NODE_COMPACT_FILTERS`)** — with legacy BIP37 bloom (`NODE_BLOOM`) as a fallback — and serves that peer list via a lightweight JSON API. Built to support the [DigiByte Android Wallet](https://github.com/JohnnyLawDGB/digibytewallet-android) and any other light client that needs filter-capable peers.

## The Problem

Light wallets — like mobile wallets — can't download the full blockchain, so they sync from peers that serve **compact block filters (BIP157/158)**: the node offers one compact filter per block, the wallet decides locally which blocks to fetch, and it never has to reveal its addresses. But a node only advertises `NODE_COMPACT_FILTERS` when its operator has explicitly enabled the filter index (`blockfilterindex=basic` + `peerblockfilters=1`), and most haven't. The older mechanism — BIP37 **bloom filters** (`NODE_BLOOM`) — is disabled by default on modern nodes (`peerbloomfilters=0`) and is being retired for privacy reasons. Either way, a wallet booting from generic DNS seeds mostly lands on nodes it can't sync from.

This seeder solves that by continuously crawling the network, verifying which nodes actually serve compact block filters (and, as a fallback, bloom filters), and exposing that short, ranked list via a simple API — so wallets always have filter-capable peers to reach.

## How It Helps Mobile Wallet Development

The [DigiByte Android Wallet](https://github.com/JohnnyLawDGB/digibytewallet-android) is moving to **BIP157/158 compact block filters** for private, bandwidth-light sync (with legacy BIP37 bloom kept as a fallback for older builds). Today it leans on a single hardcoded filter-serving node (`digiscope.me`). If that node goes down, wallets lose their sync peer.

The seeder provides:
- **Redundancy** — multiple filter-serving peers instead of a single point of failure
- **Decentralization** — peers discovered across the network, not hardcoded
- **Auto-discovery** — the wallet periodically fetches fresh filter peers from the API
- **Community participation** — anyone can run a seeder, or just enable filters on their node, to contribute peers

### Wallet Integration (Planned)

The Android wallet will integrate with this seeder by:
1. Fetching `GET /peers` from the seeder API once per hour (cached locally) — compact-filter peers by default
2. Injecting the returned filter-capable peers into the SPV peer manager on each sync start
3. Falling back to `digiscope.me` if the API is unreachable

## Live Instance

A seeder runs at `digiscope.me` and is available at:

```
https://api.digiscope.me/api/peers/bloom    # Bloom peer list
https://api.digiscope.me/api/peers/stats    # Crawl statistics
```

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
3. Check the `NODE_BLOOM` bit (0x04) in the service flags
4. Store bloom-capable peers in a local SQLite database
5. Serve discovered peers at `http://localhost:8025/peers`
6. Re-crawl every 30 minutes, pruning stale peers after 24 hours

## API

### `GET /peers`

Returns the seeder's best capability-validated peers. With no parameter the default is **block-filter peers (BIP 158)**; if there are no filter peers above threshold, the seeder falls through to bloom peers. Capability can be specified explicitly:

| Query | Returns |
|---|---|
| `GET /peers` | filter peers above threshold; falls through to bloom if empty |
| `GET /peers?capability=filter` | filter peers above threshold |
| `GET /peers?capability=bloom` | bloom peers above threshold |
| `GET /peers?capability=filter\|bloom` | filter peers (ranked first) followed by bloom peers (ranked separately) |

Highest-confidence peers appear first, ranked by a composite score that blends Bayesian-smoothed 7-day reliability with a longevity bonus.

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

The response-level `capability` field reports which list the wallet got (`"filter"`, `"bloom"`, or `"filter+bloom"`). Per-peer `peer_capability` reports which capability that row's score reflects.

### `GET /stats`

Health check and crawl statistics.

```json
{
    "peers_total": 42,
    "peers_bloom_validated": 18,
    "peers_filter_validated": 6,
    "peers_bloom_above_threshold": 15,
    "peers_filter_above_threshold": 5,
    "all_peers_known": 12000,
    "attempts_7d_total": 8342,
    "last_crawl": 1743900000,
    "uptime_seconds": 86400
}
```

- `peers_bloom_validated` / `peers_filter_validated` — number of peers ever confirmed for that capability
- `peers_bloom_above_threshold` / `peers_filter_above_threshold` — number that would currently appear in `/peers?capability=...`
- `attempts_7d_total` — total crawl-attempt rows recorded in the rolling 7-day window

## Running Your Own Seeder

Anyone can run their own seeder to help decentralize the wallet's compact-filter (BIP158) infrastructure. It speaks the DigiByte P2P protocol directly and confirms each candidate actually serves filters with a BIP157 `getcfheaders` round-trip (and, for legacy peers, a bloom `filterload` probe) before listing it.

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

### Upgrading from a bloom-only seeder

If you previously ran an older bloom-only version of this seeder (where `/peers` returned bloom peers by default), three things need attention on upgrade:

1. **Schema migration is automatic.** On first start with the new code, `Storage.init()` migrates `bloom_peers` → `peers` and `bloom_peer_attempts` → `peer_attempts` inside a single transaction. Back up `bloom_seeder.db` before the deploy so a rollback can restore it.
2. **The default `/peers` response changes.** With no `?capability=` parameter, the new code serves *block-filter* peers (falling through to bloom only if no filter peers are above threshold). Older wallets that expect bloom must hit `?capability=bloom` explicitly.
3. **nginx must be updated.** If you reverse-proxy `/api/peers/bloom` → `:8025/peers`, change the upstream to `:8025/peers?capability=bloom`. Without this change, in-the-wild wallets that hit `/api/peers/bloom` will start receiving filter peers and log unsupported-mode errors.

```nginx
# Before:
location /api/peers/bloom { proxy_pass http://localhost:8025/peers; }
# After:
location /api/peers/bloom { proxy_pass http://localhost:8025/peers?capability=bloom; }
```

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

> **Legacy (BIP37 bloom).** Older SPV clients still use bloom filters. To serve them as well, also add `peerbloomfilters=1`. Bloom is off by default and is gradually being retired in favor of compact filters, so treat it as optional — enable it only if you specifically want to support legacy wallets.

### Trade-offs

**Benefits:**
- Serves modern light/mobile wallets across the DigiByte network over BIP158
- Modest, bounded cost — the filter index is compact and built once, then grows only with new blocks
- Helps decentralize light-client infrastructure so it no longer hangs on a handful of nodes

**Considerations:**
- A one-time disk + CPU cost to build the block-filter index, then a small ongoing footprint as blocks arrive
- Slightly increased bandwidth serving filter headers and filters to light clients
- Compact filters are **more private than bloom**: the client downloads filters and decides locally what to request, instead of handing the node a bloom filter of its own addresses. (If you also enable legacy bloom, note that bloom-filter analysis can fingerprint wallet addresses — a risk mitigated by Tor and Dandelion++, both planned for the mobile wallet.)

## Architecture

```
DNS Seeds → Crawler → P2P Handshake → Check NODE_BLOOM → SQLite → HTTP API
                ↑                           |
                └── getaddr (discover more peers)
```

- **`seeder/protocol.py`** — DigiByte P2P message encoding/decoding (version, verack, getaddr, addr)
- **`seeder/crawler.py`** — Async TCP crawler with configurable concurrency
- **`seeder/storage.py`** — SQLite storage for bloom peers and crawl queue
- **`seeder/api.py`** — aiohttp HTTP server (`/peers`, `/stats`)
- **`seeder/config.py`** — YAML config loader
- **`seeder.py`** — Entry point

## Tests

```bash
source .venv/bin/activate
python3 -m pytest tests/ -v
```

61 tests covering P2P protocol encoding/decoding, SQLite storage, schema migration, per-capability ranking, crawler attempt logging, and HTTP API endpoints.

## Dependencies

- `aiohttp` — async HTTP server
- `aiosqlite` — async SQLite
- `pyyaml` — config parsing
- No DigiByte Core RPC dependency — pure P2P protocol implementation

## License

MIT
