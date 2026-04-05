# DGB Bloom Seeder

Discovers DigiByte nodes with bloom filter support (`NODE_BLOOM`) and serves them via a lightweight JSON API. Built for SPV wallets that need bloom-capable peers.

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
3. Store peers that advertise `NODE_BLOOM` in their service flags
4. Serve discovered peers at `http://localhost:8025/peers`
5. Re-crawl every 30 minutes

## API

### `GET /peers`

Returns bloom-capable peers seen in the last 6 hours.

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
    "count": 1,
    "crawl_age_seconds": 120
}
```

### `GET /stats`

Health check and crawl statistics.

```json
{
    "bloom_peers_total": 42,
    "bloom_peers_recent": 38,
    "all_peers_known": 5000,
    "last_crawl": 1743900000,
    "uptime_seconds": 86400
}
```

## Configuration

Edit `config.yaml`:

```yaml
crawl_interval: 1800     # Seconds between crawls
crawl_concurrency: 10    # Simultaneous peer connections
crawl_max_peers: 500     # Peers to check per cycle
api_port: 8025           # HTTP API port
prune_hours: 24          # Remove peers not seen in this window
```

## Deployment

With PM2:
```bash
pm2 start seeder.py --name bloom-seeder --interpreter python3
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

## Why This Exists

DigiByte v8.26 nodes default to `peerbloomfilters=0`, which means they reject SPV wallet connections. Mobile wallets using bloom filters need to find the minority of nodes that have bloom enabled. This seeder automates that discovery.

**Node operators:** You can help mobile wallets by adding `peerbloomfilters=1` to your `digibyte.conf` and restarting your node.

## Tests

```bash
source .venv/bin/activate
python3 -m pytest tests/ -v
```

## License

MIT
