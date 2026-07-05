# Mobile Wallet Integration — digiscope.me Peer API

A short guide for the Android wallet team on how to consume the seeder.

> **Bloom (BIP 37) support has been retired.** The seeder now discovers, validates, and
> serves **compact-filter (BIP 157/158) peers only**. `/api/peers/bloom` and
> `/api/peers/all` are **deprecated**: they still resolve and never error, but they now
> transparently return the same filter peer list as `/api/peers`. Use `/api/peers` (or
> `/api/peers/filter`, equivalently `?capability=filter`) going forward.

## Production endpoints

All endpoints are HTTPS, GET, no auth required, public data.

| URL | Returns | When to use |
|---|---|---|
| `https://api.digiscope.me/api/peers` | Block-filter (BIP 157/158) peers above threshold. Single ranked list. | **Recommended default** for all wallet builds. |
| `https://api.digiscope.me/api/peers/filter` | Same as above, via the explicit path or `?capability=filter`. | Equivalent to the default; use if you prefer an explicit capability in the URL. |
| `https://api.digiscope.me/api/peers/bloom` | **Deprecated.** Soft-aliases to the same filter peer list — bloom is no longer discovered or served. | Only for old clients still hardcoded to this URL; migrate to `/api/peers` when possible. |
| `https://api.digiscope.me/api/peers/all` | **Deprecated.** Soft-aliases to the same filter peer list. | Same as above — no longer returns a combined filter+bloom set. |
| `https://api.digiscope.me/api/peers/stats` | Crawl statistics and filter-validated counts. | Operator dashboards / health checks. Not for wallet runtime. |

No `?capability=...` value ever errors — the raw seeder (`:8025/peers`) and every named
nginx route above ignore the parameter entirely and always return filter peers. Legacy
values (`bloom`, `dandelion`, `filter|bloom`, anything else) are accepted and quietly
treated the same as no parameter at all.

## Response shape

Every `/api/peers*` endpoint returns the same JSON shape:

```json
{
    "peers": [ ... ],
    "count": 17,
    "capability": "filter",
    "crawl_age_seconds": 120
}
```

- `peers` — array, ordered highest-confidence first
- `count` — `len(peers)`; may be 0 if no filter peers are currently above threshold
- `capability` — always `"filter"`. Kept in the response for backwards compatibility with clients that already read it; there is no other value anymore.
- `crawl_age_seconds` — seconds since the last completed crawl. Crawl cadence is 30 minutes; values up to ~1800 are normal.

Each peer object:

```json
{
    "ip": "174.131.163.123",
    "port": 12024,
    "services": 1101,
    "services_hex": "0x44d",
    "capabilities": ["NETWORK", "BLOOM", "WITNESS", "COMPACT_FILTERS", "NETWORK_LIMITED"],
    "user_agent": "/DigiByte:8.26.2/",
    "last_seen": 1778441288,
    "first_seen": 1778441288,
    "protocol_version": 70019,
    "bloom_validated_at": 1778441288,
    "filter_validated_at": 1778441288,
    "peer_capability": "filter",
    "uptime_score": 0.5454,
    "composite_score": 0.5454,
    "attempts_7d": 1,
    "successes_7d": 1,
    "tenure_days": 0.001
}
```

Fields the wallet should care about:

| Field | Type | Use |
|---|---|---|
| `ip`, `port` | string, int | The peer to connect to. |
| `peer_capability` | always `"filter"` | Kept for backwards compatibility with clients that already branch on it; every peer in every response is now filter-validated. |
| `capabilities` | array | Human-readable service flag names the peer advertised. Always reflects exactly what was on the wire — may still include `"BLOOM"` if the node happens to advertise that bit, but the seeder no longer validates or routes on it. |
| `services` / `services_hex` | int / hex string | Raw service-flag bitmask. |
| `bloom_validated_at` | int (unix) or null | Legacy field, retained for schema compatibility. The seeder no longer performs bloom validation, so this stops advancing; treat it as historical/frozen. |
| `filter_validated_at` | int (unix) or null | Last time the seeder confirmed this peer serves compact filters via `getcfheaders`. |
| `last_seen` | int (unix) | Last successful version handshake. |
| `uptime_score` | float 0..1 | Bayesian-smoothed 7-day reliability (centered at 0.5 with a prior of 5 successes / 10 attempts). |
| `composite_score` | float | Final ranking score. Same as `uptime_score × (1 + 0.30 × min(tenure_days/60, 1.0))`. **Higher is better; the array is already sorted by this.** |
| `tenure_days` | float | How long this peer has been continuously known to the seeder. |
| `user_agent` | string | Peer's advertised user-agent. Useful for debugging but not for routing. |

The wallet can safely ignore any field it doesn't recognize; the seeder may add fields without notice.

## Recommended client behavior

1. **Default endpoint:** hit `https://api.digiscope.me/api/peers` (or `/api/peers/filter` / `?capability=filter` — all equivalent). Every response is filter peers; the response-level `capability` field is always `"filter"`.
2. **Refresh cadence:** once per hour, on app foreground, and on each manual sync. Hourly is generous given the 30-min seeder crawl cadence; caching tighter (e.g., 5 min) is fine but pointless.
3. **Caching:** store the JSON response in SharedPreferences. Use cached peers on every sync start; refresh in the background.
4. **Picking peers:** use the first N entries of `peers` (the seeder caps the response at 25). The list is already ranked; just slice from the top.
5. **Connection routing:** all peers are filter (BIP 158) peers now, so there's no per-peer protocol branching to do — `peer_capability` is always `"filter"`. If a wallet build only speaks BIP 37, it can no longer get bloom peers from this seeder (see deprecation note above).
6. **Fallback chain on API failure:**
   - First: use the cached response from SharedPreferences if it's < 24 hours old
   - Then: hardcoded `digiscope.me:12024` (current v3.5.38 behavior)
   - Last resort: DigiByte DNS seeds
7. **Don't retry tightly.** If `api.digiscope.me` returns a 5xx, back off 30+ seconds. The seeder restart can take ~4 minutes during which time the public endpoints return 502.

## Notes for v3.5.38 wallets in the wild

- They keep hitting `https://api.digiscope.me/api/peers/bloom`. This URL still resolves and still returns a 200 with the standard peer-list shape, but the peers in it are now filter (BIP 158) peers, not bloom peers — a v3.5.38 wallet that only speaks BIP 37 will not be able to sync against them over that protocol. Bloom has been fully retired seeder-side; there is no bloom peer list left to serve. Migrating these wallets to a filter-capable sync path is tracked separately.
- They don't know about `peer_capability` / `capabilities` / `services_hex` / `filter_validated_at` — they'll see them as extra unknown JSON fields and ignore them. No JSON-parsing break.
- Existing peer payloads continue to include `services`, `user_agent`, `last_seen`, `first_seen`, `protocol_version`, `uptime_score`, `composite_score`, `attempts_7d`, `successes_7d`, `tenure_days` — same shape as before this upgrade.

## Example calls

```bash
# Default (filter peers)
curl -s https://api.digiscope.me/api/peers | jq '.capability, .count'

# Filter peers, explicit path
curl -s https://api.digiscope.me/api/peers/filter | jq '.peers[] | {ip, port, peer_capability, composite_score}'

# Deprecated aliases — same filter peer list as above
curl -s https://api.digiscope.me/api/peers/bloom | jq '.count'
curl -s https://api.digiscope.me/api/peers/all | jq '.count'

# Stats
curl -s https://api.digiscope.me/api/peers/stats | jq '.'
```

## Current network state

`/api/peers/stats` is the source of truth for live counts (`peers_total`, `peers_filter_validated`, `peers_filter_above_threshold`, `all_peers_known`, `attempts_7d_total`). There are no bloom counts anymore — bloom-validated peers are no longer tracked or reported. As more operators turn on `peerblockfilters=1` + `blockfilterindex=basic` in their `digibyte.conf`, the filter-validated set grows; wallets that follow the recommended pattern pick up new filter peers automatically — no app update required.

## Operational

- **Crawl interval:** 30 minutes (configurable).
- **Stale window:** peers not seen in the last 6 hours are not served.
- **Inclusion threshold:** peers with smoothed uptime < 0.50 are filtered out.
- **Response cap:** 25 peers.
- **Backwards-compat:** `/api/peers/bloom` and `/api/peers/all` remain permanent aliases and never error, but are deprecated — they now serve the same filter peer list as `/api/peers`. Prefer `/api/peers` (or `/api/peers/filter`) in new code.

For questions or to coordinate a wallet rollout, ping the seeder operator.
