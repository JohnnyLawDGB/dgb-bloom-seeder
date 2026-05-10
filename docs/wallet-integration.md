# Mobile Wallet Integration — digiscope.me Peer API

A short guide for the Android wallet team on how to consume the capability-aware seeder.

## Production endpoints

All endpoints are HTTPS, GET, no auth required, public data.

| URL | Returns | When to use |
|---|---|---|
| `https://api.digiscope.me/api/peers` | Block-filter peers above threshold; **falls through to bloom peers if filter list is empty**. Single ranked list. | **Recommended default** for new wallet builds. Lets the seeder pick the best available capability. |
| `https://api.digiscope.me/api/peers/filter` | Block-filter (BIP 157/158) peers above threshold. | Wallet explicitly wants BIP 158 peers; happy to fail if there are none. |
| `https://api.digiscope.me/api/peers/bloom` | Bloom-filter (BIP 37) peers above threshold. | Wallet only supports BIP 37 (v3.5.38). Also the v3.5.38 wallet's existing URL — backwards-compatible. |
| `https://api.digiscope.me/api/peers/all` | Block-filter peers ranked first, then bloom peers ranked separately. Single combined list. | Wallet supports both protocols and wants the maximum candidate pool. |
| `https://api.digiscope.me/api/peers/stats` | Crawl statistics and per-capability validated counts. | Operator dashboards / health checks. Not for wallet runtime. |

Unknown `?capability=...` values on the raw seeder (`:8025/peers`) return HTTP 400. The named-path nginx routes above are the wallet's public surface and never error on routing.

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
- `count` — `len(peers)`; may be 0 if no peers above threshold for the requested capability
- `capability` — `"filter"`, `"bloom"`, or `"filter+bloom"`. Reports what list the wallet got. For `/api/peers` this is how the wallet knows whether the fallthrough fired (returned `"bloom"`) or filter peers were available (returned `"filter"`).
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
| `peer_capability` | `"filter"` or `"bloom"` | Which validation the seeder did on this peer this row reflects. For mixed responses (`/api/peers/all`) this is how the wallet routes a peer to the right protocol stack. |
| `capabilities` | array | Human-readable service flag names the peer advertised. Always reflects exactly what was on the wire. |
| `services` / `services_hex` | int / hex string | Raw service-flag bitmask. |
| `bloom_validated_at` / `filter_validated_at` | int (unix) or null | Last time the seeder confirmed each capability on this peer. `null` means never validated for that capability — connect with the other protocol. |
| `last_seen` | int (unix) | Last successful version handshake. |
| `uptime_score` | float 0..1 | Bayesian-smoothed 7-day reliability (centered at 0.5 with a prior of 5 successes / 10 attempts). |
| `composite_score` | float | Final ranking score. Same as `uptime_score × (1 + 0.30 × min(tenure_days/60, 1.0))`. **Higher is better; the array is already sorted by this.** |
| `tenure_days` | float | How long this peer has been continuously known to the seeder. |
| `user_agent` | string | Peer's advertised user-agent. Useful for debugging but not for routing. |

The wallet can safely ignore any field it doesn't recognize; the seeder may add fields without notice.

## Recommended client behavior

1. **Default endpoint:** new wallet versions hit `https://api.digiscope.me/api/peers` (no capability suffix). This gets filter peers when available and bloom peers when filter peers are scarce. Read the response-level `capability` to know which protocol stack to feed.
2. **Refresh cadence:** once per hour, on app foreground, and on each manual sync. Hourly is generous given the 30-min seeder crawl cadence; caching tighter (e.g., 5 min) is fine but pointless.
3. **Caching:** store the JSON response in SharedPreferences. Use cached peers on every sync start; refresh in the background.
4. **Picking peers:** use the first N entries of `peers` (the seeder caps the response at 25). The list is already ranked; just slice from the top.
5. **Connection routing:** for each peer, check `peer_capability` and route to the matching protocol stack (BIP 37 sender vs BIP 158 sender). If the wallet only supports one protocol, hit `/api/peers/bloom` or `/api/peers/filter` directly instead.
6. **Fallback chain on API failure:**
   - First: use the cached response from SharedPreferences if it's < 24 hours old
   - Then: hardcoded `digiscope.me:12024` (current v3.5.38 behavior)
   - Last resort: DigiByte DNS seeds
7. **Don't retry tightly.** If `api.digiscope.me` returns a 5xx, back off 30+ seconds. The seeder restart can take ~4 minutes during which time the public endpoints return 502.

## Notes for v3.5.38 wallets in the wild

- They keep hitting `https://api.digiscope.me/api/peers/bloom`. This URL still serves the same content (bloom peers only). The nginx alias was updated to add `?capability=bloom` upstream, so behavior is unchanged from the wallet's perspective.
- They don't know about `peer_capability` / `capabilities` / `services_hex` / `filter_validated_at` — they'll see them as extra unknown JSON fields and ignore them. No protocol break.
- Existing peer payloads continue to include `services`, `user_agent`, `last_seen`, `first_seen`, `protocol_version`, `uptime_score`, `composite_score`, `attempts_7d`, `successes_7d`, `tenure_days` — same as before this upgrade.

## Example calls

```bash
# Default (filter, with bloom fallthrough)
curl -s https://api.digiscope.me/api/peers | jq '.capability, .count'

# Filter peers only
curl -s https://api.digiscope.me/api/peers/filter | jq '.peers[] | {ip, port, peer_capability, composite_score}'

# Bloom peers (v3.5.38 URL)
curl -s https://api.digiscope.me/api/peers/bloom | jq '.count'

# Combined — filter first, then bloom
curl -s https://api.digiscope.me/api/peers/all | jq '.peers[] | .peer_capability' | sort | uniq -c

# Stats
curl -s https://api.digiscope.me/api/peers/stats | jq '.'
```

## Current network state (snapshot, 2026-05-10)

At time of writing the seeder sees:

- ~28 bloom-validated peers
- 1 filter-validated peer (`174.131.163.123:12024`, services `0x44d`)
- Three other known filter-capable nodes are configured as static peers and being crawled every cycle; they're not validating right now (offline or unreachable from the seeder's network path).

As more operators turn on `peerblockfilters=1` + `blockfilterindex=basic` in their `digibyte.conf`, the filter-validated set will grow. The default `/api/peers` endpoint's behavior is intentionally optimistic: serve filter peers when they exist, bloom peers when they don't. Wallets that follow the recommended pattern will pick up filter peers automatically as they become available — no app update required.

## Operational

- **Crawl interval:** 30 minutes (configurable).
- **Stale window:** peers not seen in the last 6 hours are not served.
- **Inclusion threshold:** peers with smoothed uptime < 0.50 are filtered out.
- **Response cap:** 25 peers per capability.
- **Backwards-compat:** `/api/peers/bloom` is a permanent alias; safe to keep using it for any wallet version.

For questions or to coordinate a wallet rollout, ping the seeder operator.
