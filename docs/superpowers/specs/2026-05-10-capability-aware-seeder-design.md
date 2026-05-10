# Capability-Aware Peer Seeder — Design Spec

**Status:** Approved, pending implementation plan.
**Date:** 2026-05-10
**Author:** brainstorming session w/ JohnnyLawDGB
**Supersedes (in part):** `2026-05-10-uptime-ranking-design.md` — extends the schema and API introduced there from bloom-only to capability-indexed.

---

## Problem

The seeder catalogs only `NODE_BLOOM (0x04)` peers today. BIP 158 wallets need `NODE_COMPACT_FILTERS (0x40)` peers, which are different nodes with a different validation protocol. The current `bloom_peers` table, `filterload` verifier, and `/peers` endpoint cannot represent or serve them.

## Goal

Generalize the seeder into a capability-indexed catalog that detects, validates, ranks, and serves peers for both `NODE_BLOOM` and `NODE_COMPACT_FILTERS` independently. The default `/peers` endpoint serves block-filter peers and falls through to bloom only if the filter list is empty. A capability query string lets clients request one or both. Each capability is ranked using the existing Bayesian-smoothed × longevity scoring.

## Non-goals

- No support for capabilities beyond `BLOOM` and `COMPACT_FILTERS` in this pass (the schema is extensible, but only these two are wired).
- No DGB Core node config changes (`peerblockfilters=1` etc.) — operator concern, separate work.
- No wallet code changes — those happen after this ships, in the Android repo.
- No fix for `digiscope-backend` 404/429 bugs — different service.

## Architecture

```
crawl_cycle:
   ┌─ get_validated_peer_set()         ← priority pool: every known
   │                                      capability peer, always
   ├─ get_uncrawled_peers(remaining)   ← top up to crawl_max_peers
   └─ union, dedupe

   per peer (semaphore):
     handshake → version → verack
       ↓
     if services & NODE_BLOOM:
         send filterload → observe accept/disconnect → bloom_verified
     if services & NODE_COMPACT_FILTERS:
         send getcfheaders → observe accept/disconnect → filter_verified
       ↓
     record peer_attempts(capability='bloom', success=bloom_verified) IF the peer is in priority OR newly verifies bloom
     record peer_attempts(capability='filter', success=filter_verified) IF the peer is in priority OR newly verifies filter
       ↓
     upsert peers row; set bloom_validated_at and/or filter_validated_at when first/refreshed

   after workers:
     prune_attempts(window_days=7)

GET /peers (no params):
   peers = ranked_peers(capability='filter')
   if empty: peers = ranked_peers(capability='bloom')
   response.capability = 'filter' or 'bloom' depending on which list filled

GET /peers?capability=filter
GET /peers?capability=bloom
GET /peers?capability=filter|bloom
```

The wire protocol module gains one new builder (`build_getcfheaders`); the storage module's schema is renamed and extended; the crawler runs two validations per peer; the API gains query parsing and the fallback rule. No change to the `aiohttp` server framework, the config-loading scheme, or the asyncio shape of the program.

## Schema

```sql
-- Renamed and extended from bloom_peers.
CREATE TABLE peers (
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    services INTEGER NOT NULL,
    protocol_version INTEGER,
    user_agent TEXT,
    last_seen INTEGER NOT NULL,
    first_seen INTEGER NOT NULL,
    bloom_validated_at  INTEGER,   -- NULL = never validated for bloom
    filter_validated_at INTEGER,   -- NULL = never validated for filters
    PRIMARY KEY (ip, port)
);
CREATE INDEX idx_peers_last_seen ON peers(last_seen);
CREATE INDEX idx_peers_bloom  ON peers(bloom_validated_at)  WHERE bloom_validated_at  IS NOT NULL;
CREATE INDEX idx_peers_filter ON peers(filter_validated_at) WHERE filter_validated_at IS NOT NULL;

-- Renamed and extended from bloom_peer_attempts.
CREATE TABLE peer_attempts (
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    capability TEXT NOT NULL,        -- 'bloom' or 'filter'
    success INTEGER NOT NULL,        -- 0 or 1
    PRIMARY KEY (ip, port, ts, capability)
);
CREATE INDEX idx_attempts_cap_ts      ON peer_attempts(capability, ts);
CREATE INDEX idx_attempts_peer_cap_ts ON peer_attempts(ip, port, capability, ts);
```

`all_peers` (the crawl-queue table) is unchanged.

**Storage estimate.** ~30 capability peers × 48 crawls/day × 2 capabilities × 7 days ≈ 20k rows. Roughly 1.5 MB. Negligible. Grows linearly with crawl_concurrency × capabilities × validation cycles.

### Migration

Runs on startup inside `Storage.init()` after the new `CREATE TABLE IF NOT EXISTS` statements. The migration is gated by a Python-side check against `sqlite_master`: if the old `bloom_peers` table no longer exists (which is the steady state after a successful first migration), the entire block is skipped.

```python
# Pseudocode in Storage.init() after the new CREATE TABLE IF NOT EXISTS pass.
cursor = await self._db.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='bloom_peers'"
)
needs_migration = (await cursor.fetchone()) is not None

if needs_migration:
    await self._db.executescript("""
        BEGIN;
        INSERT OR IGNORE INTO peers
            (ip, port, services, protocol_version, user_agent, last_seen, first_seen,
             bloom_validated_at, filter_validated_at)
        SELECT
             ip, port, services, protocol_version, user_agent, last_seen, first_seen,
             last_seen,        -- existing rows were bloom-validated at last_seen
             NULL              -- never filter-validated
        FROM bloom_peers;

        INSERT OR IGNORE INTO peer_attempts (ip, port, ts, capability, success)
        SELECT ip, port, ts, 'bloom', success
        FROM bloom_peer_attempts;

        DROP TABLE bloom_peer_attempts;
        DROP TABLE bloom_peers;
        COMMIT;
    """)
```

Idempotent: the SELECT against `sqlite_master` returns no row after the first successful migration, so the block is skipped on subsequent boots. The old tables are dropped only after the inserts succeed, all inside a single `BEGIN/COMMIT`.

**Rollback** is `pm2 stop bloom-seeder && cp bloom_seeder.db.bak-PRE-CAPABILITY bloom_seeder.db && git checkout <prior-commit> && pm2 start bloom-seeder`. This is a clean rollback because the migration writes only to the new tables — the backup contains the old tables intact.

## Wire protocol additions (`seeder/protocol.py`)

New service flag:

```python
NODE_COMPACT_FILTERS = 0x40
```

New message builder:

```python
def build_getcfheaders(
    magic: bytes,
    filter_type: int = 0,         # 0 = basic (BIP 158)
    start_height: int = 1,
    stop_hash: bytes = b"\x00" * 32,
) -> bytes:
    """Build a getcfheaders message (BIP 157) for validating compact-filter support.

    The stop_hash defaults to all zeros — not a valid block hash, but non-supporting
    peers disconnect on getcfheaders regardless of payload validity. Supporting peers
    respond (possibly with cfheaders/notfound) or briefly hold the connection.
    """
    payload  = struct.pack("<B", filter_type)
    payload += struct.pack("<I", start_height)
    payload += stop_hash
    return make_message(magic, "getcfheaders", payload)
```

## Crawler validation

In `handshake_peer`, after `verack` exchange and before `getaddr`:

```python
bloom_verified = False
filter_verified = False

# BLOOM (existing logic, unchanged)
if info["services"] & NODE_BLOOM:
    writer.write(build_filterload(magic))
    await writer.drain()
    try:
        header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
        cmd, plen, _ = parse_message_header(header)
        if plen > 0 and plen < 100_000:
            await asyncio.wait_for(reader.readexactly(plen), timeout=2)
        bloom_verified = True
    except asyncio.TimeoutError:
        bloom_verified = True
    except (asyncio.IncompleteReadError, ConnectionError):
        bloom_verified = False

# FILTER (new, mirrors bloom)
if info["services"] & NODE_COMPACT_FILTERS:
    writer.write(build_getcfheaders(magic))
    await writer.drain()
    try:
        header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
        cmd, plen, _ = parse_message_header(header)
        if plen > 0 and plen < 100_000:
            await asyncio.wait_for(reader.readexactly(plen), timeout=2)
        filter_verified = True
    except asyncio.TimeoutError:
        filter_verified = True
    except (asyncio.IncompleteReadError, ConnectionError):
        filter_verified = False

info["bloom_verified"]  = bloom_verified
info["filter_verified"] = filter_verified
```

Total handshake adds ~1–2 seconds for peers that advertise either bit; no added cost for peers that don't. Both validations share the same TCP connection.

## Per-capability attempt logging

Two per-capability snapshots are taken at the top of the cycle so attempt logging is gated per-capability — a filter-only peer should not accumulate bloom-failure rows:

```python
known_bloom  = await storage.get_validated_peer_set(capability='bloom')
known_filter = await storage.get_validated_peer_set(capability='filter')
priority     = known_bloom | known_filter   # for crawl scheduling
```

Then in the worker:

```python
ts = int(time.time())

# Log a bloom attempt if peer was previously bloom-validated OR just verified bloom now.
if (ip, port) in known_bloom or result.get("bloom_verified"):
    await storage.record_attempt(ip, port, capability="bloom",
                                  success=bool(result and result.get("bloom_verified")),
                                  ts=ts)

# Same gate, separately, for filter.
if (ip, port) in known_filter or result.get("filter_verified"):
    await storage.record_attempt(ip, port, capability="filter",
                                  success=bool(result and result.get("filter_verified")),
                                  ts=ts)
```

Per-capability gating means: a peer that's filter-only doesn't accumulate bloom-failure rows just because it sits in the priority pool. Each capability tracks only the peers that have ever been (or are now) valid for it.

Two attempt rows can be logged per peer per cycle (one per capability), each tracking that capability's verification result.

## Priority pool

Two per-capability snapshots are taken at the top of `crawl_cycle`. They drive both the scheduling priority and the per-capability attempt-logging gate (see "Per-capability attempt logging" above):

```python
known_bloom  = await storage.get_validated_peer_set(capability='bloom')
known_filter = await storage.get_validated_peer_set(capability='filter')
priority     = known_bloom | known_filter   # union — every peer worth re-checking

budget = max(0, config.crawl_max_peers - len(priority))
normal = await storage.get_uncrawled_peers(limit=budget)
peers  = list(priority) + [p for p in normal if p not in priority]
```

`storage.get_validated_peer_set(capability)` returns:

```sql
-- capability='bloom'
SELECT ip, port FROM peers WHERE bloom_validated_at IS NOT NULL

-- capability='filter'
SELECT ip, port FROM peers WHERE filter_validated_at IS NOT NULL
```

Priority peers are crawled every cycle regardless of `last_crawled`. They consume budget from `crawl_max_peers`, so total per-cycle load is bounded. At 30 priority peers + 500-budget, the normal queue still gets 470 picks per cycle.

## Static peers

`config.yaml`:

```yaml
# Manually-known peers loaded into the crawl queue on every startup.
# Use this for known-good nodes (especially on non-default ports) that
# might not be reachable via DNS-seed gossip alone.
static_peers:
  - { ip: "129.212.182.152", port: 12024, source: "filter-vps" }
  - { ip: "174.131.163.123", port: 12024, source: "filter-vps" }
  - { ip: "149.76.66.207",   port: 22024, source: "johnnylaw" }
```

`Config` dataclass gains:

```python
static_peers: list[dict] = field(default_factory=list)
```

In `seeder.py main()`, after DNS resolution:

```python
if config.static_peers:
    static = [(p["ip"], p["port"]) for p in config.static_peers]
    await storage.add_crawl_peers(static)
    log.info("Loaded %d static peers from config", len(static))
```

`source` is metadata for the operator only; the code doesn't read it. The `INSERT OR IGNORE` semantics of `add_crawl_peers` mean re-adding existing peers is a no-op.

## API

### Endpoints

| Path | Behavior |
|---|---|
| `GET /peers` | Filter peers above threshold. If empty, fall through to bloom peers above threshold. |
| `GET /peers?capability=filter` | Filter peers above threshold. |
| `GET /peers?capability=bloom` | Bloom peers above threshold. |
| `GET /peers?capability=filter\|bloom` | Filter peers above threshold (ranked among themselves), followed by bloom peers above threshold (ranked among themselves). |
| `GET /stats` | (unchanged structure, but counts now per-capability — see below) |

`capability` is parsed case-insensitively. Unknown values return HTTP 400 with `{"error": "invalid capability"}`.

### Response shape (`/peers`)

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

- Response `"capability"` reports which list the wallet got: `"filter"`, `"bloom"`, or `"filter+bloom"`.
- Per-peer `"peer_capability"` reports which capability *this row's* uptime/score reflects.
- For mixed responses (`filter|bloom`), filter rows come first, then bloom rows. The response `"count"` is the total length; per-section counts can be derived by counting `peer_capability`.

`capabilities` is a human-readable list derived from the `services` integer, computed in the API layer (no schema change). Bit-name mapping:

```python
SERVICE_FLAG_NAMES = {
    0x01:  "NETWORK",
    0x04:  "BLOOM",
    0x08:  "WITNESS",
    0x40:  "COMPACT_FILTERS",
    0x400: "NETWORK_LIMITED",
}
```

### Response shape (`/stats`)

Gains per-capability counts:

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

The previous `bloom_peers_total`, `bloom_peers_recent`, and `bloom_peers_above_threshold` fields are renamed: `peers_total` (formerly `bloom_peers_total`), `peers_bloom_validated` (replaces `bloom_peers_recent` — the prior meaning was "bloom peers seen recently," now it's "peers with bloom_validated_at set"), and split-by-capability above-threshold counts.

### Backwards compatibility

Pre-existing nginx route: `/api/peers/bloom` → `:8025/peers`. After this change, that proxy needs to send the bloom query: `/api/peers/bloom` → `:8025/peers?capability=bloom`. This is a one-line nginx config change deployed alongside the seeder restart.

`/api/peers/stats` continues to proxy to `:8025/stats` — the field-name changes there are not visible to v3.5.38 wallets (the wallet doesn't parse stats).

If the nginx update is forgotten, v3.5.38 wallets would start getting filter peers (from the default endpoint) instead of bloom peers. Most wallets would still work but log "unsupported" errors. The deploy plan explicitly includes the nginx step to prevent this.

## Ranking — per capability

`Storage.get_ranked_peers` gains a `capability` parameter (`'bloom'` or `'filter'`). The CTE join now filters `peer_attempts.capability = ?` so the score reflects only that capability's history. The Bayesian prior and longevity multiplier are unchanged.

```sql
WITH stats AS (
    SELECT p.ip, p.port, p.services, p.last_seen, p.first_seen,
           p.protocol_version, p.user_agent,
           p.bloom_validated_at, p.filter_validated_at,
           COALESCE(SUM(a.success), 0) AS successes_7d,
           COALESCE(COUNT(a.ts), 0)    AS attempts_7d
    FROM peers p
    LEFT JOIN peer_attempts a
           ON a.ip = p.ip
          AND a.port = p.port
          AND a.capability = ?           -- new bind
          AND a.ts >= ?
    WHERE p.last_seen >= ?
      AND (
          (? = 'bloom'  AND p.bloom_validated_at  IS NOT NULL)
       OR (? = 'filter' AND p.filter_validated_at IS NOT NULL)
      )
    GROUP BY p.ip, p.port
),
...
```

Effectively: only peers ever validated for the queried capability are even eligible, then they're ranked by their per-capability uptime score.

## Tests

- **Protocol** — `build_getcfheaders` produces the expected payload bytes (filter_type byte + LE start_height + 32-byte stop_hash); verack/header parsing still works after the new builder is added.
- **Storage migration** — populate the old `bloom_peers` and `bloom_peer_attempts` schema in an in-memory DB; run `Storage.init()`; assert the new `peers` and `peer_attempts` rows match the originals, with `bloom_validated_at = last_seen` and `filter_validated_at IS NULL`. Run `Storage.init()` again, assert idempotency.
- **Storage** — `get_validated_peer_set` returns only validated peers; `get_ranked_peers(capability='filter')` excludes bloom-only peers and vice versa; `record_attempt` writes the correct `capability` value.
- **Crawler** — mocked `handshake_peer` returns `{bloom_verified: True, filter_verified: True}` → two `peer_attempts` rows logged; `{bloom_verified: True, filter_verified: False}` → bloom-row=1, filter-row=0; same peer in priority pool → both rows logged even if neither just-verified; unknown unverified peer → zero rows.
- **API** — `/peers` returns filter list when filter peers exist; falls through to bloom when filter list empty; sets response `"capability"` correctly in both cases; `?capability=filter` returns filter-only with `peer_capability=filter`; `?capability=bloom` returns bloom-only; `?capability=filter|bloom` returns filter rows then bloom rows; unknown capability returns 400. `/stats` exposes per-capability counts.
- **Integration** — start with the renamed schema, seed a bloom peer and a filter peer, hit each endpoint variant, assert behavior end-to-end.

## Deployment

1. Back up production DB: `cp bloom_seeder.db bloom_seeder.db.bak-PRE-CAPABILITY-$(date +%Y%m%d)`
2. Pull the merged feature commits from origin
3. Run `pytest tests/` on the server, confirm green
4. Update nginx: `/api/peers/bloom` proxy now sends `?capability=bloom` (one-line config change, `nginx -t && systemctl reload nginx`)
5. `pm2 restart bloom-seeder`. Migration runs on startup, then initial crawl populates the priority pool. Initial crawl can take ~5 minutes; API comes up after.
6. Verify `curl localhost:8025/peers`, `curl localhost:8025/peers?capability=bloom`, `curl localhost:8025/stats`. Confirm shape and counts.
7. Verify public endpoints via `https://api.digiscope.me/api/peers/bloom` (v3.5.38 wallet path) — should return bloom peers as before.

Rollback (if anything looks wrong):
```bash
pm2 stop bloom-seeder
cp bloom_seeder.db.bak-PRE-CAPABILITY-* bloom_seeder.db
git checkout <prior-master-sha>
pm2 start bloom-seeder
# revert the nginx change too: /api/peers/bloom proxies back to :8025/peers
nginx -t && systemctl reload nginx
```

## Out of scope (future work)

- Real recent-block-hash for `getcfheaders` (current heuristic uses zero stop_hash; harmless but imprecise — refining will produce cleaner cfheaders responses to inspect)
- Additional capability bits beyond `BLOOM` and `COMPACT_FILTERS` (the schema is ready for them; needs a new validator per capability)
- Per-peer history endpoint (`GET /peers/<ip>:<port>/history`) — easy bolt-on with the attempts table
- Cross-seeder reputation sharing
- Adaptive threshold tuning
- BIP 158 client validation (running real `getcfilters` requests on a peer and verifying the GCS-encoded response — overkill for this seeder, useful for a future health-check tool)
