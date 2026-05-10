# Uptime-Based Peer Ranking — Design Spec

**Status:** Approved, pending implementation plan.
**Date:** 2026-05-10
**Author:** brainstorming session w/ JohnnyLawDGB

---

## Problem

`GET /peers` currently sorts by `last_seen DESC`. A peer that responded to one crawl 30 seconds ago outranks a peer we have known for 60 days at 99% uptime. The Android wallet has no signal to prefer historically reliable peers, and there is no defense against an attacker spinning up short-lived "100% uptime" nodes that immediately appear at the top of the list.

## Goal

Rank served peers by a composite **smoothed uptime × longevity** score, computed from a rolling 7-day attempt history. Peers below an uptime threshold are filtered out entirely. Brand-new peers face a Bayesian-prior burn-in so they cannot Sybil-attack their way to the top of the list.

## Non-goals

- No change to who counts as a bloom peer — `bloom_verified` (filterload-confirmed) is still the only gate for entry into `bloom_peers`.
- No change to the crawl queue (`all_peers`).
- No new authentication, rate limiting, or response signing.
- No client-side change to the wallet — it benefits from the new sort order without reading the new fields.

## Architecture

```
[crawl cycle]
   for each peer in crawl queue:
       attempt handshake + filterload
       ┌─ if (ip, port) ∈ current bloom_peers OR newly bloom_verified:
       │      INSERT INTO bloom_peer_attempts(ip, port, ts, success)
       └─ existing upsert into bloom_peers if verified

[end of each crawl cycle]
   DELETE FROM bloom_peer_attempts WHERE ts < now - 7d
   on bloom_peer prune (24h unseen):
       DELETE FROM bloom_peer_attempts WHERE ip=? AND port=?

[GET /peers]
   compute composite_score per peer over last 7d
   filter where smoothed_uptime ≥ threshold
   ORDER BY composite_score DESC, last_seen DESC
   LIMIT api_max_results
```

One new table. One new query path in storage. A small change to the crawler's `check_peer` worker. No changes to `bloom_peers` schema, the queue, or the wire protocol module.

## Schema

```sql
CREATE TABLE bloom_peer_attempts (
    ip       TEXT NOT NULL,
    port     INTEGER NOT NULL,
    ts       INTEGER NOT NULL,    -- unix seconds
    success  INTEGER NOT NULL,    -- 0 or 1
    PRIMARY KEY (ip, port, ts)
);
CREATE INDEX idx_attempts_ts      ON bloom_peer_attempts(ts);
CREATE INDEX idx_attempts_peer_ts ON bloom_peer_attempts(ip, port, ts);
```

The `(ip, port, ts)` PK doubles as the lookup index for per-peer score computation; `idx_attempts_ts` supports the per-cycle `DELETE WHERE ts < cutoff` prune.

**Storage estimate:** ~200 verified bloom peers × 48 crawls/day × 7 days ≈ 67k rows. ~3–5 MB total. Negligible.

## Crawler changes (`seeder/crawler.py`)

`crawl_cycle` gains two responsibilities:

1. **Snapshot known bloom peers.** Before launching workers, fetch the current set of `(ip, port)` tuples in `bloom_peers` into a Python `set`.
2. **Log attempt outcome.** Inside `check_peer`, after the handshake either completes or fails:
   - If `(ip, port)` is in the snapshot, OR the result was `bloom_verified=True`:
     - Insert a row into `bloom_peer_attempts` with `success = 1 if bloom_verified else 0`.
   - Unknown IPs that did not verify are not logged (avoids polluting the table with random failed handshakes from the discovery queue).
3. **Bound the table.** After the worker loop, run `DELETE FROM bloom_peer_attempts WHERE ts < now - 7d`.
4. **Cascade-delete on prune.** When `storage.prune()` removes an unseen bloom peer, also delete that peer's attempts.

This guarantees: every known bloom peer accumulates exactly one row per crawl cycle (success or failure), and offline peers steadily acquire failure rows that drag their score down even before the 24h `last_seen` prune kicks in.

## Scoring algorithm

### Smoothed uptime (Bayesian prior)

Raw `successes / attempts` is unstable for low-volume peers. We add a phantom prior of 10 attempts at 50% success rate:

```
smoothed_uptime = (successes_7d + prior_successes) / (attempts_7d + prior_attempts)
```

With `prior_attempts = 10`, `prior_successes = 5`:

| Real history (7d) | Smoothed |
|---|---|
| 1/1 (just discovered, 1 success) | 0.545 |
| 2/2 | 0.583 |
| 50/50 (≈25h, all clean) | 0.917 |
| 200/200 (≈4d, all clean) | 0.976 |
| 200/180 (10% recent flakiness) | 0.881 |
| 200/0 (sustained outage) | 0.024 |

A new peer takes ~37 hours of perfect uptime before the prior fades enough for the score to clear 0.90. This is the burn-in that defends against fresh-IP Sybil attacks.

### Longevity bonus

```
tenure_days     = (now - first_seen) / 86400
longevity_bonus = min(tenure_days / 60, 1.0)         # 0..1, saturates at 60 days
composite_score = smoothed_uptime * (1 + 0.30 * longevity_bonus)
```

A 60+ day-known peer gets up to a 30% multiplier over an equally-reliable newer peer. Reliability still dominates: a 60% uptime peer caps at `0.60 * 1.30 = 0.78`, well below a 90% peer with no longevity (`0.90`).

**Worked examples (composite score):**

| Peer | Smoothed uptime | Tenure | Composite |
|---|---|---|---|
| Fresh attacker (clean handful of attempts) | 0.58 | 0d | 0.58 |
| 14-day steady peer at 95% | 0.95 | 14d | 1.016 |
| 60-day steady peer at 95% | 0.95 | 60d | 1.235 |
| 90-day historically reliable, currently flaky | 0.85 | 90d | 1.105 |
| 60-day mostly down peer | 0.40 | 60d | filtered (< threshold) |

### Inclusion threshold

```
include peer iff smoothed_uptime ≥ ranking_inclusion_threshold   # default 0.50
```

The Bayesian prior is centered at 0.50, so the threshold means "we have at least coin-flip evidence the peer is real and responding." A brand-new peer with one successful attempt clears it; a peer that's been failing more often than succeeding does not.

### Sort order

```
ORDER BY composite_score DESC, last_seen DESC
```

`last_seen` is a tiebreaker for two peers with identical composite scores — preserves the existing recency intuition for unranked ties.

## API changes

### `GET /peers`

Response gains five fields per peer and re-orders by composite score:

```json
{
    "peers": [
        {
            "ip": "134.199.198.90",
            "port": 12024,
            "last_seen": 1743900000,
            "first_seen": 1741000000,
            "protocol_version": 70019,
            "user_agent": "/DigiByte:8.26.0/",
            "uptime_score": 0.94,
            "composite_score": 1.18,
            "attempts_7d": 312,
            "successes_7d": 298,
            "tenure_days": 33.6
        }
    ],
    "count": 17,
    "crawl_age_seconds": 120
}
```

- `uptime_score` — smoothed uptime, 0..1
- `composite_score` — final ranking score, ≥ 0
- `attempts_7d`, `successes_7d` — raw counts (pre-prior)
- `tenure_days` — float
- `first_seen` — added so the wallet can verify our `tenure_days` if it wants to

Wallet integration is unchanged — clients can ignore all new fields and still benefit from the sort order.

### `GET /stats`

Gains:

```json
{
    "bloom_peers_total": 42,
    "bloom_peers_recent": 38,
    "bloom_peers_above_threshold": 17,
    "all_peers_known": 1250,
    "attempts_7d_total": 8342,
    "last_crawl": 1743900000,
    "uptime_seconds": 86400
}
```

`bloom_peers_above_threshold` makes a misconfigured threshold or broken scoring instantly visible in production.

## Config additions

Added to `config.yaml` and the `Config` dataclass:

```yaml
# Ranking
ranking_window_days: 7
ranking_prior_attempts: 10
ranking_prior_successes: 5
ranking_longevity_cap_days: 60
ranking_longevity_weight: 0.30
ranking_inclusion_threshold: 0.50
```

All operator-tunable without a code change.

## Storage module additions (`seeder/storage.py`)

New methods, all parameterized so the scoring function is a pure expression of the config values:

```python
async def record_attempt(self, ip: str, port: int, success: bool, ts: int): ...

async def get_ranked_peers(
    self, *,
    window_days: int,
    prior_attempts: int, prior_successes: int,
    longevity_cap_days: int, longevity_weight: float,
    inclusion_threshold: float,
    max_age_hours: int,
    limit: int,
) -> list[dict]:
    """Returns peers above threshold, sorted by composite score DESC."""
    ...

async def prune_attempts(self, window_days: int) -> int: ...

async def get_attempts_total(self, window_days: int) -> int: ...

async def get_above_threshold_count(
    self, *, threshold: float,
    prior_attempts: int, prior_successes: int,
    window_days: int, max_age_hours: int,
) -> int: ...
```

Existing `prune()` is updated to also delete from `bloom_peer_attempts` for any peer it removes from `bloom_peers` (single transaction).

The composite score is computed in SQL (cheap, single round-trip) using a subquery that aggregates `bloom_peer_attempts` over the window:

```sql
WITH stats AS (
  SELECT bp.ip, bp.port,
         bp.last_seen, bp.first_seen, bp.protocol_version, bp.user_agent,
         COALESCE(SUM(a.success), 0)            AS successes_7d,
         COALESCE(COUNT(a.success), 0)          AS attempts_7d
  FROM bloom_peers bp
  LEFT JOIN bloom_peer_attempts a
         ON a.ip = bp.ip AND a.port = bp.port AND a.ts >= ?
  WHERE bp.last_seen >= ?
  GROUP BY bp.ip, bp.port
),
scored AS (
  SELECT *,
         (successes_7d + ?) * 1.0 / (attempts_7d + ?)                       AS smoothed_uptime,
         MIN((? - first_seen) / 86400.0 / ?, 1.0)                           AS longevity_bonus
  FROM stats
)
SELECT *,
       smoothed_uptime * (1 + ? * longevity_bonus)                          AS composite_score
FROM scored
WHERE smoothed_uptime >= ?
ORDER BY composite_score DESC, last_seen DESC
LIMIT ?;
```

(Parameter binding done in the implementation; the formula matches the spec above.)

## Tests

**Storage (`tests/test_storage.py`):**
- `record_attempt` — round-trip insert + count.
- `get_ranked_peers`:
  - Empty history → no peers (filtered by threshold via prior).
  - Single 100% peer with 1 attempt → included, score reflects prior.
  - Two peers with identical uptime, different tenure → longer tenure ranks first.
  - Two peers with different uptime → higher uptime wins regardless of tenure (within reason).
  - Peer below threshold → excluded.
  - `LIMIT` honored.
- `prune_attempts` — rows older than window deleted, newer kept.
- `prune` (existing) — also deletes attempt rows for pruned peers.
- `get_above_threshold_count` — matches the count of `get_ranked_peers`.

**Crawler (new `tests/test_crawler.py`):**
- Mock `handshake_peer` to return controlled outcomes; assert `bloom_peer_attempts` rows match expectations across a synthetic crawl cycle.
- Assert that an IP not in `bloom_peers` AND not currently verifying is not logged.
- Assert that an IP currently in `bloom_peers` but failing to handshake gets a `success=0` row.

## Migration

The seeder runs as a single process with a SQLite file. On next start, `Storage.init()` runs `CREATE TABLE IF NOT EXISTS bloom_peer_attempts` — no manual migration step.

Existing `bloom_peers` rows have no historical attempts logged, so they will start with `attempts_7d = 0` and a smoothed_uptime of `prior_successes / prior_attempts = 0.50`, exactly at the threshold. They will climb or fall as the crawler accumulates real evidence over the next 24–48 hours. This is the correct behavior — we should not pretend to have history we don't have.

## Out of scope (future work)

- Per-peer history endpoint (`GET /peers/<ip>:<port>/history`).
- Hourly heatmap visualization.
- Cross-seeder reputation sharing.
- Adaptive thresholds (raising the bar when there are many high-quality peers).
- Geographic distribution scoring.

These are all easy to bolt on later because the per-attempt log retains the underlying data; deferring them costs us nothing today.
