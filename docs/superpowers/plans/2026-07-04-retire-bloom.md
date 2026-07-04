# Retire BIP37 Bloom — Compact-Filter-Only Seeder — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the seeder to a single capability — compact block filters (`filter`) — removing all BIP37 bloom and the redundant `dandelion` code paths.

**Architecture:** Outside-in removal that keeps the test suite green after every task: (1) API stops serving bloom/dandelion and soft-aliases every request to filter; (2) crawler stops discovering/validating bloom; (3) storage drops its bloom methods and the now-single `capability` parameter; (4) protocol drops the dead `build_filterload`; (5) docs updated. Tasks 1–2 keep calling storage with `capability="filter"` (still valid) so each commit is green; Task 3 removes the parameter and updates those call sites together.

**Tech Stack:** Python 3.11+, aiohttp, aiosqlite, pytest / pytest-asyncio. Run tests with `.venv/bin/pytest`.

## Global Constraints

- Single served capability is `filter` (compact block filters). `/peers` always returns `"capability": "filter"`.
- **Soft-alias:** every capability input — `""`, `filter`, legacy `bloom`/`dandelion`/`bloom|filter`/`filter|bloom`, or any unknown string — returns filter peers. Never 400, never 404.
- **DB schema is NOT migrated.** The `bloom_validated_at` column, `idx_peers_bloom` index, the `peer_attempts.capability` column, and the old `bloom_peers` migration block all stay in `storage.py` untouched (dormant). Only stop *writing/reading* bloom.
- `/stats` keys after this change: `peers_total`, `peers_filter_validated`, `peers_filter_above_threshold`, `all_peers_known`, `attempts_7d_total` (plus `last_crawl`, `uptime_seconds` added by the API layer). No `peers_bloom_*`.
- Keep `NODE_BLOOM` in `protocol.py` and `(0x004, "BLOOM")` in `api.py`'s `SERVICE_FLAG_NAMES` (honest display of advertised bits).
- No renames (`bloom_seeder.db`, repo, seeder user-agent stay).
- TDD, frequent commits. Run the full suite (`.venv/bin/pytest tests/ -q`) at the end of each task — it must stay green.

---

### Task 1: API — collapse to filter-only + soft-alias

**Files:**
- Modify: `seeder/api.py` (delete lines 30–43 dandelion helpers; rewrite `handle_peers`, currently 57–141)
- Test: `tests/test_api.py`

**Interfaces:**
- Consumes: `storage.get_ranked_peers(capability="filter", ...)` (unchanged this task — the `capability` kwarg is still valid until Task 3).
- Produces: `GET /peers` always returns `{"peers":[...], "count":N, "capability":"filter", "crawl_age_seconds":N}`; each peer has `peer_capability="filter"`, `services_hex`, `capabilities`.

- [ ] **Step 1: Rewrite the API tests to the filter-only contract**

Replace the whole body of `tests/test_api.py` below the fixtures (everything from `test_peers_default_returns_filter_when_present` onward) with these tests. Delete `test_peers_default_falls_through_to_bloom`, `test_peers_capability_bloom_explicit`, `test_peers_capability_combined`, `test_peers_unknown_capability_returns_400`. Keep the imports and the `db`/`make_config`/`client` fixtures as-is.

```python
async def _seed_filter_peer(db, ip="2.2.2.2", port=12024):
    now = int(time.time())
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES (?, ?, 0x44d, 70019, '/f/', ?, ?, NULL, ?)
    """, (ip, port, now, now, now))
    await db._db.commit()
    await db.record_attempt(ip, port, capability="filter", success=True, ts=now)


@pytest.mark.asyncio
async def test_peers_default_returns_filter(client, db):
    await _seed_filter_peer(db)
    resp = await client.get("/peers")
    assert resp.status == 200
    data = await resp.json()
    assert data["capability"] == "filter"
    assert data["count"] == 1
    assert data["peers"][0]["ip"] == "2.2.2.2"
    assert data["peers"][0]["peer_capability"] == "filter"


@pytest.mark.asyncio
async def test_peers_capability_filter_explicit(client, db):
    await _seed_filter_peer(db)
    resp = await client.get("/peers?capability=filter")
    data = await resp.json()
    assert data["capability"] == "filter"
    assert data["count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cap", ["bloom", "dandelion", "filter|bloom", "bloom|filter", "totally-unknown"])
async def test_peers_legacy_and_unknown_capabilities_soft_alias_to_filter(client, db, cap):
    """Every legacy/unknown capability returns the filter list — never 400/404."""
    await _seed_filter_peer(db)
    resp = await client.get(f"/peers?capability={cap}")
    assert resp.status == 200
    data = await resp.json()
    assert data["capability"] == "filter"
    assert data["count"] == 1
    assert data["peers"][0]["peer_capability"] == "filter"


@pytest.mark.asyncio
async def test_peers_empty_returns_filter_capability(client, db):
    resp = await client.get("/peers")
    data = await resp.json()
    assert data["count"] == 0
    assert data["peers"] == []
    assert data["capability"] == "filter"


@pytest.mark.asyncio
async def test_peers_response_includes_services_hex_and_capabilities(client, db):
    await _seed_filter_peer(db)  # services 0x44d
    resp = await client.get("/peers")
    peer = (await resp.json())["peers"][0]
    assert peer["services_hex"] == "0x44d"
    assert "BLOOM" in peer["capabilities"]
    assert "COMPACT_FILTERS" in peer["capabilities"]
    assert "NETWORK" in peer["capabilities"]
```

- [ ] **Step 2: Run the API tests — expect failures**

Run: `.venv/bin/pytest tests/test_api.py -q`
Expected: FAIL — `test_peers_legacy_and_unknown_capabilities_soft_alias_to_filter[bloom]` etc. fail (current code returns `capability="bloom"` / 400), and the default-fallthrough behavior differs.

- [ ] **Step 3: Delete the dandelion helpers**

In `seeder/api.py` delete lines 30–43 inclusive (the `# Dandelion ...` comment block, `_DANDELION_MIN_VERSION`, `_ua_version`, `_is_dandelion_capable`). Keep `_services_to_capabilities` and `SERVICE_FLAG_NAMES` (incl. the `BLOOM` entry).

- [ ] **Step 4: Rewrite `handle_peers`**

Replace `handle_peers` (the whole function, currently 57–141) with:

```python
    async def handle_peers(request: web.Request) -> web.Response:
        # Single capability: compact block filters. Any capability value
        # (incl. legacy bloom / dandelion / combined, or anything unknown)
        # soft-aliases to filter — never 400/404.
        peers = await storage.get_ranked_peers(
            capability="filter",
            window_days=config.ranking_window_days,
            prior_attempts=config.ranking_prior_attempts,
            prior_successes=config.ranking_prior_successes,
            longevity_cap_days=config.ranking_longevity_cap_days,
            longevity_weight=config.ranking_longevity_weight,
            inclusion_threshold=config.ranking_inclusion_threshold,
            max_age_hours=config.api_max_age_hours,
            limit=config.api_max_results,
        )
        for p in peers:
            p["peer_capability"] = "filter"
            p["services_hex"] = f"0x{p['services']:x}"
            p["capabilities"] = _services_to_capabilities(p["services"])

        crawl_age = int(time.time() - _last_crawl_time) if _last_crawl_time else -1
        return web.json_response({
            "peers": peers,
            "count": len(peers),
            "capability": "filter",
            "crawl_age_seconds": crawl_age,
        })
```

The `import re` at the top of the file is now unused — remove it.

- [ ] **Step 5: Run the API tests — expect pass**

Run: `.venv/bin/pytest tests/test_api.py -q`
Expected: PASS (all API tests).

- [ ] **Step 6: Run the full suite (must stay green)**

Run: `.venv/bin/pytest tests/ -q`
Expected: PASS. (crawler/storage tests unchanged and still valid — storage still has bloom methods.)

- [ ] **Step 7: Commit**

```bash
git add seeder/api.py tests/test_api.py
git commit -m "feat(api): serve filter only; soft-alias legacy capabilities to filter

Drop bloom/dandelion/combined modes and the dandelion helpers. Any
capability input returns the filter list; no 400/404."
```

---

### Task 2: Crawler — filter-only discovery/validation (+ seeder.py log)

**Files:**
- Modify: `seeder/crawler.py` (imports 10–15; `handshake_peer` bloom block 85–103 + 148; `crawl_cycle` 178–283)
- Modify: `seeder.py` (initial-crawl log, lines 57–61)
- Test: `tests/test_crawler.py`

**Interfaces:**
- Consumes: `storage.get_validated_peer_set(capability="filter")`, `storage.record_attempt(..., capability="filter", ...)`, `storage.clear_validation(..., capability="filter")`, `storage.upsert_filter_peer(...)` (all still valid until Task 3).
- Produces: `handshake_peer` result dict no longer contains `bloom_verified` (only `filter_verified`). `crawl_cycle` stats dict no longer contains `bloom_found`.

- [ ] **Step 1: Rewrite the crawler tests to filter-only**

Rewrite `tests/test_crawler.py`. Change the import line 11 to `from seeder.protocol import NODE_NETWORK, NODE_COMPACT_FILTERS`. Delete the `verified_result` helper and these bloom tests: `test_crawl_logs_success_for_newly_verified_peer`, `test_crawl_logs_failure_for_known_peer_that_drops`, `test_crawl_does_not_log_unknown_unverified_peer`, `test_crawl_logs_failure_when_known_peer_advertises_bloom_but_unverified`, `test_crawl_logs_both_capabilities_for_dual_validated_peer`, `test_crawl_logs_bloom_failure_for_known_bloom_peer_with_no_filter`, `test_crawl_does_not_log_bloom_attempt_for_filter_only_peer`, `test_crawl_clears_bloom_validation_on_services_downgrade`.

Keep `filter_only_result` (drop its `"bloom_verified"` key), `test_crawl_logs_filter_attempt_when_newly_verified`, and `test_crawl_prioritizes_static_peers_even_when_recently_crawled` (unchanged). Replace `test_crawl_clears_filter_validation_on_services_downgrade` with the filter-only version below (its old setup used `upsert_bloom_peer`, which is being removed), and add a filter-failure test to replace the deleted coverage:

```python
def filter_only_result(ip: str, port: int) -> dict:
    return {
        "ip": ip, "port": port, "protocol_version": 70019,
        "services": NODE_NETWORK | NODE_COMPACT_FILTERS,
        "user_agent": "/DigiByte:9.26.4/", "timestamp": 0, "start_height": 0,
        "relay": False, "discovered_peers": [], "filter_verified": True,
    }


@pytest.mark.asyncio
async def test_crawl_logs_filter_failure_for_known_peer_that_drops(db):
    """A known filter peer that fails to answer this cycle logs a filter failure."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_filter_peer("1.1.1.1", 12024, 0x40, 70019, "/a/", now - 3600)
    await db.add_crawl_peers([("1.1.1.1", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts WHERE ip='1.1.1.1'"
    )
    rows = await cursor.fetchall()
    assert [(r["capability"], r["success"]) for r in rows] == [("filter", 0)]


@pytest.mark.asyncio
async def test_crawl_does_not_log_unknown_unverified_peer(db):
    cfg = make_config()
    await db.add_crawl_peers([("9.9.9.9", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute("SELECT COUNT(*) FROM peer_attempts")
    assert (await cursor.fetchone())[0] == 0


@pytest.mark.asyncio
async def test_crawl_clears_filter_validation_on_services_downgrade(db):
    """A filter-validated peer that stops advertising NODE_COMPACT_FILTERS gets cleared."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_filter_peer("5.5.5.5", 12024, 0x44d, 70019, "/up/", now - 3600)
    await db.add_crawl_peers([("5.5.5.5", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        r = filter_only_result(ip, port)
        r["services"] = NODE_NETWORK | 0x04   # NETWORK | BLOOM only — no COMPACT_FILTERS
        r["filter_verified"] = False
        return r

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT filter_validated_at FROM peers WHERE ip='5.5.5.5'"
    )
    assert (await cursor.fetchone())["filter_validated_at"] is None
```

- [ ] **Step 2: Run the crawler tests — expect failures/errors**

Run: `.venv/bin/pytest tests/test_crawler.py -q`
Expected: FAIL/ERROR — new tests reference the filter-only behavior the crawler doesn't produce yet, and the import of `NODE_NETWORK, NODE_COMPACT_FILTERS` plus removed `verified_result` shifts behavior.

- [ ] **Step 3: Strip bloom from `handshake_peer`**

In `seeder/crawler.py`:
- Change the import (lines 10–15) to drop `NODE_BLOOM` and `build_filterload`:

```python
from seeder.protocol import (
    HEADER_SIZE, NODE_COMPACT_FILTERS,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr,
    build_getcfheaders, parse_addr_payload,
)
```

- In `handshake_peer`, delete the `bloom_verified = False` line (77) and the entire NODE_BLOOM filterload block (comment + `if info["services"] & NODE_BLOOM:` through the `bloom_verified = False` in the except, lines 85–103). Delete `info["bloom_verified"] = bloom_verified` (148). Keep the NODE_COMPACT_FILTERS block and `info["filter_verified"] = filter_verified`.

- [ ] **Step 4: Strip bloom from `crawl_cycle`**

In `crawl_cycle`:
- Delete `known_bloom = await storage.get_validated_peer_set(capability="bloom")` (178); change priority to `priority = known_filter | static_set` (181).
- Delete `bloom_found = 0` (188) and `bloom_found` from the `nonlocal` (195).
- Delete `bloom_verified = bool(...)` (204) and the bloom attempt-logging `if` block (208–212).
- Delete the bloom downgrade block (228–231).
- Delete the bloom upsert block (`if bloom_verified:` ... 238–247).
- Delete `"bloom_found": bloom_found,` from the returned `stats` dict (277).

The filter attempt-logging, filter downgrade, and `upsert_filter_peer` blocks remain unchanged.

- [ ] **Step 5: Fix the initial-crawl log in `seeder.py`**

Replace `seeder.py` lines 57–61:

```python
    stats = await crawl_cycle(config, storage)
    set_last_crawl_time(int(time.time()))
    log.info(
        "Initial crawl complete: %d filter peers verified",
        stats.get("filter_found", 0),
    )
```

- [ ] **Step 6: Run crawler tests, then full suite**

Run: `.venv/bin/pytest tests/test_crawler.py -q` → Expected: PASS.
Run: `.venv/bin/pytest tests/ -q` → Expected: PASS (storage tests still use bloom methods that still exist).

- [ ] **Step 7: Commit**

```bash
git add seeder/crawler.py seeder.py tests/test_crawler.py
git commit -m "feat(crawler): discover/validate compact-filter peers only

Remove the NODE_BLOOM filterload probe, known_bloom priority set, bloom
attempt-logging, bloom downgrade and bloom upsert. Fix the initial-crawl
log line to filter-only."
```

---

### Task 3: Storage — filter-only methods + stats (drops the `capability` param)

**Files:**
- Modify: `seeder/storage.py` (remove `upsert_bloom_peer` 95–111; make `clear_validation`, `get_ranked_peers`, `get_above_threshold_count`, `get_validated_peer_set`, `record_attempt` filter-only; rewrite `get_stats` 383–435)
- Modify: `seeder/api.py` (the `get_ranked_peers(capability="filter", ...)` call — remove the kwarg)
- Modify: `seeder/crawler.py` (call sites — remove `capability=...` kwargs)
- Test: `tests/test_storage.py`
- Test: `tests/test_api.py` (its `_seed_filter_peer` helper still passes `capability="filter"` to `record_attempt`)

**Interfaces:**
- Produces (final signatures):
  - `record_attempt(ip, port, *, success, ts)` — writes `capability='filter'` internally.
  - `clear_validation(ip, port)` — clears `filter_validated_at`.
  - `get_validated_peer_set()` → `set[(ip, port)]` of filter-validated peers.
  - `get_ranked_peers(*, window_days, prior_attempts, prior_successes, longevity_cap_days, longevity_weight, inclusion_threshold, max_age_hours, limit)` → filter peers.
  - `get_above_threshold_count(*, threshold, prior_attempts, prior_successes, window_days, max_age_hours)` → int.
  - `get_stats(*, max_age_hours, threshold, prior_attempts, prior_successes, window_days)` → dict with keys `peers_total`, `peers_filter_validated`, `peers_filter_above_threshold`, `all_peers_known`, `attempts_7d_total`.

- [ ] **Step 1: Rewrite the storage tests to filter-only**

In `tests/test_storage.py`:

Change `RANK_DEFAULTS` (205–215) — drop the `capability` key:

```python
RANK_DEFAULTS = dict(
    window_days=7,
    prior_attempts=10,
    prior_successes=5,
    longevity_cap_days=60,
    longevity_weight=0.30,
    inclusion_threshold=0.50,
    max_age_hours=6,
    limit=25,
)
```

**Delete** these bloom-specific tests entirely: `test_record_attempt_separates_capabilities`, `test_get_validated_peer_set_bloom`, `test_ranked_filter_excludes_bloom_only_peers`, `test_get_above_threshold_count_filters_capability`, `test_upsert_both_capabilities_independent`, `test_clear_validation_bloom_preserves_filter`, `test_clear_validation_rejects_unknown_capability`.

**Keep unchanged** (they exercise retained migration/schema code): `test_add_and_get_crawl_peers`, `test_mark_crawled`, `test_migration_runs_in_storage_init`, `test_migration_when_bloom_peer_attempts_table_is_missing`, `test_upsert_filter_peer`.

**Mechanically convert** every remaining test that uses bloom to filter, applying this transformation rule verbatim:
- `await db.upsert_bloom_peer(IP, PORT, SVC, PV, UA, TS)` → `await db.upsert_filter_peer(IP, PORT, SVC, PV, UA, TS)`
- `await db.record_attempt(IP, PORT, capability="bloom", success=S, ts=T)` → `await db.record_attempt(IP, PORT, success=S, ts=T)`
- `capability="filter"` kwarg on `record_attempt` → remove it too
- `await db.get_validated_peer_set(capability="bloom"|"filter")` → `await db.get_validated_peer_set()`
- `await db.get_ranked_peers(**{**RANK_DEFAULTS, "capability": ...})` → `await db.get_ranked_peers(**RANK_DEFAULTS)`; a bare `get_ranked_peers(**RANK_DEFAULTS)` stays as-is
- `await db.get_above_threshold_count(capability="bloom"|"filter", ...)` → drop the `capability=` kwarg
- `await db.clear_validation(IP, PORT, capability="filter")` → `await db.clear_validation(IP, PORT)`
- Raw SQL `capability='bloom'` in a test's own SELECT/INSERT → `capability='filter'`

The tests to convert this way: `test_prune_old_peers`, `test_peer_attempts_table_exists`, `test_record_attempt_success_and_failure`, `test_prune_attempts_drops_old_rows`, `test_get_validated_peer_set_filter`, `test_prune_cascades_to_attempts`, `test_ranked_peer_with_one_success_is_included`, `test_ranked_peer_below_threshold_excluded`, `test_ranked_higher_uptime_wins_over_longevity`, `test_ranked_longevity_breaks_tie`, `test_ranked_respects_max_age_hours`, `test_ranked_respects_limit`, `test_ranked_attempts_outside_window_ignored`, `test_get_attempts_total`, `test_get_above_threshold_count`, `test_ranked_filter_picks_up_filter_validated_peer` (drop its second `capability="bloom"` assertion block), `test_clear_validation_filter_preserves_bloom` (rename to `test_clear_validation_drops_filter`, keep only the filter/services/last_seen assertions), `test_clear_validation_drops_peer_from_ranked_filter`.

Worked example — `test_get_attempts_total` becomes:

```python
@pytest.mark.asyncio
async def test_get_attempts_total(db):
    now = int(time.time())
    in_window = now - 1 * 3600
    out_window = now - 8 * 86400
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=in_window)
    await db.record_attempt("1.1.1.1", 12024, success=False, ts=in_window - 1)
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=out_window)
    total = await db.get_attempts_total(window_days=7)
    assert total == 2
```

Rewrite `test_get_stats` (49–82) to the filter-only shape:

```python
@pytest.mark.asyncio
async def test_get_stats(db):
    now = int(time.time())
    await db.upsert_filter_peer("2.2.2.2", 12024, 0x40, 70019, "/f/", now)
    for i in range(20):
        await db.record_attempt("2.2.2.2", 12024, success=True, ts=now - i)
    await db.add_crawl_peers([("3.3.3.3", 12024), ("4.4.4.4", 12024)])

    stats = await db.get_stats(
        max_age_hours=6, threshold=0.50, prior_attempts=10,
        prior_successes=5, window_days=7,
    )
    assert stats["peers_total"] == 1
    assert stats["peers_filter_validated"] == 1
    assert stats["peers_filter_above_threshold"] == 1
    assert stats["all_peers_known"] == 2
    assert stats["attempts_7d_total"] == 20
    assert "peers_bloom_validated" not in stats
    assert "peers_bloom_above_threshold" not in stats
```

- [ ] **Step 2: Run storage tests — expect failures**

Run: `.venv/bin/pytest tests/test_storage.py -q`
Expected: FAIL — methods still require `capability`, `upsert_bloom_peer` still referenced by nothing (fine), `get_stats` still returns bloom keys.

- [ ] **Step 3: Make the storage methods filter-only**

In `seeder/storage.py`:
- Delete `upsert_bloom_peer` (95–111).
- `clear_validation` — replace with:

```python
    async def clear_validation(self, ip: str, port: int):
        """Clear filter_validated_at for a peer that stopped advertising
        NODE_COMPACT_FILTERS — drops it from /peers on the next call."""
        await self._db.execute(
            "UPDATE peers SET filter_validated_at = NULL WHERE ip = ? AND port = ?",
            (ip, port),
        )
        await self._db.commit()
```

- `record_attempt` — replace with:

```python
    async def record_attempt(self, ip: str, port: int, *, success: bool, ts: int):
        """Log a single filter crawl-attempt outcome against a peer."""
        await self._db.execute(
            "INSERT OR REPLACE INTO peer_attempts (ip, port, ts, capability, success) "
            "VALUES (?, ?, ?, 'filter', ?)",
            (ip, port, ts, 1 if success else 0),
        )
        await self._db.commit()
```

- `get_validated_peer_set` — drop the param and the if/elif; hardcode the column:

```python
    async def get_validated_peer_set(self) -> set[tuple[str, int]]:
        """(ip, port) tuples for peers ever validated for compact filters."""
        cursor = await self._db.execute(
            "SELECT ip, port FROM peers WHERE filter_validated_at IS NOT NULL"
        )
        rows = await cursor.fetchall()
        return {(r["ip"], r["port"]) for r in rows}
```

- `get_ranked_peers` — remove the `capability` parameter and the `if capability ...` block (169–174); set `validated_col = "filter_validated_at"` directly; in the SQL change the JOIN predicate `AND a.capability = ?` to `AND a.capability = 'filter'`, and remove the leading `capability,` from the params tuple (220–221).
- `get_above_threshold_count` — same treatment: remove the `capability` parameter, remove the if/elif (256–261) → `validated_col = "filter_validated_at"`, change `AND a.capability = ?` to `AND a.capability = 'filter'`, remove the leading `capability,` from the params tuple.
- `get_stats` — replace (383–435) with:

```python
    async def get_stats(
        self, *, max_age_hours: int, threshold: float,
        prior_attempts: int, prior_successes: int, window_days: int,
    ) -> dict:
        cursor = await self._db.execute("SELECT COUNT(*) FROM peers")
        total = (await cursor.fetchone())[0]

        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM peers WHERE filter_validated_at IS NOT NULL"
        )
        filter_validated = (await cursor.fetchone())[0]

        cursor = await self._db.execute("SELECT COUNT(*) FROM all_peers")
        all_known = (await cursor.fetchone())[0]

        filter_above = await self.get_above_threshold_count(
            threshold=threshold, prior_attempts=prior_attempts,
            prior_successes=prior_successes, window_days=window_days,
            max_age_hours=max_age_hours,
        )
        attempts_total = await self.get_attempts_total(window_days=window_days)

        return {
            "peers_total": total,
            "peers_filter_validated": filter_validated,
            "peers_filter_above_threshold": filter_above,
            "all_peers_known": all_known,
            "attempts_7d_total": attempts_total,
        }
```

Leave the schema `executescript`, the `bloom_peers` migration block, `upsert_filter_peer`, `get_attempts_total`, `add_crawl_peers`, `get_uncrawled_peers`, `mark_crawled`, `prune_attempts`, `prune` untouched.

- [ ] **Step 4: Update the call sites (production + the test_api helper)**

- `seeder/api.py` `handle_peers`: remove the `capability="filter",` line from the `get_ranked_peers(...)` call.
- `seeder/crawler.py` `crawl_cycle`: `known_filter = await storage.get_validated_peer_set()`; the filter attempt-log call → `await storage.record_attempt(ip, port, success=filter_verified, ts=ts)`; the filter downgrade call → `await storage.clear_validation(ip, port)`.
- `tests/test_api.py`: in the `_seed_filter_peer` helper, change `await db.record_attempt(ip, port, capability="filter", success=True, ts=now)` → `await db.record_attempt(ip, port, success=True, ts=now)`.

- [ ] **Step 5: Run storage + api tests, then full suite**

Run: `.venv/bin/pytest tests/test_storage.py tests/test_api.py -q` → Expected: PASS.
Run: `.venv/bin/pytest tests/ -q` → Expected: PASS (all files).

- [ ] **Step 6: Commit**

```bash
git add seeder/storage.py seeder/api.py seeder/crawler.py tests/test_storage.py tests/test_api.py
git commit -m "feat(storage): filter-only methods; drop capability param and bloom stats

Remove upsert_bloom_peer; make ranking/attempt/validation methods
filter-only; /stats drops peers_bloom_*. Schema left dormant."
```

---

### Task 4: Protocol — delete dead `build_filterload`

**Files:**
- Modify: `seeder/protocol.py` (delete `build_filterload`, ~160–175)

**Interfaces:** none consumed/produced (dead code removal; `NODE_BLOOM` stays).

- [ ] **Step 1: Confirm no references remain**

Run: `git grep -n build_filterload -- '*.py'`
Expected: no matches outside `seeder/protocol.py` (crawler's import was removed in Task 2). If `tests/` or elsewhere match, stop and reconcile.

- [ ] **Step 2: Delete the function**

In `seeder/protocol.py` delete the `build_filterload` function and its docstring/comment block (the function that builds the tiny filterload probe). Keep `NODE_BLOOM` and everything else.

- [ ] **Step 3: Run the full suite**

Run: `.venv/bin/pytest tests/ -q`
Expected: PASS (test_protocol.py never referenced `build_filterload`).

- [ ] **Step 4: Commit**

```bash
git add seeder/protocol.py
git commit -m "refactor(protocol): remove dead build_filterload (bloom probe)"
```

---

### Task 5: Docs — reframe to compact-filter-only

**Files:**
- Modify: `docs/operator-quickstart.md`
- Modify: `README.md`
- Modify: `docs/wallet-integration.md`, `docs/wallet-bip158-integration.md`

**Interfaces:** none (docs).

- [ ] **Step 1: operator-quickstart.md**

Delete the "## Legacy (BIP37 bloom) — optional" section entirely. Add a short line under "Enable it" stating operators should NOT set `peerbloomfilters` — bloom (BIP37) is retired; enable only `blockfilterindex=basic` + `peerblockfilters=1`.

- [ ] **Step 2: README.md**

Read `README.md`, then update: reframe the seeder as compact-filter-only (single `filter` capability); document that `/peers` (and every legacy alias/param) returns filter peers with `"capability":"filter"`; update the `/stats` field list to `peers_total`, `peers_filter_validated`, `peers_filter_above_threshold`, `all_peers_known`, `attempts_7d_total`; remove bloom/dandelion capability descriptions; note that `/api/peers/bloom` and `/api/peers/all` still work but transparently serve filter.

- [ ] **Step 3: wallet docs**

In `docs/wallet-integration.md` and `docs/wallet-bip158-integration.md`, mark the bloom endpoints deprecated: recommend `/api/peers` or `?capability=filter`; note legacy bloom endpoints soft-alias to filter and `/stats` no longer carries bloom counts.

- [ ] **Step 4: Commit**

```bash
git add docs/operator-quickstart.md README.md docs/wallet-integration.md docs/wallet-bip158-integration.md
git commit -m "docs: reframe seeder as compact-filter-only; drop legacy BIP37 guidance"
```

---

## Adjacent actions (NOT in this plan — tracked separately)

- **Operator/own nodes:** remove `peerbloomfilters=1` (services `0x44d`→`0x449`; filter validation via `getcfheaders` unaffected).
- **Wallet repo:** switch the peer query to `?capability=filter` or rely on the new filter-only default.

## Deploy & rollback (after merge-readiness on `feat/retire-bloom`)

1. `ssh root@digiscope.me` → `cd /opt/dgb-bloom-seeder` → `cp bloom_seeder.db bloom_seeder.db.bak-PRE-RETIRE-BLOOM-$(date +%Y%m%d)`.
2. `git fetch origin && git checkout feat/retire-bloom` (deploy the branch to prod first).
3. `.venv/bin/python3 -m pytest tests/` — green on the server.
4. `pm2 restart bloom-seeder`; wait for "API listening".
5. Verify: `/peers`, `/api/peers/bloom` + `/api/peers/all` (all return filter peers, `"capability":"filter"`), `/stats` (no `peers_bloom_*`), and that the ~13 filter peers still validate over a crawl cycle.
6. Merge `feat/retire-bloom` → `master`, push; on the server `git checkout master && git pull --ff-only`; delete the branch local + origin.

Rollback: `git checkout <prior-sha>` + restore the DB backup + `pm2 restart`. Schema is unchanged, so the DB backup is belt-and-suspenders.
