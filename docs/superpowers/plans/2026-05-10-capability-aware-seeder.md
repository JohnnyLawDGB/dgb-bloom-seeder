# Capability-Aware Seeder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generalize the seeder from bloom-only to a capability-indexed catalog that detects, validates, ranks, and serves both `NODE_BLOOM (0x04)` and `NODE_COMPACT_FILTERS (0x40)` peers, with per-capability uptime tracking and a default `/peers` endpoint that serves filter peers (falling through to bloom if empty).

**Architecture:** Existing `bloom_peers` and `bloom_peer_attempts` tables are renamed and extended to `peers` and `peer_attempts` (with a `capability` column on attempts and per-capability validation timestamps on peers). A new BIP 157 `getcfheaders` validator runs in the crawler alongside the existing `filterload`. Per-capability ranking uses the same Bayesian-smoothed × longevity score, computed independently per capability. The API gains query-string capability filtering with server-side fallthrough.

**Tech Stack:** Python 3.10+, asyncio, aiosqlite, aiohttp, pytest, pytest-asyncio. Existing venv at `/home/polloloco/dgb-bloom-seeder/.venv`.

**Spec:** `docs/superpowers/specs/2026-05-10-capability-aware-seeder-design.md`

**Working directory for all commands:** `/home/polloloco/dgb-bloom-seeder`
**Branch:** `feat/capability-aware-seeder`

**File map:**
- Modify: `seeder/protocol.py` — add `NODE_COMPACT_FILTERS`, `build_getcfheaders`
- Modify: `seeder/storage.py` — rename tables, migrate, add capability params to all methods
- Modify: `seeder/crawler.py` — add filter validation, per-capability snapshots, per-capability attempt logging
- Modify: `seeder/api.py` — capability query parsing, default fallthrough, new response shape
- Modify: `seeder/config.py` — add `static_peers` field
- Modify: `config.yaml` — add `static_peers` section
- Modify: `seeder.py` — load static peers on startup
- Modify: `tests/test_protocol.py` — `build_getcfheaders` tests
- Modify: `tests/test_storage.py` — migration test, per-capability ranking, validated-set helper
- Modify: `tests/test_crawler.py` — per-capability attempt logging
- Create: `tests/test_api.py` — HTTP-layer coverage (closes the gap noted in the previous feature's final review)
- Modify: `README.md` — updated `/peers` and `/stats` examples

---

### Task 1: Wire protocol — `NODE_COMPACT_FILTERS` and `build_getcfheaders`

**Files:**
- Modify: `seeder/protocol.py`
- Modify: `tests/test_protocol.py`

- [ ] **Step 1: Append tests to `tests/test_protocol.py`**

```python
def test_node_compact_filters_constant():
    from seeder.protocol import NODE_COMPACT_FILTERS
    assert NODE_COMPACT_FILTERS == 0x40


def test_build_getcfheaders_default():
    from seeder.protocol import build_getcfheaders, parse_message_header, DGB_MAGIC
    msg = build_getcfheaders(DGB_MAGIC)
    cmd, plen, _ = parse_message_header(msg[:24])
    assert cmd == "getcfheaders"
    # Payload: 1 byte filter_type + 4 bytes start_height + 32 bytes stop_hash = 37 bytes
    assert plen == 37
    payload = msg[24:]
    assert payload[0] == 0   # filter_type = 0 (basic)
    # start_height default = 1, little-endian uint32
    assert struct.unpack_from("<I", payload, 1)[0] == 1
    # stop_hash default = all zeros
    assert payload[5:37] == b"\x00" * 32


def test_build_getcfheaders_explicit_args():
    from seeder.protocol import build_getcfheaders, DGB_MAGIC
    stop = bytes(range(32))
    msg = build_getcfheaders(DGB_MAGIC, filter_type=0, start_height=12345, stop_hash=stop)
    payload = msg[24:]
    assert payload[0] == 0
    assert struct.unpack_from("<I", payload, 1)[0] == 12345
    assert payload[5:37] == stop
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_protocol.py -k "compact_filters or getcfheaders" -v`

Expected: FAIL — `NODE_COMPACT_FILTERS` and `build_getcfheaders` don't exist yet.

- [ ] **Step 3: Add the constant and the builder**

In `seeder/protocol.py`, find the `# Service flags` section (currently has `NODE_NETWORK`, `NODE_BLOOM`, `NODE_WITNESS`) and add:

```python
NODE_COMPACT_FILTERS = 0x40
```

Then, immediately after the existing `build_getaddr` function, add:

```python
def build_getcfheaders(
    magic: bytes,
    filter_type: int = 0,
    start_height: int = 1,
    stop_hash: bytes = b"\x00" * 32,
) -> bytes:
    """Build a getcfheaders message (BIP 157) for validating compact-filter support.

    Default stop_hash is all zeros — not a valid block hash, but non-supporting peers
    disconnect on getcfheaders regardless of payload validity. Supporting peers respond
    (cfheaders/notfound) or briefly hold the connection."""
    payload = struct.pack("<B", filter_type)
    payload += struct.pack("<I", start_height)
    if len(stop_hash) != 32:
        raise ValueError("stop_hash must be 32 bytes")
    payload += stop_hash
    return make_message(magic, "getcfheaders", payload)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_protocol.py -v`

Expected: all green, including 3 new tests.

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/protocol.py tests/test_protocol.py
git commit -m "feat: protocol — NODE_COMPACT_FILTERS and build_getcfheaders (BIP 157)"
```

---

### Task 2: Schema migration — `bloom_peers`/`bloom_peer_attempts` → `peers`/`peer_attempts`

This task renames the tables, adds the new columns/capability column, and updates every storage method that references the old names. Method signatures and behavior stay the same in this task — capability awareness comes in subsequent tasks. After this task, all existing tests still pass against the new schema.

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write a migration test that starts with the old schema**

Append to `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_migration_from_bloom_schema_preserves_data():
    """Set up the OLD schema with sample rows, then run Storage.init()
    and verify the data lands in the new tables correctly."""
    import aiosqlite
    db_path = ":memory:"

    # Create old schema directly via aiosqlite (bypass Storage.init()).
    raw = await aiosqlite.connect(db_path)
    raw.row_factory = aiosqlite.Row
    await raw.executescript("""
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
        CREATE TABLE bloom_peer_attempts (
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            success INTEGER NOT NULL,
            PRIMARY KEY (ip, port, ts)
        );
        CREATE TABLE all_peers (
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            last_crawled INTEGER DEFAULT 0,
            PRIMARY KEY (ip, port)
        );
    """)
    await raw.execute(
        "INSERT INTO bloom_peers VALUES (?,?,?,?,?,?,?)",
        ("1.1.1.1", 12024, 5, 70019, "/test/", 1700000100, 1700000000),
    )
    await raw.execute(
        "INSERT INTO bloom_peer_attempts VALUES (?,?,?,?)",
        ("1.1.1.1", 12024, 1700000100, 1),
    )
    await raw.commit()
    await raw.close()


@pytest.mark.asyncio
async def test_migration_runs_in_storage_init(tmp_path):
    """Realistic path: a real SQLite file with the old schema, then Storage.init() against it."""
    import aiosqlite
    db_path = str(tmp_path / "old.db")

    raw = await aiosqlite.connect(db_path)
    await raw.executescript("""
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
        CREATE TABLE bloom_peer_attempts (
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            ts INTEGER NOT NULL,
            success INTEGER NOT NULL,
            PRIMARY KEY (ip, port, ts)
        );
        CREATE TABLE all_peers (
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            last_crawled INTEGER DEFAULT 0,
            PRIMARY KEY (ip, port)
        );
    """)
    await raw.execute(
        "INSERT INTO bloom_peers VALUES (?,?,?,?,?,?,?)",
        ("1.1.1.1", 12024, 5, 70019, "/test/", 1700000100, 1700000000),
    )
    await raw.execute(
        "INSERT INTO bloom_peer_attempts VALUES (?,?,?,?)",
        ("1.1.1.1", 12024, 1700000100, 1),
    )
    await raw.commit()
    await raw.close()

    # Now run Storage.init() against this DB
    store = Storage(db_path)
    await store.init()

    # Verify new tables have the migrated data
    cursor = await store._db.execute(
        "SELECT ip, port, services, last_seen, first_seen, "
        "bloom_validated_at, filter_validated_at FROM peers"
    )
    rows = await cursor.fetchall()
    assert len(rows) == 1
    r = rows[0]
    assert r["ip"] == "1.1.1.1"
    assert r["bloom_validated_at"] == 1700000100   # equals last_seen
    assert r["filter_validated_at"] is None

    cursor = await store._db.execute(
        "SELECT ip, port, ts, capability, success FROM peer_attempts"
    )
    rows = await cursor.fetchall()
    assert len(rows) == 1
    assert rows[0]["capability"] == "bloom"
    assert rows[0]["success"] == 1

    # Verify old tables are dropped
    cursor = await store._db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('bloom_peers', 'bloom_peer_attempts')"
    )
    rows = await cursor.fetchall()
    assert rows == []

    # Second init should be idempotent — no error
    await store.close()
    store2 = Storage(db_path)
    await store2.init()
    cursor = await store2._db.execute("SELECT COUNT(*) FROM peers")
    n = (await cursor.fetchone())[0]
    assert n == 1
    await store2.close()
```

- [ ] **Step 2: Update existing storage tests to use the new schema names**

In `tests/test_storage.py`, find every test that references `bloom_peers` or `bloom_peer_attempts` directly via `db._db.execute(...)`. There should be a small number — most tests use the public Storage API which we'll keep working.

Specifically rename in test bodies:
- `INSERT INTO bloom_peer_attempts` → `INSERT INTO peer_attempts (ip, port, ts, capability, success) VALUES (?, ?, ?, 'bloom', ?)` (note the new capability column)
- `SELECT ... FROM bloom_peer_attempts` → `SELECT ... FROM peer_attempts WHERE capability='bloom'`
- `SELECT ... FROM bloom_peers` → `SELECT ... FROM peers`

The existing test `test_bloom_peer_attempts_table_exists` should be renamed to `test_peer_attempts_table_exists` and updated to insert with capability='bloom'.

- [ ] **Step 3: Replace `Storage.init()` to use the new schema and run migration**

In `seeder/storage.py`, replace the entire `init` method:

```python
    async def init(self):
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row

        # Create new schema (idempotent).
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS peers (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                services INTEGER NOT NULL,
                protocol_version INTEGER,
                user_agent TEXT,
                last_seen INTEGER NOT NULL,
                first_seen INTEGER NOT NULL,
                bloom_validated_at  INTEGER,
                filter_validated_at INTEGER,
                PRIMARY KEY (ip, port)
            );
            CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen);
            CREATE INDEX IF NOT EXISTS idx_peers_bloom
                ON peers(bloom_validated_at)  WHERE bloom_validated_at  IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_peers_filter
                ON peers(filter_validated_at) WHERE filter_validated_at IS NOT NULL;

            CREATE TABLE IF NOT EXISTS all_peers (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                last_crawled INTEGER DEFAULT 0,
                PRIMARY KEY (ip, port)
            );

            CREATE TABLE IF NOT EXISTS peer_attempts (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                capability TEXT NOT NULL,
                success INTEGER NOT NULL,
                PRIMARY KEY (ip, port, ts, capability)
            );
            CREATE INDEX IF NOT EXISTS idx_attempts_cap_ts
                ON peer_attempts(capability, ts);
            CREATE INDEX IF NOT EXISTS idx_attempts_peer_cap_ts
                ON peer_attempts(ip, port, capability, ts);
        """)

        # One-time migration from old (bloom_peers, bloom_peer_attempts) schema.
        cursor = await self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bloom_peers'"
        )
        if (await cursor.fetchone()) is not None:
            await self._db.executescript("""
                BEGIN;
                INSERT OR IGNORE INTO peers
                    (ip, port, services, protocol_version, user_agent,
                     last_seen, first_seen, bloom_validated_at, filter_validated_at)
                SELECT ip, port, services, protocol_version, user_agent,
                       last_seen, first_seen, last_seen, NULL
                FROM bloom_peers;

                INSERT OR IGNORE INTO peer_attempts
                    (ip, port, ts, capability, success)
                SELECT ip, port, ts, 'bloom', success
                FROM bloom_peer_attempts;

                DROP TABLE bloom_peer_attempts;
                DROP TABLE bloom_peers;
                COMMIT;
            """)

        await self._db.commit()
```

- [ ] **Step 4: Update every other Storage method to reference the new tables**

In `seeder/storage.py`, find and update each method below. Behavior stays the same — just rename `bloom_peers` → `peers`, `bloom_peer_attempts` → `peer_attempts`. For `peer_attempts` operations, hardcode `capability='bloom'` for now (the next task makes this a parameter).

`upsert_bloom_peer` becomes:

```python
    async def upsert_bloom_peer(
        self, ip: str, port: int, services: int,
        protocol_version: int, user_agent: str, seen_at: int
    ):
        """Upsert a bloom-validated peer. Sets bloom_validated_at = seen_at."""
        await self._db.execute("""
            INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                               last_seen, first_seen, bloom_validated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ip, port) DO UPDATE SET
                services = excluded.services,
                protocol_version = excluded.protocol_version,
                user_agent = excluded.user_agent,
                last_seen = excluded.last_seen,
                bloom_validated_at = excluded.bloom_validated_at
        """, (ip, port, services, protocol_version, user_agent, seen_at, seen_at, seen_at))
        await self._db.commit()
```

`record_attempt` becomes (capability hardcoded for now):

```python
    async def record_attempt(self, ip: str, port: int, success: bool, ts: int):
        """Log a bloom-attempt outcome. (Capability becomes a param in Task 3.)"""
        await self._db.execute(
            """
            INSERT OR REPLACE INTO peer_attempts (ip, port, ts, capability, success)
            VALUES (?, ?, ?, 'bloom', ?)
            """,
            (ip, port, ts, 1 if success else 0),
        )
        await self._db.commit()
```

`prune_attempts` is unchanged behaviorally; just check it queries `peer_attempts`:

```python
    async def prune_attempts(self, window_days: int) -> int:
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "DELETE FROM peer_attempts WHERE ts < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount
```

`get_known_bloom_peer_set` (renamed in Task 4 — for now just rename internals):

```python
    async def get_known_bloom_peer_set(self) -> set[tuple[str, int]]:
        """Return all peers ever bloom-validated. Renamed to get_validated_peer_set in Task 4."""
        cursor = await self._db.execute(
            "SELECT ip, port FROM peers WHERE bloom_validated_at IS NOT NULL"
        )
        rows = await cursor.fetchall()
        return {(r["ip"], r["port"]) for r in rows}
```

`prune` (cascade-delete now references new tables):

```python
    async def prune(self, max_age_hours: int = 24) -> int:
        cutoff = int(time.time()) - max_age_hours * 3600
        await self._db.execute(
            """
            DELETE FROM peer_attempts
            WHERE (ip, port) IN (
                SELECT ip, port FROM peers WHERE last_seen < ?
            )
            """,
            (cutoff,),
        )
        cursor = await self._db.execute(
            "DELETE FROM peers WHERE last_seen < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount
```

`get_attempts_total` (just table rename):

```python
    async def get_attempts_total(self, window_days: int) -> int:
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM peer_attempts WHERE ts >= ?", (cutoff,)
        )
        return (await cursor.fetchone())[0]
```

`get_ranked_peers` SQL: rename `bloom_peers` → `peers`, `bloom_peer_attempts` → `peer_attempts`. For now, the JOIN still doesn't filter on capability — Task 5 adds that. Keep the WHERE/ORDER unchanged. The relevant chunk after rename:

```python
            """
            WITH stats AS (
                SELECT bp.ip, bp.port, bp.services,
                       bp.last_seen, bp.first_seen,
                       bp.protocol_version, bp.user_agent,
                       COALESCE(SUM(a.success), 0)   AS successes_7d,
                       COALESCE(COUNT(a.ts), 0)      AS attempts_7d
                FROM peers bp
                LEFT JOIN peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                GROUP BY bp.ip, bp.port
            ),
            ...
            """
```

(And the `services` column is still selected — verified to come back in the result dicts.)

Same rename in `get_above_threshold_count`.

`get_stats` keeps its current behavior + field names; per-capability fields come in Task 6.

- [ ] **Step 5: Run all tests**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: all green. The new migration tests pass. Existing storage tests pass against the renamed schema. Crawler and protocol tests pass unchanged.

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: schema — rename bloom_peers/bloom_peer_attempts to peers/peer_attempts, with one-shot migration"
```

---

### Task 3: Storage — `record_attempt(capability)` parameter

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`
- Modify: `seeder/crawler.py` (call site update)

- [ ] **Step 1: Update existing test `test_record_attempt_success_and_failure` to pass capability**

In `tests/test_storage.py`, find `test_record_attempt_success_and_failure` and replace its body so the call now passes capability:

```python
@pytest.mark.asyncio
async def test_record_attempt_success_and_failure(db):
    await db.record_attempt("1.2.3.4", 12024, capability="bloom", success=True, ts=1700000000)
    await db.record_attempt("1.2.3.4", 12024, capability="bloom", success=False, ts=1700000001)
    cursor = await db._db.execute(
        "SELECT ts, success FROM peer_attempts "
        "WHERE ip=? AND port=? AND capability=? ORDER BY ts",
        ("1.2.3.4", 12024, "bloom"),
    )
    rows = await cursor.fetchall()
    assert [(r["ts"], r["success"]) for r in rows] == [
        (1700000000, 1),
        (1700000001, 0),
    ]


@pytest.mark.asyncio
async def test_record_attempt_separates_capabilities(db):
    """Same peer, different capabilities, same ts — both rows persist."""
    await db.record_attempt("1.2.3.4", 12024, capability="bloom",  success=True, ts=1700000000)
    await db.record_attempt("1.2.3.4", 12024, capability="filter", success=False, ts=1700000000)
    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts "
        "WHERE ip=? AND port=? ORDER BY capability",
        ("1.2.3.4", 12024),
    )
    rows = await cursor.fetchall()
    assert [(r["capability"], r["success"]) for r in rows] == [
        ("bloom", 1),
        ("filter", 0),
    ]
```

- [ ] **Step 2: Run the tests, expect FAIL**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -k "record_attempt" -v`

Expected: FAIL — current `record_attempt` signature doesn't take `capability`.

- [ ] **Step 3: Update `record_attempt`**

In `seeder/storage.py`, replace the `record_attempt` method:

```python
    async def record_attempt(
        self, ip: str, port: int, *, capability: str, success: bool, ts: int
    ):
        """Log a single crawl-attempt outcome against a peer for a specific capability.

        capability must be 'bloom' or 'filter'."""
        if capability not in ("bloom", "filter"):
            raise ValueError(f"unknown capability: {capability!r}")
        await self._db.execute(
            """
            INSERT OR REPLACE INTO peer_attempts (ip, port, ts, capability, success)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ip, port, ts, capability, 1 if success else 0),
        )
        await self._db.commit()
```

- [ ] **Step 4: Update the existing crawler call site**

In `seeder/crawler.py`, find the existing call `await storage.record_attempt(ip, port, success=verified, ts=ts)` and update to:

```python
            if was_known or verified:
                await storage.record_attempt(
                    ip, port, capability="bloom", success=verified, ts=ts,
                )
```

(Task 9 will replace this whole gate; for now, just keep the call signature compatible.)

- [ ] **Step 5: Run the full test suite, verify green**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green. Both new record_attempt tests pass; existing crawler tests pass with the updated call site.

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py seeder/crawler.py tests/test_storage.py
git commit -m "feat: storage.record_attempt takes capability parameter (bloom/filter)"
```

---

### Task 4: Storage — `get_validated_peer_set(capability)` (renames `get_known_bloom_peer_set`)

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`
- Modify: `seeder/crawler.py` (call site update)

- [ ] **Step 1: Replace the existing test for the helper**

In `tests/test_storage.py`, find `test_get_known_bloom_peer_set` and replace it with:

```python
@pytest.mark.asyncio
async def test_get_validated_peer_set_bloom(db):
    now = int(time.time())
    # bloom-validated peer
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    # not bloom-validated
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/b/', ?, ?, NULL, ?)
    """, (now, now, now))
    await db._db.commit()

    s = await db.get_validated_peer_set(capability="bloom")
    assert s == {("1.1.1.1", 12024)}


@pytest.mark.asyncio
async def test_get_validated_peer_set_filter(db):
    now = int(time.time())
    # filter-validated peer
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/b/', ?, ?, NULL, ?)
    """, (now, now, now))
    # bloom-only peer
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    await db._db.commit()

    s = await db.get_validated_peer_set(capability="filter")
    assert s == {("2.2.2.2", 12024)}
```

- [ ] **Step 2: Run, expect FAIL (`get_validated_peer_set` doesn't exist)**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -k "get_validated_peer_set" -v`

Expected: FAIL.

- [ ] **Step 3: Replace `get_known_bloom_peer_set` with `get_validated_peer_set(capability)`**

In `seeder/storage.py`, find `get_known_bloom_peer_set` and replace with:

```python
    async def get_validated_peer_set(
        self, *, capability: str
    ) -> set[tuple[str, int]]:
        """Return (ip, port) tuples for peers ever validated for the given capability.

        capability must be 'bloom' or 'filter'."""
        if capability == "bloom":
            col = "bloom_validated_at"
        elif capability == "filter":
            col = "filter_validated_at"
        else:
            raise ValueError(f"unknown capability: {capability!r}")
        # Column name is whitelisted above so f-string interpolation is safe.
        cursor = await self._db.execute(
            f"SELECT ip, port FROM peers WHERE {col} IS NOT NULL"
        )
        rows = await cursor.fetchall()
        return {(r["ip"], r["port"]) for r in rows}
```

- [ ] **Step 4: Update the crawler call site**

In `seeder/crawler.py`, find `known_bloom = await storage.get_known_bloom_peer_set()` and replace with:

```python
    known_bloom = await storage.get_validated_peer_set(capability="bloom")
```

(Task 9 will add the filter snapshot. For now this preserves behavior.)

- [ ] **Step 5: Run all tests, verify green**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green.

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py seeder/crawler.py tests/test_storage.py
git commit -m "feat: storage.get_validated_peer_set replaces get_known_bloom_peer_set, takes capability"
```

---

### Task 5: Storage — `get_ranked_peers(capability)` and `get_above_threshold_count(capability)`

These two methods both filter by capability now: only peers ever validated for the queried capability are eligible, and only that capability's attempt history feeds the score.

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`
- Modify: `seeder/api.py` (call site update)

- [ ] **Step 1: Update existing ranking tests + add capability tests**

In `tests/test_storage.py`, update `RANK_DEFAULTS` to include capability:

```python
RANK_DEFAULTS = dict(
    capability="bloom",
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

The existing 7 ranking tests already pass `**RANK_DEFAULTS`, so they'll continue to test the bloom-capability path.

Append two new tests:

```python
@pytest.mark.asyncio
async def test_ranked_filter_excludes_bloom_only_peers(db):
    """A peer validated for bloom only should NOT appear in ?capability=filter results."""
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/bloom-only/", now)
    await db.record_attempt("1.1.1.1", 12024, capability="bloom", success=True, ts=now)

    args = {**RANK_DEFAULTS, "capability": "filter"}
    peers = await db.get_ranked_peers(**args)
    assert peers == []


@pytest.mark.asyncio
async def test_ranked_filter_picks_up_filter_validated_peer(db):
    """A peer validated for filter only is returned by ?capability=filter and not bloom."""
    now = int(time.time())
    # Insert filter-only validated peer manually
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/filter-only/', ?, ?, NULL, ?)
    """, (now, now, now))
    await db._db.commit()
    await db.record_attempt("2.2.2.2", 12024, capability="filter", success=True, ts=now)

    args_filter = {**RANK_DEFAULTS, "capability": "filter"}
    peers = await db.get_ranked_peers(**args_filter)
    assert len(peers) == 1
    assert peers[0]["ip"] == "2.2.2.2"

    args_bloom = {**RANK_DEFAULTS, "capability": "bloom"}
    peers = await db.get_ranked_peers(**args_bloom)
    assert peers == []


@pytest.mark.asyncio
async def test_get_above_threshold_count_filters_capability(db):
    """Above-threshold count is per-capability."""
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/bloom/", now)
    for i in range(50):
        await db.record_attempt("1.1.1.1", 12024, capability="bloom", success=True, ts=now - i)

    bloom_count = await db.get_above_threshold_count(
        capability="bloom",
        threshold=0.50, prior_attempts=10, prior_successes=5,
        window_days=7, max_age_hours=6,
    )
    filter_count = await db.get_above_threshold_count(
        capability="filter",
        threshold=0.50, prior_attempts=10, prior_successes=5,
        window_days=7, max_age_hours=6,
    )
    assert bloom_count == 1
    assert filter_count == 0
```

- [ ] **Step 2: Run tests, expect FAIL**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -v`

Expected: FAIL — `get_ranked_peers` and `get_above_threshold_count` don't take `capability`.

- [ ] **Step 3: Update `get_ranked_peers` to filter by capability**

In `seeder/storage.py`, replace `get_ranked_peers`:

```python
    async def get_ranked_peers(
        self,
        *,
        capability: str,
        window_days: int,
        prior_attempts: int,
        prior_successes: int,
        longevity_cap_days: int,
        longevity_weight: float,
        inclusion_threshold: float,
        max_age_hours: int,
        limit: int,
    ) -> list[dict]:
        """Return peers above threshold for the given capability, sorted by composite score DESC.

        capability must be 'bloom' or 'filter'."""
        if capability == "bloom":
            validated_col = "bloom_validated_at"
        elif capability == "filter":
            validated_col = "filter_validated_at"
        else:
            raise ValueError(f"unknown capability: {capability!r}")

        now = int(time.time())
        window_cutoff = now - window_days * 86400
        last_seen_cutoff = now - max_age_hours * 3600
        longevity_now = now
        tenure_now = now

        cursor = await self._db.execute(
            f"""
            WITH stats AS (
                SELECT bp.ip, bp.port, bp.services,
                       bp.last_seen, bp.first_seen,
                       bp.protocol_version, bp.user_agent,
                       bp.bloom_validated_at, bp.filter_validated_at,
                       COALESCE(SUM(a.success), 0) AS successes_7d,
                       COALESCE(COUNT(a.ts), 0)    AS attempts_7d
                FROM peers bp
                LEFT JOIN peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.capability = ?
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                  AND bp.{validated_col} IS NOT NULL
                GROUP BY bp.ip, bp.port
            ),
            scored AS (
                SELECT *,
                       (successes_7d + ?) * 1.0 / (attempts_7d + ?) AS uptime_score,
                       MIN((? - first_seen) / 86400.0 / ?, 1.0)     AS longevity_bonus
                FROM stats
            )
            SELECT ip, port, services,
                   last_seen, first_seen,
                   protocol_version, user_agent,
                   bloom_validated_at, filter_validated_at,
                   successes_7d, attempts_7d,
                   uptime_score,
                   uptime_score * (1 + ? * longevity_bonus) AS composite_score,
                   (? - first_seen) / 86400.0              AS tenure_days
            FROM scored
            WHERE uptime_score >= ?
            ORDER BY composite_score DESC, last_seen DESC
            LIMIT ?
            """,
            (
                capability,
                window_cutoff,
                last_seen_cutoff,
                prior_successes,
                prior_attempts,
                longevity_now,
                longevity_cap_days,
                longevity_weight,
                tenure_now,
                inclusion_threshold,
                limit,
            ),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
```

(`validated_col` is whitelisted via the if/elif/else above so the f-string is safe.)

- [ ] **Step 4: Update `get_above_threshold_count` similarly**

In `seeder/storage.py`, replace `get_above_threshold_count`:

```python
    async def get_above_threshold_count(
        self,
        *,
        capability: str,
        threshold: float,
        prior_attempts: int,
        prior_successes: int,
        window_days: int,
        max_age_hours: int,
    ) -> int:
        """How many peers would appear in /peers?capability=... given current threshold."""
        if capability == "bloom":
            validated_col = "bloom_validated_at"
        elif capability == "filter":
            validated_col = "filter_validated_at"
        else:
            raise ValueError(f"unknown capability: {capability!r}")

        now = int(time.time())
        window_cutoff = now - window_days * 86400
        last_seen_cutoff = now - max_age_hours * 3600

        cursor = await self._db.execute(
            f"""
            WITH stats AS (
                SELECT bp.ip, bp.port,
                       COALESCE(SUM(a.success), 0) AS successes_7d,
                       COALESCE(COUNT(a.ts), 0)    AS attempts_7d
                FROM peers bp
                LEFT JOIN peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.capability = ?
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                  AND bp.{validated_col} IS NOT NULL
                GROUP BY bp.ip, bp.port
            )
            SELECT COUNT(*) FROM stats
            WHERE (successes_7d + ?) * 1.0 / (attempts_7d + ?) >= ?
            """,
            (
                capability,
                window_cutoff,
                last_seen_cutoff,
                prior_successes,
                prior_attempts,
                threshold,
            ),
        )
        return (await cursor.fetchone())[0]
```

- [ ] **Step 5: Update the API call site (just to keep tests green; full API refactor in Task 10)**

In `seeder/api.py`, find the `handle_peers` call to `get_ranked_peers` and add `capability="bloom"` so it stays compilable:

```python
    async def handle_peers(request: web.Request) -> web.Response:
        peers = await storage.get_ranked_peers(
            capability="bloom",
            window_days=config.ranking_window_days,
            prior_attempts=config.ranking_prior_attempts,
            prior_successes=config.ranking_prior_successes,
            longevity_cap_days=config.ranking_longevity_cap_days,
            longevity_weight=config.ranking_longevity_weight,
            inclusion_threshold=config.ranking_inclusion_threshold,
            max_age_hours=config.api_max_age_hours,
            limit=config.api_max_results,
        )
        crawl_age = int(time.time() - _last_crawl_time) if _last_crawl_time else -1
        return web.json_response({
            "peers": peers,
            "count": len(peers),
            "crawl_age_seconds": crawl_age,
        })
```

Find the `get_above_threshold_count` call inside `Storage.get_stats` and add `capability="bloom"` similarly.

- [ ] **Step 6: Run all tests**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green.

- [ ] **Step 7: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py seeder/api.py tests/test_storage.py
git commit -m "feat: per-capability ranking — get_ranked_peers and get_above_threshold_count take capability"
```

---

### Task 6: Storage — per-capability `get_stats`

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`
- Modify: `seeder/api.py`

- [ ] **Step 1: Update the existing `test_get_stats` to expect the new fields**

Replace `test_get_stats` in `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_get_stats(db):
    now = int(time.time())
    # Bloom-validated peer with attempts
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    for i in range(50):
        await db.record_attempt("1.1.1.1", 12024, capability="bloom", success=True, ts=now - i)

    # Filter-validated peer with attempts
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/f/', ?, ?, NULL, ?)
    """, (now, now, now))
    await db._db.commit()
    for i in range(20):
        await db.record_attempt("2.2.2.2", 12024, capability="filter", success=True, ts=now - i)

    # Random crawl-queue peers
    await db.add_crawl_peers([("3.3.3.3", 12024), ("4.4.4.4", 12024)])

    stats = await db.get_stats(
        max_age_hours=6,
        threshold=0.50,
        prior_attempts=10,
        prior_successes=5,
        window_days=7,
    )
    assert stats["peers_total"] == 2
    assert stats["peers_bloom_validated"] == 1
    assert stats["peers_filter_validated"] == 1
    assert stats["peers_bloom_above_threshold"] == 1
    assert stats["peers_filter_above_threshold"] == 1
    assert stats["all_peers_known"] == 2
    assert stats["attempts_7d_total"] == 70
```

- [ ] **Step 2: Run, expect FAIL**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_get_stats -v`

Expected: FAIL — current `get_stats` returns the old field names.

- [ ] **Step 3: Replace `Storage.get_stats`**

In `seeder/storage.py`, replace `get_stats`:

```python
    async def get_stats(
        self,
        *,
        max_age_hours: int,
        threshold: float,
        prior_attempts: int,
        prior_successes: int,
        window_days: int,
    ) -> dict:
        cursor = await self._db.execute("SELECT COUNT(*) FROM peers")
        total = (await cursor.fetchone())[0]

        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM peers WHERE bloom_validated_at IS NOT NULL"
        )
        bloom_validated = (await cursor.fetchone())[0]

        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM peers WHERE filter_validated_at IS NOT NULL"
        )
        filter_validated = (await cursor.fetchone())[0]

        cursor = await self._db.execute("SELECT COUNT(*) FROM all_peers")
        all_known = (await cursor.fetchone())[0]

        bloom_above = await self.get_above_threshold_count(
            capability="bloom",
            threshold=threshold,
            prior_attempts=prior_attempts,
            prior_successes=prior_successes,
            window_days=window_days,
            max_age_hours=max_age_hours,
        )
        filter_above = await self.get_above_threshold_count(
            capability="filter",
            threshold=threshold,
            prior_attempts=prior_attempts,
            prior_successes=prior_successes,
            window_days=window_days,
            max_age_hours=max_age_hours,
        )

        attempts_total = await self.get_attempts_total(window_days=window_days)

        return {
            "peers_total": total,
            "peers_bloom_validated": bloom_validated,
            "peers_filter_validated": filter_validated,
            "peers_bloom_above_threshold": bloom_above,
            "peers_filter_above_threshold": filter_above,
            "all_peers_known": all_known,
            "attempts_7d_total": attempts_total,
        }
```

- [ ] **Step 4: Update the API `handle_stats` to reflect the new field names**

In `seeder/api.py`, the `handle_stats` body's only change is that the new dict shape is returned as-is — `_last_crawl_time` and `_start_time` are still tacked on. The current implementation should already work without modification:

```python
    async def handle_stats(request: web.Request) -> web.Response:
        stats = await storage.get_stats(
            max_age_hours=config.api_max_age_hours,
            threshold=config.ranking_inclusion_threshold,
            prior_attempts=config.ranking_prior_attempts,
            prior_successes=config.ranking_prior_successes,
            window_days=config.ranking_window_days,
        )
        stats["last_crawl"] = _last_crawl_time
        stats["uptime_seconds"] = int(time.time() - _start_time)
        return web.json_response(stats)
```

(No change needed — `get_stats` returns the new shape, `handle_stats` just wraps it.)

- [ ] **Step 5: Run all tests**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green.

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: get_stats — per-capability validated and above-threshold counts"
```

---

### Task 7: Storage — `upsert_filter_peer` (parallel to `upsert_bloom_peer`)

The crawler's bloom path uses `upsert_bloom_peer` to set `bloom_validated_at`. Add the equivalent for filter validation.

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Append a test**

```python
@pytest.mark.asyncio
async def test_upsert_filter_peer(db):
    now = int(time.time())
    await db.upsert_filter_peer("2.2.2.2", 12024, 0x40, 70019, "/filter/", now)

    cursor = await db._db.execute(
        "SELECT bloom_validated_at, filter_validated_at, last_seen FROM peers WHERE ip=?",
        ("2.2.2.2",),
    )
    row = await cursor.fetchone()
    assert row["bloom_validated_at"] is None
    assert row["filter_validated_at"] == now
    assert row["last_seen"] == now


@pytest.mark.asyncio
async def test_upsert_both_capabilities_independent(db):
    """Bloom upsert sets bloom_validated_at; filter upsert sets filter_validated_at; the other stays."""
    t1 = int(time.time()) - 100
    t2 = int(time.time())
    await db.upsert_bloom_peer("9.9.9.9", 12024, 0x44, 70019, "/x/", t1)
    await db.upsert_filter_peer("9.9.9.9", 12024, 0x44, 70019, "/x/", t2)

    cursor = await db._db.execute(
        "SELECT bloom_validated_at, filter_validated_at FROM peers WHERE ip=?",
        ("9.9.9.9",),
    )
    row = await cursor.fetchone()
    assert row["bloom_validated_at"] == t1
    assert row["filter_validated_at"] == t2
```

- [ ] **Step 2: Run, expect FAIL**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -k "upsert_filter or upsert_both" -v`

Expected: FAIL.

- [ ] **Step 3: Add `upsert_filter_peer`**

In `seeder/storage.py`, just after `upsert_bloom_peer`, add:

```python
    async def upsert_filter_peer(
        self, ip: str, port: int, services: int,
        protocol_version: int, user_agent: str, seen_at: int
    ):
        """Upsert a filter-validated peer. Sets filter_validated_at = seen_at.
        Does NOT modify bloom_validated_at (use upsert_bloom_peer for that)."""
        await self._db.execute("""
            INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                               last_seen, first_seen, filter_validated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ip, port) DO UPDATE SET
                services = excluded.services,
                protocol_version = excluded.protocol_version,
                user_agent = excluded.user_agent,
                last_seen = excluded.last_seen,
                filter_validated_at = excluded.filter_validated_at
        """, (ip, port, services, protocol_version, user_agent, seen_at, seen_at, seen_at))
        await self._db.commit()
```

- [ ] **Step 4: Run all tests, verify green**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green.

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: storage.upsert_filter_peer — sets filter_validated_at independently"
```

---

### Task 8: Crawler — `handshake_peer` adds filter validation

**Files:**
- Modify: `seeder/crawler.py`
- Modify: `tests/test_crawler.py`

This task adds the BIP 157 `getcfheaders` round-trip to `handshake_peer`. The function gains a new returned key `filter_verified` (parallel to `bloom_verified`).

- [ ] **Step 1: Update `seeder/crawler.py` imports**

At the top of `seeder/crawler.py`, find the `from seeder.protocol import ...` block and add `NODE_COMPACT_FILTERS` and `build_getcfheaders`:

```python
from seeder.protocol import (
    HEADER_SIZE, NODE_BLOOM, NODE_COMPACT_FILTERS,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr, build_filterload,
    build_getcfheaders, parse_addr_payload,
)
```

- [ ] **Step 2: Add filter validation block to `handshake_peer`**

In `seeder/crawler.py`, locate the existing bloom-verification block inside `handshake_peer`. It currently looks like:

```python
            # If peer advertises NODE_BLOOM, verify by sending a filterload.
            if info["services"] & NODE_BLOOM:
                writer.write(build_filterload(magic))
                await writer.drain()
                ...
                bloom_verified = ...
```

Immediately after the existing bloom-verification block (and before the `Send getaddr` block), add the filter-verification block:

```python
            # If peer advertises NODE_COMPACT_FILTERS, verify with a getcfheaders round-trip.
            # Mirrors the bloom path: a peer that doesn't actually support BIP 157 will
            # disconnect on this message.
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
```

Initialize `filter_verified = False` near the top of the function alongside the existing `bloom_verified = False`.

Set `info["filter_verified"] = filter_verified` immediately before the existing `info["bloom_verified"] = bloom_verified` line.

- [ ] **Step 3: Add a crawler test for filter validation**

Append to `tests/test_crawler.py`:

```python
def filter_only_result(ip: str, port: int) -> dict:
    return {
        "ip": ip,
        "port": port,
        "protocol_version": 70019,
        "services": NODE_NETWORK | 0x40,  # NODE_COMPACT_FILTERS
        "user_agent": "/DigiByte:8.26.2/",
        "timestamp": 0,
        "start_height": 0,
        "relay": False,
        "discovered_peers": [],
        "bloom_verified": False,
        "filter_verified": True,
    }


@pytest.mark.asyncio
async def test_crawl_logs_filter_attempt_when_newly_verified(db):
    cfg = make_config()
    await db.add_crawl_peers([("8.8.8.8", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return filter_only_result(ip, port)

    with patch("seeder.crawler.handshake_peer",
               new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts WHERE ip='8.8.8.8' ORDER BY capability"
    )
    rows = await cursor.fetchall()
    # We expect the new gate (Task 9) to log a filter attempt — but Task 8 is the
    # protocol-level wiring, not the gate. This test will start passing once Task 9
    # lands. For now it MAY fail with no rows — that's fine; revisit in Task 9.
    # (Marker test: skip on Task 8, pass on Task 9.)
    pytest.skip("attempt-logging gate added in Task 9")
```

(The skip ensures Task 8's commit doesn't fail CI. Task 9 will replace `pytest.skip(...)` with assertions.)

- [ ] **Step 4: Run tests, verify nothing broke**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green (the new test is currently skipped; existing tests still pass — `handshake_peer` returns `filter_verified=False` for peers without the bit, which doesn't affect existing test setups).

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/crawler.py tests/test_crawler.py
git commit -m "feat: handshake_peer validates compact-filter support via getcfheaders"
```

---

### Task 9: Crawler — per-capability snapshots and per-capability attempt logging in `crawl_cycle`

This task replaces the bloom-only attempt-logging gate in `crawl_cycle` with per-capability gates and uses both bloom and filter snapshots for the priority pool.

**Files:**
- Modify: `seeder/crawler.py`
- Modify: `tests/test_crawler.py`

- [ ] **Step 1: Update test fixtures and replace the skipped Task 8 test**

In `tests/test_crawler.py`, find the `pytest.skip(...)` test added in Task 8 and replace its body:

```python
@pytest.mark.asyncio
async def test_crawl_logs_filter_attempt_when_newly_verified(db):
    cfg = make_config()
    await db.add_crawl_peers([("8.8.8.8", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return filter_only_result(ip, port)

    with patch("seeder.crawler.handshake_peer",
               new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts WHERE ip='8.8.8.8' ORDER BY capability"
    )
    rows = await cursor.fetchall()
    assert [(r["capability"], r["success"]) for r in rows] == [("filter", 1)]
```

Append three more tests:

```python
@pytest.mark.asyncio
async def test_crawl_logs_both_capabilities_for_dual_validated_peer(db):
    cfg = make_config()
    await db.add_crawl_peers([("9.9.9.9", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        r = verified_result(ip, port)
        r["filter_verified"] = True   # both bits work
        return r

    with patch("seeder.crawler.handshake_peer",
               new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts WHERE ip='9.9.9.9' ORDER BY capability"
    )
    rows = await cursor.fetchall()
    assert [(r["capability"], r["success"]) for r in rows] == [
        ("bloom",  1),
        ("filter", 1),
    ]


@pytest.mark.asyncio
async def test_crawl_logs_bloom_failure_for_known_bloom_peer_with_no_filter(db):
    """A peer in the bloom-validated set that fails this cycle logs a bloom failure but
    NOT a filter row (it was never filter-validated)."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now - 3600)
    await db.add_crawl_peers([("1.1.1.1", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None  # connection failed entirely

    with patch("seeder.crawler.handshake_peer",
               new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts WHERE ip='1.1.1.1'"
    )
    rows = await cursor.fetchall()
    assert [(r["capability"], r["success"]) for r in rows] == [("bloom", 0)]


@pytest.mark.asyncio
async def test_crawl_does_not_log_bloom_attempt_for_filter_only_peer(db):
    """A filter-only-validated peer in priority should NOT accumulate bloom-failure rows."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_filter_peer("2.2.2.2", 12024, 0x40, 70019, "/f/", now - 3600)
    await db.add_crawl_peers([("2.2.2.2", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None  # offline

    with patch("seeder.crawler.handshake_peer",
               new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT capability, success FROM peer_attempts WHERE ip='2.2.2.2'"
    )
    rows = await cursor.fetchall()
    # Filter row logged (peer was in filter-priority set), bloom row NOT logged.
    assert [(r["capability"], r["success"]) for r in rows] == [("filter", 0)]
```

- [ ] **Step 2: Run, expect failures**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_crawler.py -v`

Expected: 4 of the new tests fail (current `crawl_cycle` only logs bloom).

- [ ] **Step 3: Replace `crawl_cycle`**

In `seeder/crawler.py`, replace the entire `crawl_cycle` function:

```python
async def crawl_cycle(config: Config, storage: Storage) -> dict:
    """Run one crawl cycle. Returns stats dict."""
    log.info("Starting crawl cycle")
    start = time.time()

    # DNS top-up if queue is small
    peers = await storage.get_uncrawled_peers(limit=config.crawl_max_peers)
    if len(peers) < 50:
        dns_peers = await resolve_seeds(config.dns_seeds, config.dgb_port)
        await storage.add_crawl_peers(dns_peers)
        peers = await storage.get_uncrawled_peers(limit=config.crawl_max_peers)

    # Per-capability snapshots taken before workers run.
    known_bloom  = await storage.get_validated_peer_set(capability="bloom")
    known_filter = await storage.get_validated_peer_set(capability="filter")
    priority     = known_bloom | known_filter

    # Priority peers always get crawled this cycle, taking budget from the queue.
    budget = max(0, config.crawl_max_peers - len(priority))
    normal = await storage.get_uncrawled_peers(limit=budget) if budget > 0 else []
    peers  = list(priority) + [p for p in normal if p not in priority]

    bloom_found = 0
    filter_found = 0
    total_checked = 0
    new_peers_discovered = 0
    sem = asyncio.Semaphore(config.crawl_concurrency)

    async def check_peer(ip: str, port: int):
        nonlocal bloom_found, filter_found, total_checked, new_peers_discovered
        async with sem:
            await storage.mark_crawled(ip, port)
            result = await handshake_peer(
                ip, port, config.dgb_magic, config.crawl_timeout
            )
            total_checked += 1

            ts = int(time.time())
            bloom_verified  = bool(result and result.get("bloom_verified"))
            filter_verified = bool(result and result.get("filter_verified"))

            # Per-capability attempt logging gates.
            if (ip, port) in known_bloom or bloom_verified:
                await storage.record_attempt(
                    ip, port, capability="bloom",
                    success=bloom_verified, ts=ts,
                )
            if (ip, port) in known_filter or filter_verified:
                await storage.record_attempt(
                    ip, port, capability="filter",
                    success=filter_verified, ts=ts,
                )

            if result is None:
                return

            # Upsert per capability that just verified.
            if bloom_verified:
                bloom_found += 1
                await storage.upsert_bloom_peer(
                    ip, port, result["services"],
                    result["protocol_version"],
                    result["user_agent"],
                    ts,
                )
                log.info("BLOOM VERIFIED: %s:%d %s (services=0x%02x)",
                         ip, port, result["user_agent"], result["services"])

            if filter_verified:
                filter_found += 1
                await storage.upsert_filter_peer(
                    ip, port, result["services"],
                    result["protocol_version"],
                    result["user_agent"],
                    ts,
                )
                log.info("FILTER VERIFIED: %s:%d %s (services=0x%02x)",
                         ip, port, result["user_agent"], result["services"])

            # Add discovered peers to crawl queue
            discovered = result.get("discovered_peers", [])
            if discovered:
                new_peers_discovered += len(discovered)
                await storage.add_crawl_peers(
                    [(p["ip"], p["port"]) for p in discovered]
                )

    tasks = [check_peer(ip, port) for ip, port in peers]
    await asyncio.gather(*tasks)

    pruned = await storage.prune(max_age_hours=config.prune_hours)
    pruned_attempts = await storage.prune_attempts(window_days=config.ranking_window_days)

    elapsed = time.time() - start
    stats = {
        "checked": total_checked,
        "bloom_found": bloom_found,
        "filter_found": filter_found,
        "new_peers": new_peers_discovered,
        "pruned": pruned,
        "pruned_attempts": pruned_attempts,
        "elapsed_seconds": round(elapsed, 1),
    }
    log.info("Crawl complete: %s", stats)
    return stats
```

- [ ] **Step 4: Run all tests, verify green**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green.

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/crawler.py tests/test_crawler.py
git commit -m "feat: crawler logs per-capability attempts; priority pool unions bloom + filter"
```

---

### Task 10: Config — `static_peers` field

**Files:**
- Modify: `seeder/config.py`
- Modify: `config.yaml`

- [ ] **Step 1: Add the field to the `Config` dataclass**

In `seeder/config.py`, add to the `Config` dataclass after the `dns_seeds` field:

```python
    # Manually-known peers loaded into the crawl queue on startup.
    # Each entry: {ip: str, port: int, source: str (optional, operator-only metadata)}
    static_peers: list[dict] = field(default_factory=list)
```

- [ ] **Step 2: Add `static_peers` to `config.yaml`**

In `config.yaml`, append after the `dns_seeds:` block:

```yaml

# Manually-known peers loaded into the crawl queue on every startup.
# Use this for known-good nodes (especially on non-default ports) that
# might not be reachable via DNS-seed gossip alone.
static_peers:
  - { ip: "129.212.182.152", port: 12024, source: "filter-vps" }
  - { ip: "174.131.163.123", port: 12024, source: "filter-vps" }
  - { ip: "149.76.66.207",   port: 22024, source: "johnnylaw" }
```

- [ ] **Step 3: Verify config loads**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -c "from seeder.config import load_config; c = load_config(); print(len(c.static_peers), c.static_peers[0])"`

Expected output: `3 {'ip': '129.212.182.152', 'port': 12024, 'source': 'filter-vps'}`

- [ ] **Step 4: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/config.py config.yaml
git commit -m "feat: config — static_peers list for known nodes (incl. non-default ports)"
```

---

### Task 11: Entry point — load static peers on startup

**Files:**
- Modify: `seeder.py`

- [ ] **Step 1: Add the static-peer load step in `main()`**

In `seeder.py`, find the block that seeds DNS peers:

```python
    # Seed initial peers from DNS
    dns_peers = await resolve_seeds(config.dns_seeds, config.dgb_port)
    await storage.add_crawl_peers(dns_peers)
    log.info("Seeded %d peers from DNS", len(dns_peers))
```

Immediately after that, add:

```python
    # Load any operator-configured static peers
    if config.static_peers:
        static = [(p["ip"], p["port"]) for p in config.static_peers]
        await storage.add_crawl_peers(static)
        log.info("Loaded %d static peers from config", len(static))
```

- [ ] **Step 2: Verify the seeder still imports cleanly**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -c "import seeder as s; print('ok')"`

Wait, that fails — `seeder` is also the package name. Use:

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -c "import importlib.util; spec = importlib.util.spec_from_file_location('seeder_main', 'seeder.py'); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); print('ok')"`

Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder.py
git commit -m "feat: seeder.py loads static_peers into crawl queue at startup"
```

---

### Task 12: API — capability query parsing + default fallthrough

**Files:**
- Modify: `seeder/api.py`
- Modify: `tests/test_api.py` (NEW file)

- [ ] **Step 1: Create `tests/test_api.py` with capability-query coverage**

Create `tests/test_api.py`:

```python
"""HTTP-layer tests for the seeder API using aiohttp's TestClient."""

import time
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from seeder.api import create_app
from seeder.config import Config
from seeder.storage import Storage


@pytest_asyncio.fixture
async def db():
    s = Storage(":memory:")
    await s.init()
    yield s
    await s.close()


def make_config() -> Config:
    return Config()


@pytest_asyncio.fixture
async def client(db) -> TestClient:
    cfg = make_config()
    app = create_app(cfg, db)
    server = TestServer(app)
    client = TestClient(server)
    await client.start_server()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_peers_default_returns_filter_when_present(client, db):
    """Default /peers returns filter peers when filter peers exist above threshold."""
    now = int(time.time())
    # Seed a filter-validated peer
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/f/', ?, ?, NULL, ?)
    """, (now, now, now))
    await db._db.commit()
    await db.record_attempt("2.2.2.2", 12024, capability="filter", success=True, ts=now)

    resp = await client.get("/peers")
    assert resp.status == 200
    data = await resp.json()
    assert data["capability"] == "filter"
    assert data["count"] == 1
    assert data["peers"][0]["ip"] == "2.2.2.2"


@pytest.mark.asyncio
async def test_peers_default_falls_through_to_bloom(client, db):
    """When no filter peers, default /peers returns bloom peers with capability='bloom'."""
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/b/", now)
    await db.record_attempt("1.1.1.1", 12024, capability="bloom", success=True, ts=now)

    resp = await client.get("/peers")
    assert resp.status == 200
    data = await resp.json()
    assert data["capability"] == "bloom"
    assert data["count"] == 1
    assert data["peers"][0]["ip"] == "1.1.1.1"


@pytest.mark.asyncio
async def test_peers_capability_bloom_explicit(client, db):
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/b/", now)
    await db.record_attempt("1.1.1.1", 12024, capability="bloom", success=True, ts=now)

    resp = await client.get("/peers?capability=bloom")
    data = await resp.json()
    assert data["capability"] == "bloom"
    assert data["count"] == 1


@pytest.mark.asyncio
async def test_peers_capability_filter_explicit(client, db):
    now = int(time.time())
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/f/', ?, ?, NULL, ?)
    """, (now, now, now))
    await db._db.commit()
    await db.record_attempt("2.2.2.2", 12024, capability="filter", success=True, ts=now)

    resp = await client.get("/peers?capability=filter")
    data = await resp.json()
    assert data["capability"] == "filter"
    assert data["count"] == 1
    assert data["peers"][0]["ip"] == "2.2.2.2"


@pytest.mark.asyncio
async def test_peers_capability_combined(client, db):
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/b/", now)
    await db.record_attempt("1.1.1.1", 12024, capability="bloom", success=True, ts=now)
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x40, 70019, '/f/', ?, ?, NULL, ?)
    """, (now, now, now))
    await db._db.commit()
    await db.record_attempt("2.2.2.2", 12024, capability="filter", success=True, ts=now)

    resp = await client.get("/peers?capability=filter|bloom")
    data = await resp.json()
    assert data["capability"] == "filter+bloom"
    assert data["count"] == 2
    # Filter rows come first.
    assert data["peers"][0]["peer_capability"] == "filter"
    assert data["peers"][1]["peer_capability"] == "bloom"


@pytest.mark.asyncio
async def test_peers_unknown_capability_returns_400(client):
    resp = await client.get("/peers?capability=unknown")
    assert resp.status == 400
    data = await resp.json()
    assert "error" in data


@pytest.mark.asyncio
async def test_peers_empty_default(client, db):
    """No peers at all → empty response, capability='filter' (default attempted)."""
    resp = await client.get("/peers")
    data = await resp.json()
    assert data["count"] == 0
    assert data["peers"] == []
    # Even with no peers, the response should declare which list it tried.
    assert data["capability"] in ("filter", "bloom")
```

- [ ] **Step 2: Run new tests, expect FAIL**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_api.py -v`

Expected: FAIL — current `/peers` doesn't take a capability param and the response shape doesn't include `capability`.

- [ ] **Step 3: Replace `handle_peers` in `seeder/api.py`**

In `seeder/api.py`, replace the entire `handle_peers` inner function:

```python
    async def handle_peers(request: web.Request) -> web.Response:
        cap = request.query.get("capability", "").lower()

        # Validate
        if cap == "":
            mode = "default"
        elif cap == "bloom":
            mode = "bloom"
        elif cap == "filter":
            mode = "filter"
        elif cap in ("bloom|filter", "filter|bloom"):
            mode = "combined"
        else:
            return web.json_response(
                {"error": f"invalid capability: {cap!r}"}, status=400
            )

        async def fetch(capability: str) -> list[dict]:
            return await storage.get_ranked_peers(
                capability=capability,
                window_days=config.ranking_window_days,
                prior_attempts=config.ranking_prior_attempts,
                prior_successes=config.ranking_prior_successes,
                longevity_cap_days=config.ranking_longevity_cap_days,
                longevity_weight=config.ranking_longevity_weight,
                inclusion_threshold=config.ranking_inclusion_threshold,
                max_age_hours=config.api_max_age_hours,
                limit=config.api_max_results,
            )

        if mode == "default":
            peers = await fetch("filter")
            response_capability = "filter"
            if not peers:
                peers = await fetch("bloom")
                response_capability = "bloom"
            for p in peers:
                p["peer_capability"] = response_capability
        elif mode == "bloom":
            peers = await fetch("bloom")
            response_capability = "bloom"
            for p in peers:
                p["peer_capability"] = "bloom"
        elif mode == "filter":
            peers = await fetch("filter")
            response_capability = "filter"
            for p in peers:
                p["peer_capability"] = "filter"
        elif mode == "combined":
            filter_peers = await fetch("filter")
            for p in filter_peers:
                p["peer_capability"] = "filter"
            bloom_peers = await fetch("bloom")
            for p in bloom_peers:
                p["peer_capability"] = "bloom"
            peers = filter_peers + bloom_peers
            response_capability = "filter+bloom"

        # Enrich each peer with services_hex and capabilities array (Task 13).
        for p in peers:
            p["services_hex"] = f"0x{p['services']:x}"
            p["capabilities"] = _services_to_capabilities(p["services"])

        crawl_age = int(time.time() - _last_crawl_time) if _last_crawl_time else -1
        return web.json_response({
            "peers": peers,
            "count": len(peers),
            "capability": response_capability,
            "crawl_age_seconds": crawl_age,
        })
```

- [ ] **Step 4: Add the `_services_to_capabilities` helper to `seeder/api.py`**

At module top of `seeder/api.py` (just after the imports), add:

```python
SERVICE_FLAG_NAMES = [
    (0x001, "NETWORK"),
    (0x002, "GETUTXO"),
    (0x004, "BLOOM"),
    (0x008, "WITNESS"),
    (0x040, "COMPACT_FILTERS"),
    (0x400, "NETWORK_LIMITED"),
]


def _services_to_capabilities(services: int) -> list[str]:
    """Translate a services bitmask into a list of human-readable capability names."""
    return [name for bit, name in SERVICE_FLAG_NAMES if services & bit]
```

- [ ] **Step 5: Run all tests, verify green**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: green.

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/api.py tests/test_api.py
git commit -m "feat: /peers — capability query, default fallthrough, services_hex, capabilities array"
```

---

### Task 13: Live smoke test

Production-style validation against the real DigiByte network. Mirrors the smoke test from the previous feature.

**Files:** none (validation only)

- [ ] **Step 1: Clean state**

```bash
cd /home/polloloco/dgb-bloom-seeder && rm -f bloom_seeder.db /tmp/seeder_smoke.log /tmp/seeder_pid
```

- [ ] **Step 2: Start the seeder in the background**

```bash
cd /home/polloloco/dgb-bloom-seeder && nohup .venv/bin/python3 seeder.py > /tmp/seeder_smoke.log 2>&1 &
echo $! > /tmp/seeder_pid
```

- [ ] **Step 3: Poll the log until the API is listening**

```bash
for i in $(seq 1 90); do
  if grep -q "API listening" /tmp/seeder_smoke.log 2>/dev/null; then
    echo "API up after ~$((i * 4))s"
    break
  fi
  sleep 4
done
tail -30 /tmp/seeder_smoke.log
```

If the API never comes up, capture the log and stop. Static peer load lines should appear: `Loaded 3 static peers from config`.

- [ ] **Step 4: Verify each /peers variant**

```bash
echo "=== default ==="
curl -s http://localhost:8025/peers | .venv/bin/python3 -c "
import json, sys
d = json.load(sys.stdin)
print('capability:', d['capability'], '  count:', d['count'])
for p in d['peers'][:2]:
    print(' ', p['ip'], p.get('peer_capability'), p.get('uptime_score'))
"

echo "=== ?capability=bloom ==="
curl -s 'http://localhost:8025/peers?capability=bloom' | .venv/bin/python3 -c "
import json, sys
d = json.load(sys.stdin)
print('capability:', d['capability'], '  count:', d['count'])
"

echo "=== ?capability=filter ==="
curl -s 'http://localhost:8025/peers?capability=filter' | .venv/bin/python3 -c "
import json, sys
d = json.load(sys.stdin)
print('capability:', d['capability'], '  count:', d['count'])
"

echo "=== ?capability=filter|bloom ==="
curl -s 'http://localhost:8025/peers?capability=filter%7Cbloom' | .venv/bin/python3 -c "
import json, sys
d = json.load(sys.stdin)
print('capability:', d['capability'], '  count:', d['count'])
"

echo "=== invalid ==="
curl -s -o /dev/null -w 'HTTP %{http_code}\n' 'http://localhost:8025/peers?capability=foo'
```

Expected:
- Default returns either filter (if any) or bloom (fallthrough)
- Both single-capability variants succeed
- Combined returns count = filter + bloom
- Invalid returns HTTP 400

- [ ] **Step 5: Verify /stats shape**

```bash
curl -s http://localhost:8025/stats | .venv/bin/python3 -m json.tool
```

Required keys: `peers_total`, `peers_bloom_validated`, `peers_filter_validated`, `peers_bloom_above_threshold`, `peers_filter_above_threshold`, `all_peers_known`, `attempts_7d_total`, `last_crawl`, `uptime_seconds`.

- [ ] **Step 6: Verify static peers landed and validation ran**

```bash
.venv/bin/python3 << 'EOF'
import sqlite3
db = sqlite3.connect("/home/polloloco/dgb-bloom-seeder/bloom_seeder.db")
db.row_factory = sqlite3.Row
print("== peers from config.static_peers ==")
for ip in ("129.212.182.152", "174.131.163.123", "149.76.66.207"):
    rows = db.execute(
        "SELECT ip, port, bloom_validated_at, filter_validated_at FROM peers WHERE ip=?",
        (ip,)
    ).fetchall()
    if rows:
        for r in rows:
            print(f"  {dict(r)}")
    else:
        print(f"  {ip}: not yet validated (may take 1+ crawl cycles)")
EOF
```

Expected: at least one of the three static peers shows up with `filter_validated_at` set (the others may be checked next cycle).

- [ ] **Step 7: Stop the seeder**

```bash
kill "$(cat /tmp/seeder_pid)" 2>/dev/null || true
sleep 2
ps -p "$(cat /tmp/seeder_pid)" 2>/dev/null && kill -9 "$(cat /tmp/seeder_pid)" || true
echo "seeder stopped"
```

- [ ] **Step 8: Final commit (only if smoke test exposed bugs requiring a fix)**

If the smoke test surfaced any issue, fix it and commit. Otherwise nothing to commit.

---

### Task 14: README — update `/peers` and `/stats` examples

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update the `/peers` description and example**

In `README.md`, find the section starting with `### \`GET /peers\`` and replace it with:

```markdown
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
```

- [ ] **Step 2: Update the `/stats` example**

Find the `### \`GET /stats\`` section and replace its example block with:

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

And update the description bullet list to:

```markdown
- `peers_bloom_validated` / `peers_filter_validated` — number of peers ever confirmed for that capability
- `peers_bloom_above_threshold` / `peers_filter_above_threshold` — number that would currently appear in `/peers?capability=...`
- `attempts_7d_total` — total crawl-attempt rows recorded in the rolling 7-day window
```

- [ ] **Step 3: Update the test count line**

Find `37 tests covering ...` and replace with:

```markdown
50+ tests covering P2P protocol encoding/decoding, SQLite storage, schema migration, per-capability ranking, crawler attempt logging, and HTTP API endpoints.
```

(After all tasks land, run `pytest tests/ | tail -1` to get the exact count and update if substantially off.)

- [ ] **Step 4: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add README.md
git commit -m "docs: README — capability query, /peers default fallthrough, /stats per-cap counts"
```

---

## Summary of changes shipped

- **Wire protocol:** `NODE_COMPACT_FILTERS = 0x40`, `build_getcfheaders` (BIP 157)
- **Schema:** `bloom_peers`/`bloom_peer_attempts` renamed to `peers`/`peer_attempts`; per-capability `validated_at` columns; `capability` column on attempts; one-shot migration from old schema
- **Storage:** all methods take a `capability` parameter where relevant; new `upsert_filter_peer`, `get_validated_peer_set(capability)`; ranking and stats are per-capability
- **Crawler:** parallel `getcfheaders` validator alongside `filterload`; per-capability snapshots feed both the priority-pool scheduler and the per-capability attempt-logging gate
- **Config:** new `static_peers` list, loaded into the queue at startup
- **API:** `/peers` accepts `?capability=...` with a default-fallthrough rule; response gains `services_hex`, `capabilities` (human-readable), `peer_capability`, and the response-level `capability` field; `/stats` gains per-capability validated and above-threshold counts
- **Tests:** new `tests/test_api.py` with TestClient coverage; protocol, storage, and crawler tests extended for filter/per-capability paths; migration test
- **Docs:** README updated for the new `/peers` and `/stats` shapes

## Deployment plan (for production rollout after merge)

Mirrors the previous feature's deploy:

1. Back up production DB: `cp bloom_seeder.db bloom_seeder.db.bak-PRE-CAPABILITY-$(date +%Y%m%d)`
2. `git pull --ff-only origin master`
3. `pytest tests/` on the server, confirm green
4. **nginx config update:** `/api/peers/bloom` → proxy to `:8025/peers?capability=bloom` (was `:8025/peers`). One-line change. `nginx -t && systemctl reload nginx`
5. `pm2 restart bloom-seeder`. Migration runs at startup. Initial crawl ~5 min before API comes up
6. Verify all `/peers` variants and `/stats` from production
7. Verify `https://api.digiscope.me/api/peers/bloom` (v3.5.38 wallet path) — should serve bloom peers as before
