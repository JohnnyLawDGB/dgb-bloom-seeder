# Uptime-Based Peer Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rank `/peers` results by a Bayesian-smoothed 7-day uptime score multiplied by a longevity bonus, hide peers below an inclusion threshold, and defend against fresh-IP Sybil attacks via prior burn-in.

**Architecture:** A new `bloom_peer_attempts` table records every crawl attempt against a known bloom peer (success or failure). Score is computed in SQL at API call time over the last 7 days. The crawler logs attempts; the API queries them. No changes to the wire protocol or to the `bloom_peers` schema itself.

**Tech Stack:** Python 3.10+, asyncio, aiosqlite, aiohttp, pytest, pytest-asyncio. Existing venv at `/home/polloloco/dgb-bloom-seeder/.venv` (activate before running tests).

**Spec:** `docs/superpowers/specs/2026-05-10-uptime-ranking-design.md`

**Working directory for all commands:** `/home/polloloco/dgb-bloom-seeder`

**File map:**
- Modify: `seeder/config.py` — six new ranking parameters in `Config` dataclass
- Modify: `config.yaml` — six new ranking keys
- Modify: `seeder/storage.py` — new schema, new methods, cascade-delete in `prune()`
- Modify: `seeder/crawler.py` — snapshot known bloom peers, log every attempt, prune attempts at cycle end
- Modify: `seeder/api.py` — `/peers` uses ranked query, `/stats` exposes new fields
- Modify: `tests/test_storage.py` — new tests for ranking, attempts, cascade prune
- Create: `tests/test_crawler.py` — test attempt-logging behavior with mocked handshake
- Delete (after migration): `Storage.get_bloom_peers` and its tests — superseded by `get_ranked_peers`

---

### Task 1: Config additions

**Files:**
- Modify: `seeder/config.py`
- Modify: `config.yaml`

- [ ] **Step 1: Add ranking fields to the Config dataclass**

In `seeder/config.py`, add these fields to the `@dataclass class Config` block, immediately after `api_max_age_hours: int = 6`:

```python
    # Ranking
    ranking_window_days: int = 7
    ranking_prior_attempts: int = 10
    ranking_prior_successes: int = 5
    ranking_longevity_cap_days: int = 60
    ranking_longevity_weight: float = 0.30
    ranking_inclusion_threshold: float = 0.50
```

- [ ] **Step 2: Add the same keys to config.yaml**

In `config.yaml`, append after the `api_max_age_hours: 6` line:

```yaml

# Ranking
ranking_window_days: 7
ranking_prior_attempts: 10
ranking_prior_successes: 5
ranking_longevity_cap_days: 60
ranking_longevity_weight: 0.30
ranking_inclusion_threshold: 0.50
```

- [ ] **Step 3: Verify config loads correctly**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -c "from seeder.config import load_config; c = load_config(); print(f'window={c.ranking_window_days} weight={c.ranking_longevity_weight} threshold={c.ranking_inclusion_threshold}')"`

Expected output: `window=7 weight=0.3 threshold=0.5`

- [ ] **Step 4: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/config.py config.yaml
git commit -m "feat: ranking config — window, Bayesian prior, longevity, threshold"
```

---

### Task 2: Schema — bloom_peer_attempts table

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write a test that the new table exists and accepts a row**

Append to `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_bloom_peer_attempts_table_exists(db):
    # Insert directly via the underlying connection to verify schema.
    await db._db.execute(
        "INSERT INTO bloom_peer_attempts (ip, port, ts, success) VALUES (?, ?, ?, ?)",
        ("1.2.3.4", 12024, 1700000000, 1),
    )
    await db._db.commit()
    cursor = await db._db.execute("SELECT COUNT(*) FROM bloom_peer_attempts")
    count = (await cursor.fetchone())[0]
    assert count == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_bloom_peer_attempts_table_exists -v`

Expected: FAIL with `sqlite3.OperationalError: no such table: bloom_peer_attempts`

- [ ] **Step 3: Add the table to Storage.init()**

In `seeder/storage.py`, find the `executescript` block in `Storage.init()` and add the new table definition. The block should now read:

```python
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS bloom_peers (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                services INTEGER NOT NULL,
                protocol_version INTEGER,
                user_agent TEXT,
                last_seen INTEGER NOT NULL,
                first_seen INTEGER NOT NULL,
                PRIMARY KEY (ip, port)
            );
            CREATE INDEX IF NOT EXISTS idx_bloom_last_seen ON bloom_peers(last_seen);

            CREATE TABLE IF NOT EXISTS all_peers (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                last_crawled INTEGER DEFAULT 0,
                PRIMARY KEY (ip, port)
            );

            CREATE TABLE IF NOT EXISTS bloom_peer_attempts (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                success INTEGER NOT NULL,
                PRIMARY KEY (ip, port, ts)
            );
            CREATE INDEX IF NOT EXISTS idx_attempts_ts
                ON bloom_peer_attempts(ts);
            CREATE INDEX IF NOT EXISTS idx_attempts_peer_ts
                ON bloom_peer_attempts(ip, port, ts);
        """)
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_bloom_peer_attempts_table_exists -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: schema — bloom_peer_attempts table with ts index"
```

---

### Task 3: `record_attempt` method

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write the test**

Append to `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_record_attempt_success_and_failure(db):
    await db.record_attempt("1.2.3.4", 12024, success=True, ts=1700000000)
    await db.record_attempt("1.2.3.4", 12024, success=False, ts=1700000001)
    cursor = await db._db.execute(
        "SELECT ts, success FROM bloom_peer_attempts WHERE ip=? AND port=? ORDER BY ts",
        ("1.2.3.4", 12024),
    )
    rows = await cursor.fetchall()
    assert [(r["ts"], r["success"]) for r in rows] == [
        (1700000000, 1),
        (1700000001, 0),
    ]
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_record_attempt_success_and_failure -v`

Expected: FAIL with `AttributeError: 'Storage' object has no attribute 'record_attempt'`

- [ ] **Step 3: Implement `record_attempt`**

In `seeder/storage.py`, add this method to the `Storage` class (place it just before `prune`):

```python
    async def record_attempt(self, ip: str, port: int, success: bool, ts: int):
        """Log a single crawl-attempt outcome against a known bloom peer."""
        await self._db.execute(
            """
            INSERT OR REPLACE INTO bloom_peer_attempts (ip, port, ts, success)
            VALUES (?, ?, ?, ?)
            """,
            (ip, port, ts, 1 if success else 0),
        )
        await self._db.commit()
```

(`INSERT OR REPLACE` handles the unlikely case of two attempts logged at the same `ts` for the same peer — overwrites cleanly rather than erroring.)

- [ ] **Step 4: Run to verify it passes**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_record_attempt_success_and_failure -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: storage.record_attempt — log per-cycle success/failure"
```

---

### Task 4: `prune_attempts` method

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write the test**

Append to `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_prune_attempts_drops_old_rows(db):
    now = int(time.time())
    old = now - 8 * 86400   # 8 days ago, outside 7d window
    new = now - 1 * 3600    # 1 hour ago, inside window
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=old)
    await db.record_attempt("2.2.2.2", 12024, success=True, ts=new)

    pruned = await db.prune_attempts(window_days=7)
    assert pruned == 1

    cursor = await db._db.execute("SELECT ip FROM bloom_peer_attempts")
    rows = await cursor.fetchall()
    assert [r["ip"] for r in rows] == ["2.2.2.2"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_prune_attempts_drops_old_rows -v`

Expected: FAIL with `AttributeError: ... 'prune_attempts'`

- [ ] **Step 3: Implement `prune_attempts`**

In `seeder/storage.py`, add the method to the `Storage` class (place it next to `record_attempt`):

```python
    async def prune_attempts(self, window_days: int) -> int:
        """Delete attempt rows older than the ranking window. Returns rows removed."""
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "DELETE FROM bloom_peer_attempts WHERE ts < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_prune_attempts_drops_old_rows -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: storage.prune_attempts — bound attempts table by window"
```

---

### Task 5: `get_known_bloom_peer_set` helper

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write the test**

Append to `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_get_known_bloom_peer_set(db):
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/b/", now)
    s = await db.get_known_bloom_peer_set()
    assert s == {("1.1.1.1", 12024), ("2.2.2.2", 12024)}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_get_known_bloom_peer_set -v`

Expected: FAIL with `AttributeError: ... 'get_known_bloom_peer_set'`

- [ ] **Step 3: Implement the helper**

In `seeder/storage.py`, add this method to the `Storage` class (place it after `get_bloom_peers`):

```python
    async def get_known_bloom_peer_set(self) -> set[tuple[str, int]]:
        """Return the current set of (ip, port) tuples in bloom_peers.
        Used by the crawler to decide which IPs should have attempts logged."""
        cursor = await self._db.execute("SELECT ip, port FROM bloom_peers")
        rows = await cursor.fetchall()
        return {(r["ip"], r["port"]) for r in rows}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_get_known_bloom_peer_set -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: storage.get_known_bloom_peer_set — crawler snapshot helper"
```

---

### Task 6: Cascade-delete attempts when pruning bloom peers

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write the test**

Append to `tests/test_storage.py`:

```python
@pytest.mark.asyncio
async def test_prune_cascades_to_attempts(db):
    now = int(time.time())
    old = now - 25 * 3600  # 25 hours, will be pruned by 24h prune
    fresh = now - 1 * 3600

    # Old peer: will be pruned. Has an attempt row.
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/old/", old)
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=old)

    # Fresh peer: will survive. Has an attempt row.
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/new/", fresh)
    await db.record_attempt("2.2.2.2", 12024, success=True, ts=fresh)

    pruned = await db.prune(max_age_hours=24)
    assert pruned == 1

    # Attempts for the pruned peer must also be gone.
    cursor = await db._db.execute("SELECT ip FROM bloom_peer_attempts ORDER BY ip")
    rows = await cursor.fetchall()
    assert [r["ip"] for r in rows] == ["2.2.2.2"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_prune_cascades_to_attempts -v`

Expected: FAIL — the pruned peer's attempts row still exists (`assert [...] == ["2.2.2.2"]` fails because there are 2 rows).

- [ ] **Step 3: Update `prune` to cascade-delete**

In `seeder/storage.py`, replace the existing `prune` method body. The method becomes:

```python
    async def prune(self, max_age_hours: int = 24) -> int:
        """Remove peers not seen within window. Also drops their attempt history."""
        cutoff = int(time.time()) - max_age_hours * 3600
        await self._db.execute(
            """
            DELETE FROM bloom_peer_attempts
            WHERE (ip, port) IN (
                SELECT ip, port FROM bloom_peers WHERE last_seen < ?
            )
            """,
            (cutoff,),
        )
        cursor = await self._db.execute(
            "DELETE FROM bloom_peers WHERE last_seen < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount
```

(The two DELETEs share a transaction; the attempts-DELETE must run *before* the bloom_peers-DELETE so the subquery still sees which peers are about to go.)

- [ ] **Step 4: Run to verify the new test passes and the existing prune test still passes**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_prune_cascades_to_attempts tests/test_storage.py::test_prune_old_peers -v`

Expected: both PASS

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: prune cascades to bloom_peer_attempts"
```

---

### Task 7: `get_ranked_peers` — main query

This is the central piece. We write multiple tests first, then implement.

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write the tests**

Append to `tests/test_storage.py`:

```python
# Default ranking parameters used across these tests — match config.yaml defaults.
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


@pytest.mark.asyncio
async def test_ranked_peer_with_one_success_is_included(db):
    """A brand-new peer with 1 success → smoothed (1+5)/(1+10) = 0.545 ≥ 0.50."""
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=now)

    peers = await db.get_ranked_peers(**RANK_DEFAULTS)
    assert len(peers) == 1
    assert peers[0]["ip"] == "1.1.1.1"
    assert peers[0]["attempts_7d"] == 1
    assert peers[0]["successes_7d"] == 1
    assert abs(peers[0]["uptime_score"] - 0.5454) < 0.01
    assert peers[0]["composite_score"] >= 0.5454  # tiny longevity bonus possible


@pytest.mark.asyncio
async def test_ranked_peer_below_threshold_excluded(db):
    """A peer with 1 success and 9 failures → smoothed (1+5)/(10+10) = 0.30 < 0.50."""
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=now - 60)
    for i in range(9):
        await db.record_attempt(
            "1.1.1.1", 12024, success=False, ts=now - 100 - i
        )

    peers = await db.get_ranked_peers(**RANK_DEFAULTS)
    assert peers == []


@pytest.mark.asyncio
async def test_ranked_higher_uptime_wins_over_longevity(db):
    """Reliability dominates: a 95% peer with 0 tenure beats a 60% peer with 60d tenure."""
    now = int(time.time())
    long_ago = now - 60 * 86400

    # Old, mediocre peer
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/old/", now)
    await db._db.execute(
        "UPDATE bloom_peers SET first_seen=? WHERE ip=?", (long_ago, "1.1.1.1")
    )
    await db._db.commit()
    for i in range(60):  # 60% success rate
        await db.record_attempt(
            "1.1.1.1", 12024, success=(i < 36), ts=now - 100 - i
        )

    # New, reliable peer
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/new/", now)
    for i in range(60):  # 95% success rate
        await db.record_attempt(
            "2.2.2.2", 12024, success=(i < 57), ts=now - 100 - i
        )

    peers = await db.get_ranked_peers(**RANK_DEFAULTS)
    ips = [p["ip"] for p in peers]
    assert ips == ["2.2.2.2", "1.1.1.1"]


@pytest.mark.asyncio
async def test_ranked_longevity_breaks_tie(db):
    """Equal uptime → longer tenure ranks first."""
    now = int(time.time())
    long_ago = now - 60 * 86400

    # Long-known peer
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/old/", now)
    await db._db.execute(
        "UPDATE bloom_peers SET first_seen=? WHERE ip=?", (long_ago, "1.1.1.1")
    )
    await db._db.commit()
    for i in range(50):
        await db.record_attempt("1.1.1.1", 12024, success=True, ts=now - 100 - i)

    # New peer, identical uptime
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/new/", now)
    for i in range(50):
        await db.record_attempt("2.2.2.2", 12024, success=True, ts=now - 100 - i)

    peers = await db.get_ranked_peers(**RANK_DEFAULTS)
    ips = [p["ip"] for p in peers]
    assert ips == ["1.1.1.1", "2.2.2.2"]
    # And the longer-tenure peer's score reflects the +30% longevity bonus.
    assert peers[0]["composite_score"] > peers[1]["composite_score"]


@pytest.mark.asyncio
async def test_ranked_respects_max_age_hours(db):
    """A peer not seen in last 6h should not appear, even with great history."""
    now = int(time.time())
    stale = now - 7 * 3600

    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/stale/", stale)
    for i in range(50):
        await db.record_attempt("1.1.1.1", 12024, success=True, ts=stale - i)

    peers = await db.get_ranked_peers(**RANK_DEFAULTS)
    assert peers == []


@pytest.mark.asyncio
async def test_ranked_respects_limit(db):
    now = int(time.time())
    for i in range(10):
        ip = f"10.0.0.{i}"
        await db.upsert_bloom_peer(ip, 12024, 0x05, 70019, "/x/", now)
        await db.record_attempt(ip, 12024, success=True, ts=now)
    args = {**RANK_DEFAULTS, "limit": 3}
    peers = await db.get_ranked_peers(**args)
    assert len(peers) == 3


@pytest.mark.asyncio
async def test_ranked_attempts_outside_window_ignored(db):
    """Attempts older than ranking_window_days do not count toward the score."""
    now = int(time.time())
    long_ago = now - 8 * 86400  # 8 days, outside 7-day window

    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    # 100 successes 8 days ago — these MUST be ignored.
    for i in range(100):
        await db.record_attempt(
            "1.1.1.1", 12024, success=True, ts=long_ago - i
        )
    # One success in window
    await db.record_attempt("1.1.1.1", 12024, success=True, ts=now)

    peers = await db.get_ranked_peers(**RANK_DEFAULTS)
    assert len(peers) == 1
    assert peers[0]["attempts_7d"] == 1   # only the in-window row
```

- [ ] **Step 2: Run to verify all tests fail**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -k "ranked" -v`

Expected: 7 FAIL (`AttributeError: ... 'get_ranked_peers'`)

- [ ] **Step 3: Implement `get_ranked_peers`**

In `seeder/storage.py`, add this method to the `Storage` class (place it after `get_bloom_peers`):

```python
    async def get_ranked_peers(
        self,
        *,
        window_days: int,
        prior_attempts: int,
        prior_successes: int,
        longevity_cap_days: int,
        longevity_weight: float,
        inclusion_threshold: float,
        max_age_hours: int,
        limit: int,
    ) -> list[dict]:
        """Return bloom peers above threshold, sorted by composite score DESC.

        composite_score = smoothed_uptime * (1 + longevity_weight * longevity_bonus)
        smoothed_uptime = (successes_7d + prior_successes) / (attempts_7d + prior_attempts)
        longevity_bonus = min(tenure_days / longevity_cap_days, 1.0)
        """
        now = int(time.time())
        window_cutoff = now - window_days * 86400
        last_seen_cutoff = now - max_age_hours * 3600

        cursor = await self._db.execute(
            """
            WITH stats AS (
                SELECT bp.ip, bp.port, bp.services,
                       bp.last_seen, bp.first_seen,
                       bp.protocol_version, bp.user_agent,
                       COALESCE(SUM(a.success), 0)   AS successes_7d,
                       COALESCE(COUNT(a.success), 0) AS attempts_7d
                FROM bloom_peers bp
                LEFT JOIN bloom_peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                GROUP BY bp.ip, bp.port
            ),
            scored AS (
                SELECT *,
                       (successes_7d + ?) * 1.0 / (attempts_7d + ?) AS uptime_score,
                       MIN((? - first_seen) / 86400.0 / ?, 1.0)     AS longevity_bonus
                FROM stats
            )
            SELECT *,
                   uptime_score * (1 + ? * longevity_bonus) AS composite_score,
                   (? - first_seen) / 86400.0              AS tenure_days
            FROM scored
            WHERE uptime_score >= ?
            ORDER BY composite_score DESC, last_seen DESC
            LIMIT ?
            """,
            (
                window_cutoff,
                last_seen_cutoff,
                prior_successes,
                prior_attempts,
                now,
                longevity_cap_days,
                longevity_weight,
                now,
                inclusion_threshold,
                limit,
            ),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
```

- [ ] **Step 4: Run to verify all ranking tests pass**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -v`

Expected: all storage tests PASS (existing + 7 new ranking tests + table/record_attempt/prune tests).

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: storage.get_ranked_peers — composite uptime × longevity ranking"
```

---

### Task 8: Stats helpers — `get_above_threshold_count` and `get_attempts_total`

**Files:**
- Modify: `seeder/storage.py`
- Modify: `tests/test_storage.py`

- [ ] **Step 1: Write the tests**

Append to `tests/test_storage.py`:

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
    assert total == 2  # only in-window rows


@pytest.mark.asyncio
async def test_get_above_threshold_count(db):
    """Returns number of peers that would appear in /peers (above threshold)."""
    now = int(time.time())

    # Peer A — 50 successes, will pass threshold easily
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    for i in range(50):
        await db.record_attempt("1.1.1.1", 12024, success=True, ts=now - i)

    # Peer B — 1 success / 9 failures, will be below threshold
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/b/", now)
    await db.record_attempt("2.2.2.2", 12024, success=True, ts=now)
    for i in range(9):
        await db.record_attempt("2.2.2.2", 12024, success=False, ts=now - 1 - i)

    count = await db.get_above_threshold_count(
        threshold=0.50,
        prior_attempts=10,
        prior_successes=5,
        window_days=7,
        max_age_hours=6,
    )
    assert count == 1
```

- [ ] **Step 2: Run to verify they fail**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -k "attempts_total or above_threshold" -v`

Expected: FAIL (`AttributeError`)

- [ ] **Step 3: Implement both helpers**

In `seeder/storage.py`, add these methods to the `Storage` class (place them after `get_ranked_peers`):

```python
    async def get_attempts_total(self, window_days: int) -> int:
        """Count of attempt rows within the ranking window. Used by /stats."""
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM bloom_peer_attempts WHERE ts >= ?", (cutoff,)
        )
        return (await cursor.fetchone())[0]

    async def get_above_threshold_count(
        self,
        *,
        threshold: float,
        prior_attempts: int,
        prior_successes: int,
        window_days: int,
        max_age_hours: int,
    ) -> int:
        """How many peers would appear in /peers given the current threshold."""
        now = int(time.time())
        window_cutoff = now - window_days * 86400
        last_seen_cutoff = now - max_age_hours * 3600

        cursor = await self._db.execute(
            """
            WITH stats AS (
                SELECT bp.ip, bp.port,
                       COALESCE(SUM(a.success), 0)   AS successes_7d,
                       COALESCE(COUNT(a.success), 0) AS attempts_7d
                FROM bloom_peers bp
                LEFT JOIN bloom_peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                GROUP BY bp.ip, bp.port
            )
            SELECT COUNT(*) FROM stats
            WHERE (successes_7d + ?) * 1.0 / (attempts_7d + ?) >= ?
            """,
            (window_cutoff, last_seen_cutoff, prior_successes, prior_attempts, threshold),
        )
        return (await cursor.fetchone())[0]
```

- [ ] **Step 4: Run to verify they pass**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py -k "attempts_total or above_threshold" -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: storage stats helpers — attempts_total, above_threshold_count"
```

---

### Task 9: Crawler logs attempts during crawl_cycle

**Files:**
- Modify: `seeder/crawler.py`
- Create: `tests/test_crawler.py`

- [ ] **Step 1: Write the test for attempt-logging behavior**

Create `tests/test_crawler.py`:

```python
"""Tests for crawler attempt-logging behavior using a mocked handshake_peer."""

import time
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from seeder.config import Config
from seeder.crawler import crawl_cycle
from seeder.protocol import NODE_BLOOM, NODE_NETWORK
from seeder.storage import Storage


@pytest_asyncio.fixture
async def db():
    store = Storage(":memory:")
    await store.init()
    yield store
    await store.close()


def make_config() -> Config:
    cfg = Config()
    cfg.crawl_max_peers = 50
    cfg.crawl_concurrency = 1
    cfg.dns_seeds = []  # don't hit the real network in tests
    return cfg


def verified_result(ip: str, port: int) -> dict:
    return {
        "ip": ip,
        "port": port,
        "protocol_version": 70019,
        "services": NODE_NETWORK | NODE_BLOOM,
        "user_agent": "/DigiByte:8.26.0/",
        "timestamp": 0,
        "start_height": 0,
        "relay": False,
        "discovered_peers": [],
        "bloom_verified": True,
    }


@pytest.mark.asyncio
async def test_crawl_logs_success_for_newly_verified_peer(db):
    cfg = make_config()
    await db.add_crawl_peers([("1.1.1.1", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return verified_result(ip, port)

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT ip, success FROM bloom_peer_attempts ORDER BY ip"
    )
    rows = await cursor.fetchall()
    assert [(r["ip"], r["success"]) for r in rows] == [("1.1.1.1", 1)]


@pytest.mark.asyncio
async def test_crawl_logs_failure_for_known_peer_that_drops(db):
    """Peer was in bloom_peers, but this cycle handshake_peer returns None."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now - 3600)
    await db.add_crawl_peers([("1.1.1.1", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None  # connection failed

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT ip, success FROM bloom_peer_attempts WHERE ip='1.1.1.1'"
    )
    rows = await cursor.fetchall()
    assert [(r["ip"], r["success"]) for r in rows] == [("1.1.1.1", 0)]


@pytest.mark.asyncio
async def test_crawl_does_not_log_unknown_unverified_peer(db):
    """An IP in the queue but not in bloom_peers, that fails to verify, is NOT logged."""
    cfg = make_config()
    await db.add_crawl_peers([("9.9.9.9", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute("SELECT COUNT(*) FROM bloom_peer_attempts")
    count = (await cursor.fetchone())[0]
    assert count == 0


@pytest.mark.asyncio
async def test_crawl_logs_failure_when_known_peer_advertises_bloom_but_unverified(db):
    """Peer is known bloom; this cycle returns version with NODE_BLOOM but bloom_verified=False
    (i.e. peer disconnected during filterload). That counts as a failure."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now - 3600)
    await db.add_crawl_peers([("1.1.1.1", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        result = verified_result(ip, port)
        result["bloom_verified"] = False
        return result

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT success FROM bloom_peer_attempts WHERE ip='1.1.1.1'"
    )
    rows = await cursor.fetchall()
    assert [r["success"] for r in rows] == [0]
```

- [ ] **Step 2: Run to verify the tests fail**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_crawler.py -v`

Expected: FAIL — current `crawl_cycle` does not log attempts (`bloom_peer_attempts` rows do not appear).

- [ ] **Step 3: Modify `crawl_cycle` to snapshot bloom peers and log attempts**

In `seeder/crawler.py`, replace the `crawl_cycle` function. The new version snapshots bloom peers up-front and logs an attempt row for any IP that's in the snapshot **or** that just verified:

```python
async def crawl_cycle(config: Config, storage: Storage) -> dict:
    """Run one crawl cycle. Returns stats dict."""
    log.info("Starting crawl cycle")
    start = time.time()

    # Seed from DNS if we have few peers
    peers = await storage.get_uncrawled_peers(limit=config.crawl_max_peers)
    if len(peers) < 50:
        dns_peers = await resolve_seeds(config.dns_seeds, config.dgb_port)
        await storage.add_crawl_peers(dns_peers)
        peers = await storage.get_uncrawled_peers(limit=config.crawl_max_peers)

    # Snapshot the current set of known bloom peers — used to decide whether
    # to log an attempt row when this peer's handshake fails.
    known_bloom = await storage.get_known_bloom_peer_set()

    bloom_found = 0
    total_checked = 0
    new_peers_discovered = 0
    sem = asyncio.Semaphore(config.crawl_concurrency)

    async def check_peer(ip: str, port: int):
        nonlocal bloom_found, total_checked, new_peers_discovered
        async with sem:
            await storage.mark_crawled(ip, port)
            result = await handshake_peer(ip, port, config.dgb_magic, config.crawl_timeout)
            total_checked += 1

            ts = int(time.time())
            verified = bool(result and result.get("bloom_verified"))
            was_known = (ip, port) in known_bloom

            # Log an attempt for any IP we already know is a bloom peer,
            # OR for any IP that just verified as bloom for the first time.
            if was_known or verified:
                await storage.record_attempt(ip, port, success=verified, ts=ts)

            if result is None:
                return

            if verified:
                bloom_found += 1
                await storage.upsert_bloom_peer(
                    ip, port, result["services"],
                    result["protocol_version"],
                    result["user_agent"],
                    ts,
                )
                log.info("BLOOM VERIFIED: %s:%d %s (services=0x%02x)",
                         ip, port, result["user_agent"], result["services"])
            elif result["services"] & NODE_BLOOM:
                log.debug("BLOOM FAKE: %s:%d advertises NODE_BLOOM but rejected filterload",
                          ip, port)

            # Add discovered peers to crawl queue
            discovered = result.get("discovered_peers", [])
            if discovered:
                new_peers_discovered += len(discovered)
                await storage.add_crawl_peers(
                    [(p["ip"], p["port"]) for p in discovered]
                )

    tasks = [check_peer(ip, port) for ip, port in peers]
    await asyncio.gather(*tasks)

    # Prune old entries
    pruned = await storage.prune(max_age_hours=config.prune_hours)
    pruned_attempts = await storage.prune_attempts(window_days=config.ranking_window_days)

    elapsed = time.time() - start
    stats = {
        "checked": total_checked,
        "bloom_found": bloom_found,
        "new_peers": new_peers_discovered,
        "pruned": pruned,
        "pruned_attempts": pruned_attempts,
        "elapsed_seconds": round(elapsed, 1),
    }
    log.info("Crawl complete: %s", stats)
    return stats
```

(Note: this also folds in **Task 10's** prune-attempts-at-end-of-cycle change. They're inseparable — both edits live in the same function.)

- [ ] **Step 4: Run all tests — both new crawler tests and existing storage/protocol tests**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: every test PASSES (existing protocol tests, all storage tests including new ranking tests, and all 4 new crawler tests).

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/crawler.py tests/test_crawler.py
git commit -m "feat: crawler logs per-attempt outcomes; prunes attempts each cycle"
```

---

### Task 10: API — switch `/peers` to ranked query

**Files:**
- Modify: `seeder/api.py`
- Modify: `seeder/storage.py` (delete `get_bloom_peers`)
- Modify: `tests/test_storage.py` (delete tests for `get_bloom_peers`)

- [ ] **Step 1: Update `/peers` handler to use `get_ranked_peers`**

In `seeder/api.py`, replace the `handle_peers` inner function (inside `create_app`):

```python
    async def handle_peers(request: web.Request) -> web.Response:
        peers = await storage.get_ranked_peers(
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

(`get_ranked_peers` returns dicts with `uptime_score`, `composite_score`, `attempts_7d`, `successes_7d`, `tenure_days`, `first_seen` already — `web.json_response` serializes them directly.)

- [ ] **Step 2: Delete the now-unused `get_bloom_peers` method**

In `seeder/storage.py`, delete the entire `get_bloom_peers` method:

```python
    async def get_bloom_peers(self, max_age_hours: int = 6, limit: int = 25) -> list[dict]:
        cutoff = int(time.time()) - max_age_hours * 3600
        cursor = await self._db.execute("""
            SELECT ip, port, services, protocol_version, user_agent, last_seen
            FROM bloom_peers
            WHERE last_seen >= ?
            ORDER BY last_seen DESC
            LIMIT ?
        """, (cutoff, limit))
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
```

- [ ] **Step 3: Delete the four tests that exercised it**

In `tests/test_storage.py`, delete these four test functions in their entirety:

- `test_upsert_bloom_peer`
- `test_upsert_updates_last_seen`
- `test_get_bloom_peers_respects_max_age`
- `test_get_bloom_peers_limit`

(Their behaviors — upsert correctness, max-age filtering, limit handling — are now exercised through `get_ranked_peers` tests in Task 7.)

- [ ] **Step 4: Run the full test suite**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: all tests PASS, no references to `get_bloom_peers` remain.

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/api.py seeder/storage.py tests/test_storage.py
git commit -m "feat: /peers serves ranked, threshold-filtered peers"
```

---

### Task 11: API — `/stats` exposes new fields

**Files:**
- Modify: `seeder/api.py`
- Modify: `seeder/storage.py` (extend `get_stats`)
- Modify: `tests/test_storage.py` (update existing stats test)

- [ ] **Step 1: Update the existing `test_get_stats` test to cover new fields**

In `tests/test_storage.py`, find the existing `test_get_stats` and replace it with:

```python
@pytest.mark.asyncio
async def test_get_stats(db):
    now = int(time.time())
    # Peer A — will be above threshold (lots of successes)
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    for i in range(50):
        await db.record_attempt("1.1.1.1", 12024, success=True, ts=now - i)
    # Peer B — exists but no recent attempts → score = prior = 0.50, exactly at threshold (included)
    await db.upsert_bloom_peer("3.3.3.3", 12024, 0x05, 70019, "/c/", now)
    await db.add_crawl_peers([("1.1.1.1", 12024), ("2.2.2.2", 12024)])

    stats = await db.get_stats(
        max_age_hours=6,
        threshold=0.50,
        prior_attempts=10,
        prior_successes=5,
        window_days=7,
    )
    assert stats["bloom_peers_total"] == 2
    assert stats["all_peers_known"] == 2
    assert stats["bloom_peers_above_threshold"] == 2  # both included
    assert stats["attempts_7d_total"] == 50
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/test_storage.py::test_get_stats -v`

Expected: FAIL — `get_stats` does not yet accept the new keyword arguments.

- [ ] **Step 3: Extend `Storage.get_stats` to compute the new fields**

In `seeder/storage.py`, replace the existing `get_stats` method. The method now requires the threshold-related parameters because they determine `bloom_peers_above_threshold`:

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
        cutoff = int(time.time()) - max_age_hours * 3600

        cursor = await self._db.execute("SELECT COUNT(*) FROM bloom_peers")
        total = (await cursor.fetchone())[0]

        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM bloom_peers WHERE last_seen >= ?", (cutoff,)
        )
        recent = (await cursor.fetchone())[0]

        cursor = await self._db.execute("SELECT COUNT(*) FROM all_peers")
        all_known = (await cursor.fetchone())[0]

        above_threshold = await self.get_above_threshold_count(
            threshold=threshold,
            prior_attempts=prior_attempts,
            prior_successes=prior_successes,
            window_days=window_days,
            max_age_hours=max_age_hours,
        )

        attempts_total = await self.get_attempts_total(window_days=window_days)

        return {
            "bloom_peers_total": total,
            "bloom_peers_recent": recent,
            "bloom_peers_above_threshold": above_threshold,
            "all_peers_known": all_known,
            "attempts_7d_total": attempts_total,
        }
```

- [ ] **Step 4: Update the `/stats` API handler to pass the new parameters**

In `seeder/api.py`, replace the `handle_stats` inner function (inside `create_app`):

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

- [ ] **Step 5: Run the full test suite**

Run: `cd /home/polloloco/dgb-bloom-seeder && .venv/bin/python3 -m pytest tests/ -v`

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/api.py seeder/storage.py tests/test_storage.py
git commit -m "feat: /stats reports above_threshold count and 7d attempts total"
```

---

### Task 12: Live smoke test

This validates the end-to-end behavior against the real DigiByte network. It requires outbound TCP to port 12024 and is expected to be run interactively, not in CI.

**Files:** none (validation only)

- [ ] **Step 1: Delete any local DB to start fresh**

Run: `cd /home/polloloco/dgb-bloom-seeder && rm -f bloom_seeder.db`

- [ ] **Step 2: Run the seeder for ~3 minutes**

Run: `cd /home/polloloco/dgb-bloom-seeder && timeout 180 .venv/bin/python3 seeder.py 2>&1 | tee /tmp/seeder_smoke.log`

Expected log output:
- `Resolved N peers from M DNS seeds`
- One or more `BLOOM VERIFIED: ...` lines
- `Crawl complete: {'checked': ..., 'bloom_found': ..., 'pruned_attempts': 0, ...}`
- `API listening on http://0.0.0.0:8025`

- [ ] **Step 3: Hit `/peers` and confirm the new fields are present and ordering is by composite score**

While the seeder is still running (or run again briefly), in another shell:

Run: `curl -s http://localhost:8025/peers | .venv/bin/python3 -m json.tool`

Expected: a JSON object whose `peers` array contains objects with all of: `ip`, `port`, `last_seen`, `first_seen`, `protocol_version`, `user_agent`, `services`, `uptime_score`, `composite_score`, `attempts_7d`, `successes_7d`, `tenure_days`. The `composite_score` of the first element is ≥ the second, and so on.

- [ ] **Step 4: Hit `/stats` and confirm the new fields**

Run: `curl -s http://localhost:8025/stats | .venv/bin/python3 -m json.tool`

Expected: a JSON object including `bloom_peers_above_threshold` and `attempts_7d_total` (in addition to the existing `bloom_peers_total`, `bloom_peers_recent`, `all_peers_known`, `last_crawl`, `uptime_seconds`).

- [ ] **Step 5: Spot-check a peer's score math**

Pick the top peer from `/peers` and verify by hand:
- `uptime_score` ≈ `(successes_7d + 5) / (attempts_7d + 10)`
- `tenure_days` ≈ `(now - first_seen) / 86400`
- `composite_score` ≈ `uptime_score * (1 + 0.30 * min(tenure_days / 60, 1.0))`

If any of these diverge significantly, stop and investigate before committing.

- [ ] **Step 6: Stop the seeder and commit any final tweaks**

If the smoke test surfaced anything to fix, fix it and add a final commit. Otherwise nothing to commit at this step.

---

## Summary of changes shipped

- New table `bloom_peer_attempts` capturing every crawl outcome against a known bloom peer.
- Bayesian-smoothed 7-day uptime × longevity ranking; peers below 0.50 hidden.
- Six new tunable config keys, all exposed in `config.yaml`.
- `/peers` returns ranked list with five new diagnostic fields.
- `/stats` exposes `bloom_peers_above_threshold` and `attempts_7d_total`.
- Cascade-delete: pruning a stale peer removes its attempt history.
- 13+ new unit tests; one new test file (`tests/test_crawler.py`).
- Old `Storage.get_bloom_peers` removed in favor of `get_ranked_peers`.
