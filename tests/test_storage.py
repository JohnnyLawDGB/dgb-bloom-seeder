# tests/test_storage.py
import asyncio
import time
import pytest
import pytest_asyncio
from seeder.storage import Storage

@pytest_asyncio.fixture
async def db():
    store = Storage(":memory:")
    await store.init()
    yield store
    await store.close()






@pytest.mark.asyncio
async def test_add_and_get_crawl_peers(db):
    await db.add_crawl_peers([("1.2.3.4", 12024), ("5.6.7.8", 12024)])
    peers = await db.get_uncrawled_peers(limit=10)
    assert len(peers) == 2


@pytest.mark.asyncio
async def test_mark_crawled(db):
    await db.add_crawl_peers([("1.2.3.4", 12024)])
    await db.mark_crawled("1.2.3.4", 12024)
    peers = await db.get_uncrawled_peers(limit=10)
    assert len(peers) == 0


@pytest.mark.asyncio
async def test_prune_old_peers(db):
    old = int(time.time()) - 25 * 3600  # 25 hours ago
    new = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/old/", old)
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/new/", new)
    pruned = await db.prune(max_age_hours=24)
    assert pruned == 1
    # Verify the remaining peer is the new one
    s = await db.get_known_bloom_peer_set()
    assert s == {("2.2.2.2", 12024)}


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


@pytest.mark.asyncio
async def test_get_known_bloom_peer_set(db):
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now)
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/b/", now)
    s = await db.get_known_bloom_peer_set()
    assert s == {("1.1.1.1", 12024), ("2.2.2.2", 12024)}


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
