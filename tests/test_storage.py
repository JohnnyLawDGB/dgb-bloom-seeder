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
async def test_upsert_bloom_peer(db):
    now = int(time.time())
    await db.upsert_bloom_peer("1.2.3.4", 12024, 0x05, 70019, "/DigiByte:8.26.0/", now)
    peers = await db.get_bloom_peers(max_age_hours=1)
    assert len(peers) == 1
    assert peers[0]["ip"] == "1.2.3.4"
    assert peers[0]["port"] == 12024
    assert peers[0]["services"] == 0x05
    assert peers[0]["user_agent"] == "/DigiByte:8.26.0/"


@pytest.mark.asyncio
async def test_upsert_updates_last_seen(db):
    t1 = int(time.time()) - 100
    t2 = int(time.time())
    await db.upsert_bloom_peer("1.2.3.4", 12024, 0x05, 70019, "/v1/", t1)
    await db.upsert_bloom_peer("1.2.3.4", 12024, 0x05, 70019, "/v2/", t2)
    peers = await db.get_bloom_peers(max_age_hours=1)
    assert len(peers) == 1
    assert peers[0]["last_seen"] == t2
    assert peers[0]["user_agent"] == "/v2/"


@pytest.mark.asyncio
async def test_get_bloom_peers_respects_max_age(db):
    old = int(time.time()) - 7 * 3600  # 7 hours ago
    new = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/old/", old)
    await db.upsert_bloom_peer("2.2.2.2", 12024, 0x05, 70019, "/new/", new)
    peers = await db.get_bloom_peers(max_age_hours=6)
    assert len(peers) == 1
    assert peers[0]["ip"] == "2.2.2.2"


@pytest.mark.asyncio
async def test_get_bloom_peers_limit(db):
    now = int(time.time())
    for i in range(10):
        await db.upsert_bloom_peer(f"1.1.1.{i}", 12024, 0x05, 70019, "/test/", now)
    peers = await db.get_bloom_peers(max_age_hours=1, limit=5)
    assert len(peers) == 5


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
    peers = await db.get_bloom_peers(max_age_hours=48)
    assert len(peers) == 1
    assert peers[0]["ip"] == "2.2.2.2"


@pytest.mark.asyncio
async def test_get_stats(db):
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/test/", now)
    await db.add_crawl_peers([("1.1.1.1", 12024), ("2.2.2.2", 12024)])
    stats = await db.get_stats(max_age_hours=6)
    assert stats["bloom_peers_total"] == 1
    assert stats["all_peers_known"] == 2


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
