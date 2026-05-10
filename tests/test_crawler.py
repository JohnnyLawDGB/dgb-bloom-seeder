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
        "SELECT ip, success FROM peer_attempts WHERE capability='bloom' ORDER BY ip"
    )
    rows = await cursor.fetchall()
    assert [(r["ip"], r["success"]) for r in rows] == [("1.1.1.1", 1)]


@pytest.mark.asyncio
async def test_crawl_logs_failure_for_known_peer_that_drops(db):
    """Peer was in peers, but this cycle handshake_peer returns None."""
    cfg = make_config()
    now = int(time.time())
    await db.upsert_bloom_peer("1.1.1.1", 12024, 0x05, 70019, "/a/", now - 3600)
    await db.add_crawl_peers([("1.1.1.1", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None  # connection failed

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute(
        "SELECT ip, success FROM peer_attempts WHERE capability='bloom' AND ip='1.1.1.1'"
    )
    rows = await cursor.fetchall()
    assert [(r["ip"], r["success"]) for r in rows] == [("1.1.1.1", 0)]


@pytest.mark.asyncio
async def test_crawl_does_not_log_unknown_unverified_peer(db):
    """An IP in the queue but not in peers, that fails to verify, is NOT logged."""
    cfg = make_config()
    await db.add_crawl_peers([("9.9.9.9", 12024)])

    async def fake_handshake(ip, port, magic, timeout):
        return None

    with patch("seeder.crawler.handshake_peer", new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    cursor = await db._db.execute("SELECT COUNT(*) FROM peer_attempts WHERE capability='bloom'")
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
        "SELECT success FROM peer_attempts WHERE capability='bloom' AND ip='1.1.1.1'"
    )
    rows = await cursor.fetchall()
    assert [r["success"] for r in rows] == [0]


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
    assert [(r["capability"], r["success"]) for r in rows] == [("filter", 1)]


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


@pytest.mark.asyncio
async def test_crawl_prioritizes_static_peers_even_when_recently_crawled(db):
    """Static peers are crawled every cycle regardless of last_crawled,
    so operator-declared peers don't have to wait for queue rotation."""
    cfg = make_config()
    cfg.static_peers = [
        {"ip": "7.7.7.7", "port": 12024, "source": "test"},
    ]

    # Pre-populate all_peers: the static peer has a very recent last_crawled
    # (would normally be excluded by get_uncrawled_peers's cutoff).
    now = int(time.time())
    await db.add_crawl_peers([("7.7.7.7", 12024)])
    await db._db.execute(
        "UPDATE all_peers SET last_crawled=? WHERE ip='7.7.7.7' AND port=12024",
        (now,),
    )
    await db._db.commit()

    crawled_ips: list[str] = []

    async def fake_handshake(ip, port, magic, timeout):
        crawled_ips.append(ip)
        return None

    with patch("seeder.crawler.handshake_peer",
               new=AsyncMock(side_effect=fake_handshake)):
        await crawl_cycle(cfg, db)

    # 7.7.7.7 should be crawled even though its last_crawled was just now.
    assert "7.7.7.7" in crawled_ips
