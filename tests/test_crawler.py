"""Tests for crawler attempt-logging behavior using a mocked handshake_peer."""

import time
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from seeder.config import Config
from seeder.crawler import crawl_cycle
from seeder.protocol import NODE_NETWORK, NODE_COMPACT_FILTERS
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


def filter_only_result(ip: str, port: int) -> dict:
    return {
        "ip": ip, "port": port, "protocol_version": 70019,
        "services": NODE_NETWORK | NODE_COMPACT_FILTERS,
        "user_agent": "/DigiByte:9.26.4/", "timestamp": 0, "start_height": 0,
        "relay": False, "discovered_peers": [], "filter_verified": True,
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
