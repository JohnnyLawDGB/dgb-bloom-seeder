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
