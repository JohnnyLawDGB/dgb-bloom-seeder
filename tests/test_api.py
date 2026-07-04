"""HTTP-layer tests for the seeder API using aiohttp's TestClient."""

import time
import pytest
import pytest_asyncio
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
