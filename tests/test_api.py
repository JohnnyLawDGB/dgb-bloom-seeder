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
    """No peers at all → empty response, capability='filter' or 'bloom' (default attempted)."""
    resp = await client.get("/peers")
    data = await resp.json()
    assert data["count"] == 0
    assert data["peers"] == []
    # Even with no peers, the response should declare which list it tried.
    assert data["capability"] in ("filter", "bloom")


@pytest.mark.asyncio
async def test_peers_response_includes_services_hex_and_capabilities(client, db):
    now = int(time.time())
    # 0x44d = NETWORK | BLOOM | WITNESS | COMPACT_FILTERS | NETWORK_LIMITED
    await db._db.execute("""
        INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                           last_seen, first_seen, bloom_validated_at, filter_validated_at)
        VALUES ('2.2.2.2', 12024, 0x44d, 70019, '/f/', ?, ?, ?, ?)
    """, (now, now, now, now))
    await db._db.commit()
    await db.record_attempt("2.2.2.2", 12024, capability="filter", success=True, ts=now)

    resp = await client.get("/peers?capability=filter")
    data = await resp.json()
    peer = data["peers"][0]
    assert peer["services_hex"] == "0x44d"
    assert "BLOOM" in peer["capabilities"]
    assert "COMPACT_FILTERS" in peer["capabilities"]
    assert "NETWORK" in peer["capabilities"]
    assert "WITNESS" in peer["capabilities"]
    assert "NETWORK_LIMITED" in peer["capabilities"]
