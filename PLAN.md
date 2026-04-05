# DGB Bloom Seeder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone Python service that crawls the DigiByte P2P network for bloom-capable nodes and serves them via a JSON API.

**Architecture:** Single async Python process — crawler loop discovers peers via P2P handshake, checks `NODE_BLOOM` service flag, stores results in SQLite, serves via built-in aiohttp API. No DigiByte Core RPC dependency.

**Tech Stack:** Python 3.10+, asyncio, aiohttp, aiosqlite, pyyaml

---

### Task 1: Project scaffolding and config

**Files:**
- Create: `requirements.txt`
- Create: `config.yaml`
- Create: `seeder/__init__.py`
- Create: `seeder/config.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create requirements.txt**

```
aiohttp>=3.9
aiosqlite>=0.19
pyyaml>=6.0
pytest>=8.0
pytest-asyncio>=0.23
```

- [ ] **Step 2: Create config.yaml**

```yaml
# Network
dgb_port: 12024
dgb_magic: "fac3b6da"

# Crawler
crawl_interval: 1800
crawl_concurrency: 10
crawl_timeout: 5
crawl_max_peers: 500
prune_hours: 24

# DNS Seeds
dns_seeds:
  - seed.digibyte.io
  - seed2.digibyte.io
  - seed.digibyteprojects.com
  - digibyteblockexplorer.com
  - dgbseed.org

# API
api_port: 8025
api_host: "0.0.0.0"
api_max_results: 25
api_max_age_hours: 6

# Database
db_path: "bloom_seeder.db"

# Logging
log_level: "INFO"
```

- [ ] **Step 3: Create config loader**

```python
# seeder/config.py
import yaml
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Config:
    dgb_port: int = 12024
    dgb_magic: bytes = b"\xfa\xc3\xb6\xda"

    crawl_interval: int = 1800
    crawl_concurrency: int = 10
    crawl_timeout: int = 5
    crawl_max_peers: int = 500
    prune_hours: int = 24

    dns_seeds: list[str] = field(default_factory=lambda: [
        "seed.digibyte.io",
        "seed2.digibyte.io",
        "seed.digibyteprojects.com",
        "digibyteblockexplorer.com",
        "dgbseed.org",
    ])

    api_port: int = 8025
    api_host: str = "0.0.0.0"
    api_max_results: int = 25
    api_max_age_hours: int = 6

    db_path: str = "bloom_seeder.db"
    log_level: str = "INFO"


def load_config(path: str = "config.yaml") -> Config:
    p = Path(path)
    if not p.exists():
        return Config()
    with open(p) as f:
        data = yaml.safe_load(f) or {}
    cfg = Config()
    for key, val in data.items():
        if key == "dgb_magic":
            cfg.dgb_magic = bytes.fromhex(val)
        elif hasattr(cfg, key):
            setattr(cfg, key, val)
    return cfg
```

- [ ] **Step 4: Create package init files**

`seeder/__init__.py` — empty file.
`tests/__init__.py` — empty file.

- [ ] **Step 5: Install deps and verify**

Run: `cd /home/polloloco/dgb-bloom-seeder && pip install -r requirements.txt 2>&1 | tail -5`
Expected: Successfully installed

Run: `python3 -c "from seeder.config import load_config; c = load_config(); print(f'port={c.api_port} magic={c.dgb_magic.hex()}')"`
Expected: `port=8025 magic=fac3b6da`

- [ ] **Step 6: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add -A
git commit -m "feat: project scaffolding — config, requirements, package structure"
```

---

### Task 2: P2P protocol module

**Files:**
- Create: `seeder/protocol.py`
- Create: `tests/test_protocol.py`

- [ ] **Step 1: Write protocol tests**

```python
# tests/test_protocol.py
import struct
from seeder.protocol import (
    DGB_MAGIC, NODE_BLOOM, NODE_NETWORK, NODE_WITNESS,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr,
    parse_addr_payload, encode_varint, decode_varint,
    net_addr,
)


def test_encode_varint_small():
    assert encode_varint(0) == b"\x00"
    assert encode_varint(252) == b"\xfc"


def test_encode_varint_16bit():
    assert encode_varint(253) == b"\xfd\xfd\x00"
    assert encode_varint(0xFFFF) == b"\xfd\xff\xff"


def test_encode_varint_32bit():
    assert encode_varint(0x10000) == b"\xfe\x00\x00\x01\x00"


def test_decode_varint_small():
    val, size = decode_varint(b"\x42rest")
    assert val == 0x42
    assert size == 1


def test_decode_varint_16bit():
    val, size = decode_varint(b"\xfd\x01\x01")
    assert val == 0x0101
    assert size == 3


def test_make_message_checksum():
    msg = make_message(DGB_MAGIC, "verack", b"")
    # verack has empty payload → checksum of SHA256(SHA256(""))
    assert len(msg) == 24  # 4 magic + 12 cmd + 4 len + 4 checksum
    assert msg[:4] == DGB_MAGIC
    assert msg[4:16] == b"verack\x00\x00\x00\x00\x00\x00"
    assert struct.unpack_from("<I", msg, 16)[0] == 0  # payload len = 0


def test_parse_message_header():
    msg = make_message(DGB_MAGIC, "verack", b"")
    cmd, payload_len, checksum = parse_message_header(msg[:24])
    assert cmd == "verack"
    assert payload_len == 0


def test_build_version_payload():
    payload = build_version_payload(
        protocol_version=70019,
        services=0,
        timestamp=1700000000,
        user_agent="/test:1.0/",
        start_height=0,
        relay=False,
    )
    # Parse back: protocol version at offset 0
    version = struct.unpack_from("<i", payload, 0)[0]
    assert version == 70019
    # Services at offset 4
    services = struct.unpack_from("<Q", payload, 4)[0]
    assert services == 0


def test_parse_version_payload():
    payload = build_version_payload(
        protocol_version=70019,
        services=0x0D,  # NODE_NETWORK | NODE_BLOOM | NODE_WITNESS
        timestamp=1700000000,
        user_agent="/DigiByte:8.26.0/",
        start_height=23000000,
        relay=True,
    )
    info = parse_version_payload(payload)
    assert info["protocol_version"] == 70019
    assert info["services"] == 0x0D
    assert info["user_agent"] == "/DigiByte:8.26.0/"
    assert info["start_height"] == 23000000
    assert info["relay"] is True


def test_parse_version_detects_bloom():
    payload = build_version_payload(
        protocol_version=70019,
        services=NODE_NETWORK | NODE_BLOOM,
        timestamp=1700000000,
        user_agent="/DigiByte:8.26.0/",
        start_height=0,
        relay=False,
    )
    info = parse_version_payload(payload)
    assert info["services"] & NODE_BLOOM != 0


def test_net_addr():
    addr = net_addr(0, "1.2.3.4", 12024)
    assert len(addr) == 26  # 8 services + 16 ip + 2 port


def test_build_verack():
    msg = build_verack(DGB_MAGIC)
    assert msg[:4] == DGB_MAGIC
    cmd, plen, _ = parse_message_header(msg[:24])
    assert cmd == "verack"
    assert plen == 0


def test_build_getaddr():
    msg = build_getaddr(DGB_MAGIC)
    cmd, plen, _ = parse_message_header(msg[:24])
    assert cmd == "getaddr"
    assert plen == 0


def test_parse_addr_payload_empty():
    # varint 0 = no addresses
    peers = parse_addr_payload(b"\x00")
    assert peers == []


def test_parse_addr_payload_one_peer():
    # Build a single addr entry: 4 bytes timestamp + 26 bytes net_addr
    ts = struct.pack("<I", 1700000000)
    addr = net_addr(NODE_NETWORK | NODE_BLOOM, "192.168.1.1", 12024)
    payload = encode_varint(1) + ts + addr
    peers = parse_addr_payload(payload)
    assert len(peers) == 1
    assert peers[0]["ip"] == "192.168.1.1"
    assert peers[0]["port"] == 12024
    assert peers[0]["services"] & NODE_BLOOM != 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/polloloco/dgb-bloom-seeder && python3 -m pytest tests/test_protocol.py -v 2>&1 | tail -5`
Expected: ERRORS (module not found)

- [ ] **Step 3: Implement protocol module**

```python
# seeder/protocol.py
"""DigiByte P2P protocol message encoding and decoding."""

import hashlib
import struct
import socket
import os

# Service flags
NODE_NETWORK = 0x01
NODE_BLOOM = 0x04
NODE_WITNESS = 0x08

# DigiByte mainnet magic
DGB_MAGIC = b"\xfa\xc3\xb6\xda"

# Message header: 4 magic + 12 command + 4 length + 4 checksum = 24 bytes
HEADER_SIZE = 24


def _checksum(payload: bytes) -> bytes:
    """Double SHA-256 checksum (first 4 bytes)."""
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]


def encode_varint(n: int) -> bytes:
    if n < 0xFD:
        return struct.pack("<B", n)
    elif n <= 0xFFFF:
        return b"\xfd" + struct.pack("<H", n)
    elif n <= 0xFFFFFFFF:
        return b"\xfe" + struct.pack("<I", n)
    else:
        return b"\xff" + struct.pack("<Q", n)


def decode_varint(data: bytes) -> tuple[int, int]:
    """Returns (value, bytes_consumed)."""
    first = data[0]
    if first < 0xFD:
        return first, 1
    elif first == 0xFD:
        return struct.unpack_from("<H", data, 1)[0], 3
    elif first == 0xFE:
        return struct.unpack_from("<I", data, 1)[0], 5
    else:
        return struct.unpack_from("<Q", data, 1)[0], 9


def net_addr(services: int, ip: str, port: int) -> bytes:
    """Encode a network address (without timestamp) — 26 bytes."""
    addr = struct.pack("<Q", services)
    # IPv4-mapped IPv6: 10 bytes 0x00, 2 bytes 0xFF, 4 bytes IPv4
    addr += b"\x00" * 10 + b"\xff\xff"
    addr += socket.inet_aton(ip)
    addr += struct.pack(">H", port)  # port is big-endian
    return addr


def make_message(magic: bytes, command: str, payload: bytes) -> bytes:
    """Build a complete P2P message with header."""
    cmd = command.encode("ascii").ljust(12, b"\x00")
    length = struct.pack("<I", len(payload))
    cs = _checksum(payload)
    return magic + cmd + length + cs + payload


def parse_message_header(header: bytes) -> tuple[str, int, bytes]:
    """Parse a 24-byte message header. Returns (command, payload_length, checksum)."""
    cmd = header[4:16].rstrip(b"\x00").decode("ascii")
    payload_len = struct.unpack_from("<I", header, 16)[0]
    checksum = header[20:24]
    return cmd, payload_len, checksum


def build_version_payload(
    protocol_version: int = 70019,
    services: int = 0,
    timestamp: int = 0,
    user_agent: str = "/DGB-Bloom-Seeder:1.0/",
    start_height: int = 0,
    relay: bool = False,
) -> bytes:
    """Build the payload for a version message."""
    payload = struct.pack("<i", protocol_version)
    payload += struct.pack("<Q", services)
    payload += struct.pack("<q", timestamp)
    payload += net_addr(0, "0.0.0.0", 0)  # addr_recv
    payload += net_addr(services, "0.0.0.0", 0)  # addr_from
    payload += struct.pack("<Q", int.from_bytes(os.urandom(8), "little"))  # nonce
    ua_bytes = user_agent.encode("utf-8")
    payload += encode_varint(len(ua_bytes)) + ua_bytes
    payload += struct.pack("<i", start_height)
    payload += struct.pack("<?", relay)
    return payload


def parse_version_payload(payload: bytes) -> dict:
    """Parse a version message payload. Returns dict with key fields."""
    offset = 0
    protocol_version = struct.unpack_from("<i", payload, offset)[0]
    offset += 4
    services = struct.unpack_from("<Q", payload, offset)[0]
    offset += 8
    timestamp = struct.unpack_from("<q", payload, offset)[0]
    offset += 8
    offset += 26  # addr_recv
    offset += 26  # addr_from
    offset += 8   # nonce

    ua_len, varint_size = decode_varint(payload[offset:])
    offset += varint_size
    user_agent = payload[offset:offset + ua_len].decode("utf-8", errors="replace")
    offset += ua_len

    start_height = struct.unpack_from("<i", payload, offset)[0]
    offset += 4

    relay = bool(payload[offset]) if offset < len(payload) else True

    return {
        "protocol_version": protocol_version,
        "services": services,
        "timestamp": timestamp,
        "user_agent": user_agent,
        "start_height": start_height,
        "relay": relay,
    }


def build_verack(magic: bytes) -> bytes:
    return make_message(magic, "verack", b"")


def build_getaddr(magic: bytes) -> bytes:
    return make_message(magic, "getaddr", b"")


def parse_addr_payload(payload: bytes) -> list[dict]:
    """Parse an addr message payload. Returns list of peer dicts."""
    if not payload:
        return []
    count, offset = decode_varint(payload)
    peers = []
    for _ in range(count):
        if offset + 30 > len(payload):
            break
        # 4 bytes timestamp + 26 bytes net_addr
        offset += 4  # skip timestamp
        services = struct.unpack_from("<Q", payload, offset)[0]
        offset += 8
        # IPv6 address: 16 bytes. Last 4 are IPv4 if starts with ffff
        ip_bytes = payload[offset:offset + 16]
        offset += 16
        port = struct.unpack_from(">H", payload, offset)[0]
        offset += 2

        # Extract IPv4 from IPv4-mapped IPv6
        if ip_bytes[:12] == b"\x00" * 10 + b"\xff\xff":
            ip = socket.inet_ntoa(ip_bytes[12:16])
        else:
            continue  # skip IPv6 peers for now

        # Skip private/reserved IPs
        if ip.startswith("0.") or ip.startswith("127.") or ip.startswith("10.") or ip.startswith("192.168."):
            continue

        peers.append({"ip": ip, "port": port, "services": services})
    return peers
```

- [ ] **Step 4: Run tests**

Run: `cd /home/polloloco/dgb-bloom-seeder && python3 -m pytest tests/test_protocol.py -v 2>&1 | tail -20`
Expected: All PASSED

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/protocol.py tests/test_protocol.py
git commit -m "feat: P2P protocol module — message encoding, version handshake, addr parsing"
```

---

### Task 3: Storage module

**Files:**
- Create: `seeder/storage.py`
- Create: `tests/test_storage.py`

- [ ] **Step 1: Write storage tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /home/polloloco/dgb-bloom-seeder && python3 -m pytest tests/test_storage.py -v 2>&1 | tail -5`
Expected: ERRORS (module not found)

- [ ] **Step 3: Implement storage module**

```python
# seeder/storage.py
"""SQLite storage for bloom peers and crawl queue."""

import time
import aiosqlite


class Storage:
    def __init__(self, db_path: str = "bloom_seeder.db"):
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def init(self):
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
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
        """)
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()

    async def upsert_bloom_peer(
        self, ip: str, port: int, services: int,
        protocol_version: int, user_agent: str, seen_at: int
    ):
        await self._db.execute("""
            INSERT INTO bloom_peers (ip, port, services, protocol_version, user_agent, last_seen, first_seen)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ip, port) DO UPDATE SET
                services = excluded.services,
                protocol_version = excluded.protocol_version,
                user_agent = excluded.user_agent,
                last_seen = excluded.last_seen
        """, (ip, port, services, protocol_version, user_agent, seen_at, seen_at))
        await self._db.commit()

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

    async def add_crawl_peers(self, peers: list[tuple[str, int]]):
        await self._db.executemany("""
            INSERT OR IGNORE INTO all_peers (ip, port) VALUES (?, ?)
        """, peers)
        await self._db.commit()

    async def get_uncrawled_peers(self, limit: int = 500) -> list[tuple[str, int]]:
        cutoff = int(time.time()) - 1800  # re-crawl after 30 min
        cursor = await self._db.execute("""
            SELECT ip, port FROM all_peers
            WHERE last_crawled < ?
            ORDER BY last_crawled ASC
            LIMIT ?
        """, (cutoff, limit))
        rows = await cursor.fetchall()
        return [(r["ip"], r["port"]) for r in rows]

    async def mark_crawled(self, ip: str, port: int):
        await self._db.execute("""
            UPDATE all_peers SET last_crawled = ? WHERE ip = ? AND port = ?
        """, (int(time.time()), ip, port))
        await self._db.commit()

    async def prune(self, max_age_hours: int = 24) -> int:
        cutoff = int(time.time()) - max_age_hours * 3600
        cursor = await self._db.execute(
            "DELETE FROM bloom_peers WHERE last_seen < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount

    async def get_stats(self, max_age_hours: int = 6) -> dict:
        cutoff = int(time.time()) - max_age_hours * 3600

        cursor = await self._db.execute("SELECT COUNT(*) FROM bloom_peers")
        total = (await cursor.fetchone())[0]

        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM bloom_peers WHERE last_seen >= ?", (cutoff,)
        )
        recent = (await cursor.fetchone())[0]

        cursor = await self._db.execute("SELECT COUNT(*) FROM all_peers")
        all_known = (await cursor.fetchone())[0]

        return {
            "bloom_peers_total": total,
            "bloom_peers_recent": recent,
            "all_peers_known": all_known,
        }
```

- [ ] **Step 4: Run tests**

Run: `cd /home/polloloco/dgb-bloom-seeder && python3 -m pytest tests/test_storage.py -v 2>&1 | tail -15`
Expected: All PASSED

- [ ] **Step 5: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/storage.py tests/test_storage.py
git commit -m "feat: SQLite storage — bloom peer upsert, crawl queue, pruning, stats"
```

---

### Task 4: Crawler

**Files:**
- Create: `seeder/crawler.py`

- [ ] **Step 1: Implement the crawler**

```python
# seeder/crawler.py
"""Network crawler — connects to peers, performs P2P handshake, discovers bloom-capable nodes."""

import asyncio
import logging
import socket
import time

from seeder.config import Config
from seeder.protocol import (
    HEADER_SIZE, NODE_BLOOM,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr, parse_addr_payload,
)
from seeder.storage import Storage

log = logging.getLogger("crawler")


async def resolve_seeds(seeds: list[str], port: int) -> list[tuple[str, int]]:
    """Resolve DNS seeds to IP addresses."""
    peers = []
    loop = asyncio.get_event_loop()
    for seed in seeds:
        try:
            infos = await loop.getaddrinfo(seed, None, family=socket.AF_INET)
            for info in infos:
                ip = info[4][0]
                peers.append((ip, port))
        except Exception as e:
            log.warning("Failed to resolve %s: %s", seed, e)
    log.info("Resolved %d peers from %d DNS seeds", len(peers), len(seeds))
    return peers


async def handshake_peer(
    ip: str, port: int, magic: bytes, timeout: int = 5
) -> dict | None:
    """Connect to a peer, perform version handshake, request addrs.
    Returns peer info dict or None on failure."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=timeout
        )
    except (OSError, asyncio.TimeoutError):
        return None

    try:
        # Send our version
        version_payload = build_version_payload(
            timestamp=int(time.time()),
            user_agent="/DGB-Bloom-Seeder:1.0/",
        )
        writer.write(make_message(magic, "version", version_payload))
        await writer.drain()

        # Read their version
        header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=timeout)
        cmd, payload_len, _ = parse_message_header(header)

        if cmd != "version" or payload_len > 1024:
            return None

        payload = await asyncio.wait_for(reader.readexactly(payload_len), timeout=timeout)
        info = parse_version_payload(payload)
        info["ip"] = ip
        info["port"] = port

        # Send verack
        writer.write(build_verack(magic))
        await writer.drain()

        # Try to read their verack (may or may not come)
        # Then send getaddr and try to read addr response
        addrs = []
        try:
            # Read verack
            header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
            cmd, plen, _ = parse_message_header(header)
            if plen > 0:
                await asyncio.wait_for(reader.readexactly(plen), timeout=2)

            # Send getaddr
            writer.write(build_getaddr(magic))
            await writer.drain()

            # Read responses until we get addr or timeout
            deadline = time.time() + 3
            while time.time() < deadline:
                remaining = max(0.1, deadline - time.time())
                header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=remaining)
                cmd, plen, _ = parse_message_header(header)
                body = b""
                if plen > 0 and plen < 100_000:
                    body = await asyncio.wait_for(reader.readexactly(plen), timeout=remaining)
                elif plen > 0:
                    break  # payload too large, skip
                if cmd == "addr" and body:
                    addrs = parse_addr_payload(body)
                    break
        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
            pass  # addr collection is best-effort

        info["discovered_peers"] = addrs
        return info

    except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError, Exception) as e:
        log.debug("Handshake failed with %s:%d: %s", ip, port, e)
        return None
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


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

            if result is None:
                return

            if result["services"] & NODE_BLOOM:
                bloom_found += 1
                await storage.upsert_bloom_peer(
                    ip, port, result["services"],
                    result["protocol_version"],
                    result["user_agent"],
                    int(time.time()),
                )
                log.info("BLOOM peer: %s:%d %s (services=0x%02x)",
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

    # Prune old entries
    pruned = await storage.prune(max_age_hours=config.prune_hours)

    elapsed = time.time() - start
    stats = {
        "checked": total_checked,
        "bloom_found": bloom_found,
        "new_peers": new_peers_discovered,
        "pruned": pruned,
        "elapsed_seconds": round(elapsed, 1),
    }
    log.info("Crawl complete: %s", stats)
    return stats


async def crawler_loop(config: Config, storage: Storage):
    """Run crawl cycles forever on the configured interval."""
    while True:
        try:
            await crawl_cycle(config, storage)
        except Exception:
            log.exception("Crawl cycle failed")
        await asyncio.sleep(config.crawl_interval)
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `cd /home/polloloco/dgb-bloom-seeder && python3 -c "from seeder.crawler import crawl_cycle; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/crawler.py
git commit -m "feat: network crawler — P2P handshake, bloom detection, peer discovery"
```

---

### Task 5: API server

**Files:**
- Create: `seeder/api.py`

- [ ] **Step 1: Implement the API**

```python
# seeder/api.py
"""Lightweight HTTP API serving bloom peer data."""

import time
import logging
from aiohttp import web

from seeder.config import Config
from seeder.storage import Storage

log = logging.getLogger("api")

_start_time = time.time()
_last_crawl_time = 0


def set_last_crawl_time(t: int):
    global _last_crawl_time
    _last_crawl_time = t


def create_app(config: Config, storage: Storage) -> web.Application:
    app = web.Application()

    async def handle_peers(request: web.Request) -> web.Response:
        peers = await storage.get_bloom_peers(
            max_age_hours=config.api_max_age_hours,
            limit=config.api_max_results,
        )
        crawl_age = int(time.time() - _last_crawl_time) if _last_crawl_time else -1
        return web.json_response({
            "peers": peers,
            "count": len(peers),
            "crawl_age_seconds": crawl_age,
        })

    async def handle_stats(request: web.Request) -> web.Response:
        stats = await storage.get_stats(max_age_hours=config.api_max_age_hours)
        stats["last_crawl"] = _last_crawl_time
        stats["uptime_seconds"] = int(time.time() - _start_time)
        return web.json_response(stats)

    app.router.add_get("/peers", handle_peers)
    app.router.add_get("/stats", handle_stats)

    return app


async def start_api(config: Config, storage: Storage):
    """Start the API server (runs forever)."""
    app = create_app(config, storage)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.api_host, config.api_port)
    await site.start()
    log.info("API listening on %s:%d", config.api_host, config.api_port)
    # Keep running forever — caller manages the event loop
    await asyncio.sleep(float("inf"))


# Need asyncio import for sleep
import asyncio
```

- [ ] **Step 2: Verify it imports cleanly**

Run: `cd /home/polloloco/dgb-bloom-seeder && python3 -c "from seeder.api import create_app; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder/api.py
git commit -m "feat: HTTP API server — /peers and /stats endpoints"
```

---

### Task 6: Entry point

**Files:**
- Create: `seeder.py`

- [ ] **Step 1: Create the entry point**

```python
#!/usr/bin/env python3
"""DGB Bloom Seeder — discovers bloom-capable DigiByte nodes."""

import asyncio
import logging
import sys
import time

from seeder.config import load_config
from seeder.storage import Storage
from seeder.crawler import crawl_cycle, resolve_seeds, crawler_loop
from seeder.api import start_api, set_last_crawl_time, create_app
from aiohttp import web


async def main():
    config = load_config()

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("seeder")
    log.info("DGB Bloom Seeder starting")
    log.info("Config: port=%d crawl_interval=%ds concurrency=%d",
             config.api_port, config.crawl_interval, config.crawl_concurrency)

    # Init storage
    storage = Storage(config.db_path)
    await storage.init()

    # Seed initial peers from DNS
    dns_peers = await resolve_seeds(config.dns_seeds, config.dgb_port)
    await storage.add_crawl_peers(dns_peers)
    log.info("Seeded %d peers from DNS", len(dns_peers))

    # Run initial crawl before starting API
    log.info("Running initial crawl...")
    stats = await crawl_cycle(config, storage)
    set_last_crawl_time(int(time.time()))
    log.info("Initial crawl complete: %d bloom peers found", stats["bloom_found"])

    # Start API server
    app = create_app(config, storage)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.api_host, config.api_port)
    await site.start()
    log.info("API listening on http://%s:%d", config.api_host, config.api_port)

    # Run crawler loop in background
    async def crawl_forever():
        while True:
            await asyncio.sleep(config.crawl_interval)
            try:
                await crawl_cycle(config, storage)
                set_last_crawl_time(int(time.time()))
            except Exception:
                log.exception("Crawl cycle failed")

    try:
        await crawl_forever()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()
        await storage.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down.")
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x /home/polloloco/dgb-bloom-seeder/seeder.py`

- [ ] **Step 3: Commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add seeder.py
git commit -m "feat: entry point — runs initial crawl then starts API + crawler loop"
```

---

### Task 7: Live network test and deployment

- [ ] **Step 1: Run the seeder locally against the real network**

Run: `cd /home/polloloco/dgb-bloom-seeder && timeout 120 python3 seeder.py 2>&1 | tee /tmp/seeder_test.log`

Watch for:
- DNS seed resolution succeeding
- Peer handshakes completing
- At least 1 `BLOOM peer:` line in the output
- API starts on port 8025

- [ ] **Step 2: Test the API**

In a separate terminal (or after the seeder runs for 60+ seconds):

Run: `curl -s http://localhost:8025/peers | python3 -m json.tool | head -20`
Expected: JSON with `peers` array containing at least 1 bloom peer

Run: `curl -s http://localhost:8025/stats | python3 -m json.tool`
Expected: JSON with `bloom_peers_total` > 0

- [ ] **Step 3: Create README**

```markdown
# DGB Bloom Seeder

Discovers DigiByte nodes with bloom filter support (`NODE_BLOOM`) and serves them via a lightweight JSON API. Built for SPV wallets that need bloom-capable peers.

## Quick Start

```bash
pip install -r requirements.txt
python3 seeder.py
```

The seeder will:
1. Resolve peers from DigiByte DNS seeds
2. Connect to each peer, perform a P2P version handshake
3. Store peers that advertise `NODE_BLOOM` in their service flags
4. Serve discovered peers at `http://localhost:8025/peers`
5. Re-crawl every 30 minutes

## API

### `GET /peers`

Returns bloom-capable peers seen in the last 6 hours.

```json
{
    "peers": [
        {
            "ip": "134.199.198.90",
            "port": 12024,
            "last_seen": 1743900000,
            "protocol_version": 70019,
            "user_agent": "/DigiByte:8.26.0/"
        }
    ],
    "count": 1,
    "crawl_age_seconds": 120
}
```

### `GET /stats`

Health check and crawl statistics.

```json
{
    "bloom_peers_total": 42,
    "bloom_peers_recent": 38,
    "all_peers_known": 1250,
    "last_crawl": 1743900000,
    "uptime_seconds": 86400
}
```

## Configuration

Edit `config.yaml`:

```yaml
crawl_interval: 1800     # Seconds between crawls
crawl_concurrency: 10    # Simultaneous peer connections
api_port: 8025           # HTTP API port
prune_hours: 24          # Remove peers not seen in this window
```

## Deployment

With PM2:
```bash
pm2 start seeder.py --name bloom-seeder --interpreter python3
```

With systemd:
```ini
[Unit]
Description=DGB Bloom Seeder
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/dgb-bloom-seeder/seeder.py
WorkingDirectory=/opt/dgb-bloom-seeder
Restart=always

[Install]
WantedBy=multi-user.target
```

## Why This Exists

DigiByte v8.26 nodes default to `peerbloomfilters=0`, which means they reject SPV wallet connections. Mobile wallets using bloom filters need to find the minority of nodes that have bloom enabled. This seeder automates that discovery.

**Node operators:** You can help mobile wallets by adding `peerbloomfilters=1` to your `digibyte.conf` and restarting your node.

## License

MIT
```

- [ ] **Step 4: Commit README**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add README.md
git commit -m "docs: README with quick start, API reference, deployment, and rationale"
```

- [ ] **Step 5: Deploy to VPS**

```bash
# Copy to VPS
scp -r /home/polloloco/dgb-bloom-seeder root@digiscope.me:/opt/dgb-bloom-seeder

# SSH in and set up
ssh root@digiscope.me
cd /opt/dgb-bloom-seeder
pip install -r requirements.txt
pm2 start seeder.py --name bloom-seeder --interpreter python3
pm2 save
```

- [ ] **Step 6: Add nginx proxy**

Add to nginx config for `api.digiscope.me`:
```nginx
location /api/peers/bloom {
    proxy_pass http://localhost:8025/peers;
}
location /api/peers/stats {
    proxy_pass http://localhost:8025/stats;
}
```

Reload: `nginx -t && systemctl reload nginx`

- [ ] **Step 7: Verify production endpoint**

Run: `curl -s https://api.digiscope.me/api/peers/bloom | python3 -m json.tool | head -10`
Expected: JSON with bloom peers

- [ ] **Step 8: Create GitHub repo and push**

```bash
cd /home/polloloco/dgb-bloom-seeder
gh repo create JohnnyLawDGB/dgb-bloom-seeder --public --source=. --push
```

- [ ] **Step 9: Final commit**

```bash
cd /home/polloloco/dgb-bloom-seeder
git add -A
git commit -m "chore: complete DGB bloom seeder v1.0"
git push
```
