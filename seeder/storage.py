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

    async def get_known_bloom_peer_set(self) -> set[tuple[str, int]]:
        """Return the current set of (ip, port) tuples in bloom_peers.
        Used by the crawler to decide which IPs should have attempts logged."""
        cursor = await self._db.execute("SELECT ip, port FROM bloom_peers")
        rows = await cursor.fetchall()
        return {(r["ip"], r["port"]) for r in rows}

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

    async def prune_attempts(self, window_days: int) -> int:
        """Delete attempt rows older than the ranking window. Returns rows removed."""
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "DELETE FROM bloom_peer_attempts WHERE ts < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount

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
