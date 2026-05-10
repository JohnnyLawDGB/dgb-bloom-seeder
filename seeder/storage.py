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
        longevity_now = now      # used to compute tenure for longevity bonus
        tenure_now = now         # used for tenure_days output

        cursor = await self._db.execute(
            """
            WITH stats AS (
                SELECT bp.ip, bp.port, bp.services,
                       bp.last_seen, bp.first_seen,
                       bp.protocol_version, bp.user_agent,
                       COALESCE(SUM(a.success), 0)   AS successes_7d,
                       COALESCE(COUNT(a.ts), 0)      AS attempts_7d
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
                longevity_now,
                longevity_cap_days,
                longevity_weight,
                tenure_now,
                inclusion_threshold,
                limit,
            ),
        )
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
