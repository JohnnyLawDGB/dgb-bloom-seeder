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

        # Create new schema (idempotent).
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS peers (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                services INTEGER NOT NULL,
                protocol_version INTEGER,
                user_agent TEXT,
                last_seen INTEGER NOT NULL,
                first_seen INTEGER NOT NULL,
                bloom_validated_at  INTEGER,
                filter_validated_at INTEGER,
                PRIMARY KEY (ip, port)
            );
            CREATE INDEX IF NOT EXISTS idx_peers_last_seen ON peers(last_seen);
            CREATE INDEX IF NOT EXISTS idx_peers_bloom
                ON peers(bloom_validated_at)  WHERE bloom_validated_at  IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_peers_filter
                ON peers(filter_validated_at) WHERE filter_validated_at IS NOT NULL;

            CREATE TABLE IF NOT EXISTS all_peers (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                last_crawled INTEGER DEFAULT 0,
                PRIMARY KEY (ip, port)
            );

            CREATE TABLE IF NOT EXISTS peer_attempts (
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                ts INTEGER NOT NULL,
                capability TEXT NOT NULL,
                success INTEGER NOT NULL,
                PRIMARY KEY (ip, port, ts, capability)
            );
            CREATE INDEX IF NOT EXISTS idx_attempts_cap_ts
                ON peer_attempts(capability, ts);
            CREATE INDEX IF NOT EXISTS idx_attempts_peer_cap_ts
                ON peer_attempts(ip, port, capability, ts);
        """)

        # One-time migration from old (bloom_peers, bloom_peer_attempts) schema.
        cursor = await self._db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='bloom_peers'"
        )
        if (await cursor.fetchone()) is not None:
            await self._db.executescript("""
                BEGIN;
                INSERT OR IGNORE INTO peers
                    (ip, port, services, protocol_version, user_agent,
                     last_seen, first_seen, bloom_validated_at, filter_validated_at)
                SELECT ip, port, services, protocol_version, user_agent,
                       last_seen, first_seen, last_seen, NULL
                FROM bloom_peers;

                CREATE TABLE IF NOT EXISTS bloom_peer_attempts (
                    ip TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    ts INTEGER NOT NULL,
                    success INTEGER NOT NULL,
                    PRIMARY KEY (ip, port, ts)
                );
                INSERT OR IGNORE INTO peer_attempts
                    (ip, port, ts, capability, success)
                SELECT ip, port, ts, 'bloom', success
                FROM bloom_peer_attempts;

                DROP TABLE bloom_peer_attempts;
                DROP TABLE bloom_peers;
                COMMIT;
            """)

        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()

    async def upsert_bloom_peer(
        self, ip: str, port: int, services: int,
        protocol_version: int, user_agent: str, seen_at: int
    ):
        """Upsert a bloom-validated peer. Sets bloom_validated_at = seen_at."""
        await self._db.execute("""
            INSERT INTO peers (ip, port, services, protocol_version, user_agent,
                               last_seen, first_seen, bloom_validated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ip, port) DO UPDATE SET
                services = excluded.services,
                protocol_version = excluded.protocol_version,
                user_agent = excluded.user_agent,
                last_seen = excluded.last_seen,
                bloom_validated_at = excluded.bloom_validated_at
        """, (ip, port, services, protocol_version, user_agent, seen_at, seen_at, seen_at))
        await self._db.commit()

    async def get_ranked_peers(
        self,
        *,
        capability: str,
        window_days: int,
        prior_attempts: int,
        prior_successes: int,
        longevity_cap_days: int,
        longevity_weight: float,
        inclusion_threshold: float,
        max_age_hours: int,
        limit: int,
    ) -> list[dict]:
        """Return peers above threshold for the given capability, sorted by composite score DESC.

        capability must be 'bloom' or 'filter'."""
        if capability == "bloom":
            validated_col = "bloom_validated_at"
        elif capability == "filter":
            validated_col = "filter_validated_at"
        else:
            raise ValueError(f"unknown capability: {capability!r}")

        now = int(time.time())
        window_cutoff = now - window_days * 86400
        last_seen_cutoff = now - max_age_hours * 3600
        longevity_now = now
        tenure_now = now

        cursor = await self._db.execute(
            f"""
            WITH stats AS (
                SELECT bp.ip, bp.port, bp.services,
                       bp.last_seen, bp.first_seen,
                       bp.protocol_version, bp.user_agent,
                       bp.bloom_validated_at, bp.filter_validated_at,
                       COALESCE(SUM(a.success), 0) AS successes_7d,
                       COALESCE(COUNT(a.ts), 0)    AS attempts_7d
                FROM peers bp
                LEFT JOIN peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.capability = ?
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                  AND bp.{validated_col} IS NOT NULL
                GROUP BY bp.ip, bp.port
            ),
            scored AS (
                SELECT *,
                       (successes_7d + ?) * 1.0 / (attempts_7d + ?) AS uptime_score,
                       MIN((? - first_seen) / 86400.0 / ?, 1.0)     AS longevity_bonus
                FROM stats
            )
            SELECT ip, port, services,
                   last_seen, first_seen,
                   protocol_version, user_agent,
                   bloom_validated_at, filter_validated_at,
                   successes_7d, attempts_7d,
                   uptime_score,
                   uptime_score * (1 + ? * longevity_bonus) AS composite_score,
                   (? - first_seen) / 86400.0              AS tenure_days
            FROM scored
            WHERE uptime_score >= ?
            ORDER BY composite_score DESC, last_seen DESC
            LIMIT ?
            """,
            (
                capability,
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

    async def get_attempts_total(self, window_days: int) -> int:
        """Count of attempt rows within the ranking window. Used by /stats."""
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM peer_attempts WHERE ts >= ?", (cutoff,)
        )
        return (await cursor.fetchone())[0]

    async def get_above_threshold_count(
        self,
        *,
        capability: str,
        threshold: float,
        prior_attempts: int,
        prior_successes: int,
        window_days: int,
        max_age_hours: int,
    ) -> int:
        """How many peers would appear in /peers?capability=... given current threshold."""
        if capability == "bloom":
            validated_col = "bloom_validated_at"
        elif capability == "filter":
            validated_col = "filter_validated_at"
        else:
            raise ValueError(f"unknown capability: {capability!r}")

        now = int(time.time())
        window_cutoff = now - window_days * 86400
        last_seen_cutoff = now - max_age_hours * 3600

        cursor = await self._db.execute(
            f"""
            WITH stats AS (
                SELECT bp.ip, bp.port,
                       COALESCE(SUM(a.success), 0) AS successes_7d,
                       COALESCE(COUNT(a.ts), 0)    AS attempts_7d
                FROM peers bp
                LEFT JOIN peer_attempts a
                       ON a.ip = bp.ip
                      AND a.port = bp.port
                      AND a.capability = ?
                      AND a.ts >= ?
                WHERE bp.last_seen >= ?
                  AND bp.{validated_col} IS NOT NULL
                GROUP BY bp.ip, bp.port
            )
            SELECT COUNT(*) FROM stats
            WHERE (successes_7d + ?) * 1.0 / (attempts_7d + ?) >= ?
            """,
            (
                capability,
                window_cutoff,
                last_seen_cutoff,
                prior_successes,
                prior_attempts,
                threshold,
            ),
        )
        return (await cursor.fetchone())[0]

    async def get_validated_peer_set(
        self, *, capability: str
    ) -> set[tuple[str, int]]:
        """Return (ip, port) tuples for peers ever validated for the given capability.

        capability must be 'bloom' or 'filter'."""
        if capability == "bloom":
            col = "bloom_validated_at"
        elif capability == "filter":
            col = "filter_validated_at"
        else:
            raise ValueError(f"unknown capability: {capability!r}")
        # Column name is whitelisted above so f-string interpolation is safe.
        cursor = await self._db.execute(
            f"SELECT ip, port FROM peers WHERE {col} IS NOT NULL"
        )
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

    async def record_attempt(
        self, ip: str, port: int, *, capability: str, success: bool, ts: int
    ):
        """Log a single crawl-attempt outcome against a peer for a specific capability.

        capability must be 'bloom' or 'filter'."""
        if capability not in ("bloom", "filter"):
            raise ValueError(f"unknown capability: {capability!r}")
        await self._db.execute(
            """
            INSERT OR REPLACE INTO peer_attempts (ip, port, ts, capability, success)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ip, port, ts, capability, 1 if success else 0),
        )
        await self._db.commit()

    async def prune_attempts(self, window_days: int) -> int:
        """Delete attempt rows older than the ranking window. Returns rows removed."""
        cutoff = int(time.time()) - window_days * 86400
        cursor = await self._db.execute(
            "DELETE FROM peer_attempts WHERE ts < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount

    async def prune(self, max_age_hours: int = 24) -> int:
        """Remove peers not seen within window. Also drops their attempt history."""
        cutoff = int(time.time()) - max_age_hours * 3600
        await self._db.execute(
            """
            DELETE FROM peer_attempts
            WHERE (ip, port) IN (
                SELECT ip, port FROM peers WHERE last_seen < ?
            )
            """,
            (cutoff,),
        )
        cursor = await self._db.execute(
            "DELETE FROM peers WHERE last_seen < ?", (cutoff,)
        )
        await self._db.commit()
        return cursor.rowcount

    async def get_stats(
        self,
        *,
        max_age_hours: int,
        threshold: float,
        prior_attempts: int,
        prior_successes: int,
        window_days: int,
    ) -> dict:
        cutoff = int(time.time()) - max_age_hours * 3600

        cursor = await self._db.execute("SELECT COUNT(*) FROM peers")
        total = (await cursor.fetchone())[0]

        cursor = await self._db.execute(
            "SELECT COUNT(*) FROM peers WHERE last_seen >= ?", (cutoff,)
        )
        recent = (await cursor.fetchone())[0]

        cursor = await self._db.execute("SELECT COUNT(*) FROM all_peers")
        all_known = (await cursor.fetchone())[0]

        above_threshold = await self.get_above_threshold_count(
            capability="bloom",
            threshold=threshold,
            prior_attempts=prior_attempts,
            prior_successes=prior_successes,
            window_days=window_days,
            max_age_hours=max_age_hours,
        )

        attempts_total = await self.get_attempts_total(window_days=window_days)

        return {
            "bloom_peers_total": total,
            "bloom_peers_recent": recent,
            "bloom_peers_above_threshold": above_threshold,
            "all_peers_known": all_known,
            "attempts_7d_total": attempts_total,
        }
