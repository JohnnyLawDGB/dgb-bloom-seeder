# seeder/api.py
"""Lightweight HTTP API serving bloom peer data."""

import asyncio
import time
import logging
from aiohttp import web

from seeder.config import Config
from seeder.storage import Storage

log = logging.getLogger("api")

SERVICE_FLAG_NAMES = [
    (0x001, "NETWORK"),
    (0x002, "GETUTXO"),
    (0x004, "BLOOM"),
    (0x008, "WITNESS"),
    (0x040, "COMPACT_FILTERS"),
    (0x400, "NETWORK_LIMITED"),
]


def _services_to_capabilities(services: int) -> list[str]:
    """Translate a services bitmask into a list of human-readable capability names."""
    return [name for bit, name in SERVICE_FLAG_NAMES if services & bit]

_start_time = time.time()
_last_crawl_time = 0


def set_last_crawl_time(t: int):
    global _last_crawl_time
    _last_crawl_time = t


def create_app(config: Config, storage: Storage) -> web.Application:
    app = web.Application()

    async def handle_peers(request: web.Request) -> web.Response:
        cap = request.query.get("capability", "").lower()

        # Validate
        if cap == "":
            mode = "default"
        elif cap == "bloom":
            mode = "bloom"
        elif cap == "filter":
            mode = "filter"
        elif cap in ("bloom|filter", "filter|bloom"):
            mode = "combined"
        else:
            return web.json_response(
                {"error": f"invalid capability: {cap!r}"}, status=400
            )

        async def fetch(capability: str) -> list[dict]:
            return await storage.get_ranked_peers(
                capability=capability,
                window_days=config.ranking_window_days,
                prior_attempts=config.ranking_prior_attempts,
                prior_successes=config.ranking_prior_successes,
                longevity_cap_days=config.ranking_longevity_cap_days,
                longevity_weight=config.ranking_longevity_weight,
                inclusion_threshold=config.ranking_inclusion_threshold,
                max_age_hours=config.api_max_age_hours,
                limit=config.api_max_results,
            )

        if mode == "default":
            peers = await fetch("filter")
            response_capability = "filter"
            if not peers:
                peers = await fetch("bloom")
                response_capability = "bloom"
            for p in peers:
                p["peer_capability"] = response_capability
        elif mode == "bloom":
            peers = await fetch("bloom")
            response_capability = "bloom"
            for p in peers:
                p["peer_capability"] = "bloom"
        elif mode == "filter":
            peers = await fetch("filter")
            response_capability = "filter"
            for p in peers:
                p["peer_capability"] = "filter"
        elif mode == "combined":
            filter_peers = await fetch("filter")
            for p in filter_peers:
                p["peer_capability"] = "filter"
            bloom_peers = await fetch("bloom")
            for p in bloom_peers:
                p["peer_capability"] = "bloom"
            peers = filter_peers + bloom_peers
            response_capability = "filter+bloom"

        # Enrich each peer with services_hex and capabilities array.
        for p in peers:
            p["services_hex"] = f"0x{p['services']:x}"
            p["capabilities"] = _services_to_capabilities(p["services"])

        crawl_age = int(time.time() - _last_crawl_time) if _last_crawl_time else -1
        return web.json_response({
            "peers": peers,
            "count": len(peers),
            "capability": response_capability,
            "crawl_age_seconds": crawl_age,
        })

    async def handle_stats(request: web.Request) -> web.Response:
        stats = await storage.get_stats(
            max_age_hours=config.api_max_age_hours,
            threshold=config.ranking_inclusion_threshold,
            prior_attempts=config.ranking_prior_attempts,
            prior_successes=config.ranking_prior_successes,
            window_days=config.ranking_window_days,
        )
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
