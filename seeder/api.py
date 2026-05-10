# seeder/api.py
"""Lightweight HTTP API serving bloom peer data."""

import asyncio
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
        peers = await storage.get_ranked_peers(
            capability="bloom",
            window_days=config.ranking_window_days,
            prior_attempts=config.ranking_prior_attempts,
            prior_successes=config.ranking_prior_successes,
            longevity_cap_days=config.ranking_longevity_cap_days,
            longevity_weight=config.ranking_longevity_weight,
            inclusion_threshold=config.ranking_inclusion_threshold,
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
