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

    # Load any operator-configured static peers
    if config.static_peers:
        static = [(p["ip"], p["port"]) for p in config.static_peers]
        await storage.add_crawl_peers(static)
        log.info("Loaded %d static peers from config", len(static))

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
