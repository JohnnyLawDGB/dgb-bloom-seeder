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
