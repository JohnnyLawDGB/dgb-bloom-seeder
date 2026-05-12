# seeder/crawler.py
"""Network crawler — connects to peers, performs P2P handshake, discovers bloom-capable nodes."""

import asyncio
import logging
import socket
import time

from seeder.config import Config
from seeder.protocol import (
    HEADER_SIZE, NODE_BLOOM, NODE_COMPACT_FILTERS,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr, build_filterload,
    build_getcfheaders, parse_addr_payload,
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

        # Try to read their verack, then verify bloom support
        addrs = []
        bloom_verified = False
        filter_verified = False
        try:
            # Read verack
            header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
            cmd, plen, _ = parse_message_header(header)
            if plen > 0:
                await asyncio.wait_for(reader.readexactly(plen), timeout=2)

            # If peer advertises NODE_BLOOM, verify by sending a filterload.
            # Peers that have peerbloomfilters=0 will disconnect immediately.
            if info["services"] & NODE_BLOOM:
                writer.write(build_filterload(magic))
                await writer.drain()
                # Wait 2 seconds — if the peer doesn't disconnect, bloom works
                try:
                    header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
                    cmd, plen, _ = parse_message_header(header)
                    if plen > 0 and plen < 100_000:
                        await asyncio.wait_for(reader.readexactly(plen), timeout=2)
                    # Peer responded instead of disconnecting — bloom is real
                    bloom_verified = True
                except asyncio.TimeoutError:
                    # Timeout means peer didn't disconnect — bloom is real
                    bloom_verified = True
                except (asyncio.IncompleteReadError, ConnectionError):
                    # Peer disconnected after filterload — bloom is fake
                    bloom_verified = False

            # If peer advertises NODE_COMPACT_FILTERS, verify with a getcfheaders round-trip.
            # Mirrors the bloom path: a peer that doesn't actually support BIP 157 will
            # disconnect on this message.
            if info["services"] & NODE_COMPACT_FILTERS:
                writer.write(build_getcfheaders(magic))
                await writer.drain()
                try:
                    header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
                    cmd, plen, _ = parse_message_header(header)
                    if plen > 0 and plen < 100_000:
                        await asyncio.wait_for(reader.readexactly(plen), timeout=2)
                    filter_verified = True
                except asyncio.TimeoutError:
                    filter_verified = True
                except (asyncio.IncompleteReadError, ConnectionError):
                    filter_verified = False

            # Send getaddr to discover more peers
            try:
                writer.write(build_getaddr(magic))
                await writer.drain()

                deadline = time.time() + 3
                while time.time() < deadline:
                    remaining = max(0.1, deadline - time.time())
                    header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=remaining)
                    cmd, plen, _ = parse_message_header(header)
                    body = b""
                    if plen > 0 and plen < 100_000:
                        body = await asyncio.wait_for(reader.readexactly(plen), timeout=remaining)
                    elif plen > 0:
                        break
                    if cmd == "addr" and body:
                        addrs = parse_addr_payload(body)
                        break
            except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
                pass  # addr collection is best-effort

        except (asyncio.TimeoutError, asyncio.IncompleteReadError, ConnectionError):
            pass

        info["discovered_peers"] = addrs
        info["filter_verified"] = filter_verified
        info["bloom_verified"] = bloom_verified
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

    # DNS top-up if queue is small
    peers = await storage.get_uncrawled_peers(limit=config.crawl_max_peers)
    if len(peers) < 50:
        dns_peers = await resolve_seeds(config.dns_seeds, config.dgb_port)
        await storage.add_crawl_peers(dns_peers)
        peers = await storage.get_uncrawled_peers(limit=config.crawl_max_peers)

    # Per-capability snapshots taken before workers run. Static peers from
    # config also join the priority pool until they validate, so operator-declared
    # peers are crawled every cycle even when they pre-existed in the queue with
    # a recent last_crawled timestamp from earlier organic discovery.
    known_bloom  = await storage.get_validated_peer_set(capability="bloom")
    known_filter = await storage.get_validated_peer_set(capability="filter")
    static_set   = {(p["ip"], p["port"]) for p in config.static_peers}
    priority     = known_bloom | known_filter | static_set

    # Priority peers always get crawled this cycle, taking budget from the queue.
    budget = max(0, config.crawl_max_peers - len(priority))
    normal = await storage.get_uncrawled_peers(limit=budget) if budget > 0 else []
    peers  = list(priority) + [p for p in normal if p not in priority]

    bloom_found = 0
    filter_found = 0
    total_checked = 0
    new_peers_discovered = 0
    sem = asyncio.Semaphore(config.crawl_concurrency)

    async def check_peer(ip: str, port: int):
        nonlocal bloom_found, filter_found, total_checked, new_peers_discovered
        async with sem:
            await storage.mark_crawled(ip, port)
            result = await handshake_peer(
                ip, port, config.dgb_magic, config.crawl_timeout
            )
            total_checked += 1

            ts = int(time.time())
            bloom_verified  = bool(result and result.get("bloom_verified"))
            filter_verified = bool(result and result.get("filter_verified"))

            # Per-capability attempt-logging gates.
            if (ip, port) in known_bloom or bloom_verified:
                await storage.record_attempt(
                    ip, port, capability="bloom",
                    success=bloom_verified, ts=ts,
                )
            if (ip, port) in known_filter or filter_verified:
                await storage.record_attempt(
                    ip, port, capability="filter",
                    success=filter_verified, ts=ts,
                )

            if result is None:
                return

            # Explicit-downgrade detection. If a previously-validated peer's
            # current handshake no longer advertises the capability bit, clear
            # its validation timestamp so it drops from the per-capability API
            # list on the next call — rather than waiting for uptime_score to
            # decay below threshold via the failure-attempt path.
            advertised_services = result["services"]
            if (ip, port) in known_bloom and not (advertised_services & NODE_BLOOM):
                await storage.clear_validation(ip, port, capability="bloom")
                log.info("BLOOM DOWNGRADED: %s:%d cleared validation (services=0x%x)",
                         ip, port, advertised_services)
            if (ip, port) in known_filter and not (advertised_services & NODE_COMPACT_FILTERS):
                await storage.clear_validation(ip, port, capability="filter")
                log.info("FILTER DOWNGRADED: %s:%d cleared validation (services=0x%x)",
                         ip, port, advertised_services)

            # Upsert per capability that just verified.
            if bloom_verified:
                bloom_found += 1
                await storage.upsert_bloom_peer(
                    ip, port, result["services"],
                    result["protocol_version"],
                    result["user_agent"],
                    ts,
                )
                log.info("BLOOM VERIFIED: %s:%d %s (services=0x%02x)",
                         ip, port, result["user_agent"], result["services"])

            if filter_verified:
                filter_found += 1
                await storage.upsert_filter_peer(
                    ip, port, result["services"],
                    result["protocol_version"],
                    result["user_agent"],
                    ts,
                )
                log.info("FILTER VERIFIED: %s:%d %s (services=0x%02x)",
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

    pruned = await storage.prune(max_age_hours=config.prune_hours)
    pruned_attempts = await storage.prune_attempts(window_days=config.ranking_window_days)

    elapsed = time.time() - start
    stats = {
        "checked": total_checked,
        "bloom_found": bloom_found,
        "filter_found": filter_found,
        "new_peers": new_peers_discovered,
        "pruned": pruned,
        "pruned_attempts": pruned_attempts,
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
