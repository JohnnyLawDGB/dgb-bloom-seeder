# seeder/crawler.py
"""Network crawler — connects to peers, performs P2P handshake, discovers compact-filter-capable nodes."""

import asyncio
import logging
import socket
import time

from seeder.config import Config
from seeder.protocol import (
    HEADER_SIZE, NODE_COMPACT_FILTERS,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr,
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

        # Try to read their verack, then verify compact-filter support
        addrs = []
        filter_verified = False
        try:
            # Read verack
            header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=2)
            cmd, plen, _ = parse_message_header(header)
            if plen > 0:
                await asyncio.wait_for(reader.readexactly(plen), timeout=2)

            # If peer advertises NODE_COMPACT_FILTERS, verify with a getcfheaders round-trip.
            # A peer that doesn't actually support BIP 157 will disconnect on this message.
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


HOLD_PROBE_UA = "/DGB-Bloom-Seeder-Hold:1.0/"


async def hold_probe(ip: str, port: int, magic: bytes, hold_secs: int,
                     timeout: int = 5) -> "bool | None":
    """Connect, handshake, then HOLD the connection and see whether the peer keeps us.

    This measures the one thing uptime_score structurally cannot: SATURATION. A node at
    maxconnections still accepts a probe -- it evicts some other peer to make room -- so
    a successful connect says nothing about whether a wallet session would survive. Core
    protects the longest-connected and best-ping peers and evicts the YOUNGEST, which on
    a busy node is always our wallet.

    Returns:
        True  -- still connected after hold_secs (the node has room for us)
        False -- the PEER closed on us inside the window (evicted / dropped)
        None  -- never completed a handshake. NOT a survival datapoint: that is a
                 reachability failure and peer_attempts already measures it. Recording
                 it as survived=False would conflate "unreachable" with "evicted".
    """
    handshaked = False
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=timeout
        )
    except (OSError, asyncio.TimeoutError):
        return None

    try:
        writer.write(make_message(magic, "version", build_version_payload(
            timestamp=int(time.time()), user_agent=HOLD_PROBE_UA)))
        await writer.drain()

        got_version = got_verack = False
        hs_deadline = time.monotonic() + timeout
        while not (got_version and got_verack):
            if time.monotonic() >= hs_deadline:
                return None
            header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=timeout)
            cmd, plen, _ = parse_message_header(header)
            body = await asyncio.wait_for(reader.readexactly(plen), timeout=timeout) if plen else b""
            if cmd == "version":
                got_version = True
                writer.write(build_verack(magic))
                await writer.drain()
            elif cmd == "verack":
                got_verack = True

        handshaked = True

        # ---- THE HOLD ----
        end = time.monotonic() + hold_secs
        while True:
            remaining = end - time.monotonic()
            if remaining <= 0:
                return True                      # outlasted the window, still open
            try:
                header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE),
                                                timeout=remaining)
            except asyncio.TimeoutError:
                return True                      # quiet, but still connected
            cmd, plen, _ = parse_message_header(header)
            body = b""
            if plen:
                body = await asyncio.wait_for(
                    reader.readexactly(plen),
                    timeout=max(1.0, end - time.monotonic()))
            if cmd == "ping":
                # MUST answer. An unanswered ping gets us dropped for OUR rudeness,
                # which is indistinguishable from eviction and would make every long
                # hold read as a drop.
                writer.write(make_message(magic, "pong", body[:8]))
                await writer.drain()

    except (asyncio.IncompleteReadError, ConnectionError, OSError, asyncio.TimeoutError):
        # After a completed handshake, a close/reset IS the signal we came for.
        return False if handshaked else None
    except Exception as e:
        log.debug("hold_probe error %s:%d: %s", ip, port, e)
        return False if handshaked else None
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def hold_probe_cycle(config: Config, storage: Storage) -> dict:
    """Hold-probe the currently ranked filter peers. No-op unless explicitly enabled.

    Scope is deliberately the RANKED set, not the crawl queue. This is expensive in a way
    a handshake is not: every held socket occupies a slot on a node that may be short of
    them, and on a saturated node our connect costs some other peer theirs. Probing the
    ~20 peers we actually hand to wallets IS the question; probing thousands would make
    us part of the problem we are trying to measure.
    """
    if not config.hold_probe_enabled:
        return {"enabled": False}

    peers = await storage.get_ranked_peers(
        window_days=config.ranking_window_days,
        prior_attempts=config.ranking_prior_attempts,
        prior_successes=config.ranking_prior_successes,
        longevity_cap_days=config.ranking_longevity_cap_days,
        longevity_weight=config.ranking_longevity_weight,
        inclusion_threshold=config.ranking_inclusion_threshold,
        max_age_hours=config.api_max_age_hours,
        limit=config.api_max_results,
    )
    sem = asyncio.Semaphore(config.hold_probe_concurrency)
    survived = dropped = skipped = 0

    async def one(ip, port):
        nonlocal survived, dropped, skipped
        async with sem:
            r = await hold_probe(ip, port, config.dgb_magic,
                                 config.hold_probe_seconds, config.hold_probe_timeout)
        if r is None:
            skipped += 1
            return
        await storage.record_hold(ip, port, survived=r,
                                  hold_secs=config.hold_probe_seconds,
                                  ts=int(time.time()))
        if r:
            survived += 1
        else:
            dropped += 1
            log.info("HOLD DROPPED: %s:%d closed on us inside %ds -- saturation signal",
                     ip, port, config.hold_probe_seconds)

    await asyncio.gather(*[one(pr["ip"], pr["port"]) for pr in peers])
    stats = {"enabled": True, "probed": len(peers), "survived": survived,
             "dropped": dropped, "no_handshake": skipped}
    log.info("Hold probe complete: %s", stats)
    return stats


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

    # Filter-validated + static peers form the priority set, crawled every cycle.
    # Static peers from config join the priority pool until they validate, so
    # operator-declared peers are crawled every cycle even when they pre-existed
    # in the queue with a recent last_crawled timestamp from earlier organic
    # discovery.
    known_filter = await storage.get_validated_peer_set()
    static_set   = {(p["ip"], p["port"]) for p in config.static_peers}
    priority     = known_filter | static_set

    # Priority peers always get crawled this cycle, taking budget from the queue.
    budget = max(0, config.crawl_max_peers - len(priority))
    normal = await storage.get_uncrawled_peers(limit=budget) if budget > 0 else []
    peers  = list(priority) + [p for p in normal if p not in priority]

    filter_found = 0
    total_checked = 0
    new_peers_discovered = 0
    sem = asyncio.Semaphore(config.crawl_concurrency)

    async def check_peer(ip: str, port: int):
        nonlocal filter_found, total_checked, new_peers_discovered
        async with sem:
            await storage.mark_crawled(ip, port)
            result = await handshake_peer(
                ip, port, config.dgb_magic, config.crawl_timeout
            )
            total_checked += 1

            ts = int(time.time())
            filter_verified = bool(result and result.get("filter_verified"))

            # Filter attempt-logging gate.
            if (ip, port) in known_filter or filter_verified:
                await storage.record_attempt(
                    ip, port, success=filter_verified, ts=ts,
                )

            if result is None:
                return

            # Explicit-downgrade detection. If a previously-validated peer's
            # current handshake no longer advertises the compact-filter bit,
            # clear its validation timestamp so it drops from the filter API
            # list on the next call — rather than waiting for uptime_score to
            # decay below threshold via the failure-attempt path.
            advertised_services = result["services"]
            if (ip, port) in known_filter and not (advertised_services & NODE_COMPACT_FILTERS):
                await storage.clear_validation(ip, port)
                log.info("FILTER DOWNGRADED: %s:%d cleared validation (services=0x%x)",
                         ip, port, advertised_services)

            # Upsert if the filter capability just verified.
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
        # AFTER the crawl, so it probes the set the crawl just validated, and in its own
        # try/except so a probe failure can never cost us a crawl cycle. No-op unless
        # hold_probe_enabled.
        try:
            await hold_probe_cycle(config, storage)
        except Exception:
            log.exception("Hold probe cycle failed")
        await asyncio.sleep(config.crawl_interval)
