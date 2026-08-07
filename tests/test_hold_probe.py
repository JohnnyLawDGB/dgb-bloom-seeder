"""Hold-and-see probe: does it tell EVICTION apart from UNREACHABLE?

The whole value of this probe is one distinction. A node at maxconnections accepts a
connection and then drops somebody -- so "connected successfully" and "session survived"
are different facts, and only the second predicts whether a wallet can sync. If the probe
cannot tell a post-handshake drop (eviction) from a failed connect (unreachable), it adds
a metric that looks like saturation data and is not, which is worse than having none:
peer_attempts already measures reachability.

Each case drives the REAL hold_probe against a real asyncio server speaking the real
handshake. Nothing is mocked.

HARNESS NOTE (learned the hard way): fake-peer handlers must be RELEASABLE. An earlier
version parked them in `await asyncio.sleep(10)` and closed the server with
`async with server:`. Since Python 3.12 `Server.wait_closed()` blocks until every handler
task has finished, so teardown sat waiting on sleeps that the assertions no longer cared
about -- the suite wedged with no output long after the tests themselves had passed. Here
handlers park on an Event that teardown sets, and lingering tasks are cancelled outright.
"""

import asyncio
import time

import pytest

from seeder.crawler import hold_probe
from seeder.protocol import (
    HEADER_SIZE, make_message, parse_message_header, build_version_payload,
    build_verack,
)

MAGIC = b"\xfa\xc3\xb6\xda"


async def _read_one(reader, timeout=5):
    header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=timeout)
    cmd, plen, _ = parse_message_header(header)
    body = await asyncio.wait_for(reader.readexactly(plen), timeout=timeout) if plen else b""
    return cmd, body


async def _do_handshake(reader, writer):
    """Accept their version, reply version+verack -- the minimum hold_probe waits for."""
    await _read_one(reader)                                   # their version
    writer.write(make_message(MAGIC, "version", build_version_payload(
        timestamp=int(time.time()), user_agent="/FakePeer:1.0/")))
    writer.write(build_verack(MAGIC))
    await writer.drain()


class FakePeer:
    """A server whose handlers can always be released, so teardown is bounded."""

    def __init__(self, handler):
        self._handler = handler
        self.release = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._server = None
        self.port = None

    async def __aenter__(self):
        async def wrap(reader, writer):
            self._tasks.append(asyncio.current_task())
            try:
                await self._handler(reader, writer, self.release)
            except (asyncio.CancelledError, ConnectionError, OSError):
                pass
            finally:
                try:
                    writer.close()
                except Exception:
                    pass

        self._server = await asyncio.start_server(wrap, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc):
        self.release.set()                 # let parked handlers return
        self._server.close()               # stop accepting; do NOT wait_closed()
        for t in self._tasks:              # and never wait on a handler
            if not t.done():
                t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_peer_that_drops_after_handshake_reads_as_evicted():
    """THE CASE THIS EXISTS FOR: handshake succeeds, then the peer closes on us."""
    async def handler(reader, writer, release):
        await _do_handshake(reader, writer)
        writer.close()                                        # evict

    async with FakePeer(handler) as peer:
        result = await hold_probe("127.0.0.1", peer.port, MAGIC, hold_secs=5, timeout=3)
    assert result is False, "a post-handshake close must read as DROPPED, not survived"


@pytest.mark.asyncio
async def test_peer_that_keeps_us_reads_as_survived():
    async def handler(reader, writer, release):
        await _do_handshake(reader, writer)
        await release.wait()                                  # hold us until teardown

    async with FakePeer(handler) as peer:
        start = time.monotonic()
        result = await hold_probe("127.0.0.1", peer.port, MAGIC, hold_secs=2, timeout=3)
        elapsed = time.monotonic() - start
    assert result is True
    assert elapsed >= 1.8, "must actually HOLD the window, not return early (%.2fs)" % elapsed


@pytest.mark.asyncio
async def test_unreachable_is_none_not_false():
    """A refused connect is a REACHABILITY failure. Recording it as survived=False would
    conflate 'node is down' with 'node evicted us' and poison the saturation signal."""
    async def handler(reader, writer, release):
        await release.wait()

    async with FakePeer(handler) as peer:
        port = peer.port
        peer._server.close()                                  # nothing listening now
        await asyncio.sleep(0)
        result = await hold_probe("127.0.0.1", port, MAGIC, hold_secs=2, timeout=1)
    assert result is None, "unreachable must be None (no datapoint), never False"


@pytest.mark.asyncio
async def test_connect_without_handshake_is_none():
    """Accepts TCP but never speaks. Not an eviction -- we were never a peer."""
    async def handler(reader, writer, release):
        await release.wait()                                  # silent

    async with FakePeer(handler) as peer:
        result = await hold_probe("127.0.0.1", peer.port, MAGIC, hold_secs=2, timeout=1)
    assert result is None, "a handshake that never completes is not a survival datapoint"


@pytest.mark.asyncio
async def test_ping_is_answered_so_a_long_hold_is_not_our_own_rudeness():
    """If the probe ignored ping, the peer would disconnect us for being unresponsive and
    every long hold would read as an eviction. This server DEMANDS a pong and drops the
    connection if it does not get one."""
    got_pong = asyncio.Event()

    nonce = b"\x01\x02\x03\x04\x05\x06\x07\x08"

    async def handler(reader, writer, release):
        await _do_handshake(reader, writer)
        writer.write(make_message(MAGIC, "ping", nonce))
        await writer.drain()
        # DRAIN TO THE PONG, skipping anything else. A real peer does this; the first
        # version of this test read exactly ONE message and judged on it, which caught
        # the probe's own verack (sent in reply to our version) rather than the pong,
        # declared "no pong", and dropped the connection -- a FALSE failure against
        # correct probe code. The assertion below is only meaningful if the server gives
        # the probe an honest chance to answer.
        deadline = time.monotonic() + 4
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                cmd, body = await _read_one(reader, timeout=remaining)
                if cmd == "pong" and body[:8] == nonce:
                    got_pong.set()
                    await release.wait()                      # rewarded: keep them
                    return
        except Exception:
            pass
        writer.close()                                        # no pong -> drop them

    async with FakePeer(handler) as peer:
        result = await hold_probe("127.0.0.1", peer.port, MAGIC, hold_secs=3, timeout=3)
    assert got_pong.is_set(), "hold_probe must answer ping, or it measures its own rudeness"
    assert result is True
