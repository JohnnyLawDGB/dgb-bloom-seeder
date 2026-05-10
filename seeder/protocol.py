# seeder/protocol.py
"""DigiByte P2P protocol message encoding and decoding."""

import hashlib
import struct
import socket
import os

# Service flags
NODE_NETWORK = 0x01
NODE_BLOOM = 0x04
NODE_WITNESS = 0x08
NODE_COMPACT_FILTERS = 0x40

# DigiByte mainnet magic
DGB_MAGIC = b"\xfa\xc3\xb6\xda"

# Message header: 4 magic + 12 command + 4 length + 4 checksum = 24 bytes
HEADER_SIZE = 24


def _checksum(payload: bytes) -> bytes:
    """Double SHA-256 checksum (first 4 bytes)."""
    return hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]


def encode_varint(n: int) -> bytes:
    if n < 0xFD:
        return struct.pack("<B", n)
    elif n <= 0xFFFF:
        return b"\xfd" + struct.pack("<H", n)
    elif n <= 0xFFFFFFFF:
        return b"\xfe" + struct.pack("<I", n)
    else:
        return b"\xff" + struct.pack("<Q", n)


def decode_varint(data: bytes) -> tuple[int, int]:
    """Returns (value, bytes_consumed)."""
    first = data[0]
    if first < 0xFD:
        return first, 1
    elif first == 0xFD:
        return struct.unpack_from("<H", data, 1)[0], 3
    elif first == 0xFE:
        return struct.unpack_from("<I", data, 1)[0], 5
    else:
        return struct.unpack_from("<Q", data, 1)[0], 9


def net_addr(services: int, ip: str, port: int) -> bytes:
    """Encode a network address (without timestamp) — 26 bytes."""
    addr = struct.pack("<Q", services)
    # IPv4-mapped IPv6: 10 bytes 0x00, 2 bytes 0xFF, 4 bytes IPv4
    addr += b"\x00" * 10 + b"\xff\xff"
    addr += socket.inet_aton(ip)
    addr += struct.pack(">H", port)  # port is big-endian
    return addr


def make_message(magic: bytes, command: str, payload: bytes) -> bytes:
    """Build a complete P2P message with header."""
    cmd = command.encode("ascii").ljust(12, b"\x00")
    length = struct.pack("<I", len(payload))
    cs = _checksum(payload)
    return magic + cmd + length + cs + payload


def parse_message_header(header: bytes) -> tuple[str, int, bytes]:
    """Parse a 24-byte message header. Returns (command, payload_length, checksum)."""
    cmd = header[4:16].rstrip(b"\x00").decode("ascii")
    payload_len = struct.unpack_from("<I", header, 16)[0]
    checksum = header[20:24]
    return cmd, payload_len, checksum


def build_version_payload(
    protocol_version: int = 70019,
    services: int = 0,
    timestamp: int = 0,
    user_agent: str = "/DGB-Bloom-Seeder:1.0/",
    start_height: int = 0,
    relay: bool = False,
) -> bytes:
    """Build the payload for a version message."""
    payload = struct.pack("<i", protocol_version)
    payload += struct.pack("<Q", services)
    payload += struct.pack("<q", timestamp)
    payload += net_addr(0, "0.0.0.0", 0)  # addr_recv
    payload += net_addr(services, "0.0.0.0", 0)  # addr_from
    payload += struct.pack("<Q", int.from_bytes(os.urandom(8), "little"))  # nonce
    ua_bytes = user_agent.encode("utf-8")
    payload += encode_varint(len(ua_bytes)) + ua_bytes
    payload += struct.pack("<i", start_height)
    payload += struct.pack("<?", relay)
    return payload


def parse_version_payload(payload: bytes) -> dict:
    """Parse a version message payload. Returns dict with key fields."""
    offset = 0
    protocol_version = struct.unpack_from("<i", payload, offset)[0]
    offset += 4
    services = struct.unpack_from("<Q", payload, offset)[0]
    offset += 8
    timestamp = struct.unpack_from("<q", payload, offset)[0]
    offset += 8
    offset += 26  # addr_recv
    offset += 26  # addr_from
    offset += 8   # nonce

    ua_len, varint_size = decode_varint(payload[offset:])
    offset += varint_size
    user_agent = payload[offset:offset + ua_len].decode("utf-8", errors="replace")
    offset += ua_len

    start_height = struct.unpack_from("<i", payload, offset)[0]
    offset += 4

    relay = bool(payload[offset]) if offset < len(payload) else True

    return {
        "protocol_version": protocol_version,
        "services": services,
        "timestamp": timestamp,
        "user_agent": user_agent,
        "start_height": start_height,
        "relay": relay,
    }


def build_verack(magic: bytes) -> bytes:
    return make_message(magic, "verack", b"")


def build_getaddr(magic: bytes) -> bytes:
    return make_message(magic, "getaddr", b"")


def build_getcfheaders(
    magic: bytes,
    filter_type: int = 0,
    start_height: int = 1,
    stop_hash: bytes = b"\x00" * 32,
) -> bytes:
    """Build a getcfheaders message (BIP 157) for validating compact-filter support.

    Default stop_hash is all zeros — not a valid block hash, but non-supporting peers
    disconnect on getcfheaders regardless of payload validity. Supporting peers respond
    (cfheaders/notfound) or briefly hold the connection."""
    payload = struct.pack("<B", filter_type)
    payload += struct.pack("<I", start_height)
    if len(stop_hash) != 32:
        raise ValueError("stop_hash must be 32 bytes")
    payload += stop_hash
    return make_message(magic, "getcfheaders", payload)


def build_filterload(magic: bytes) -> bytes:
    """Build a minimal bloom filter (filterload) to test if a peer actually
    accepts bloom connections. The filter is tiny (8 bytes, 1 hash function)
    with a single test element — just enough to trigger a rejection from
    nodes that advertise NODE_BLOOM but have peerbloomfilters=0."""
    # BIP37 filterload payload:
    #   [varint] filter_size
    #   [bytes]  filter_data
    #   [u32]    nHashFuncs
    #   [u32]    nTweak
    #   [u8]     nFlags (BLOOM_UPDATE_NONE = 0)
    filter_data = b"\x00" * 8  # 8 bytes, all zeros
    payload = encode_varint(len(filter_data))
    payload += filter_data
    payload += struct.pack("<I", 1)   # 1 hash function
    payload += struct.pack("<I", 0)   # nTweak = 0
    payload += struct.pack("<B", 0)   # BLOOM_UPDATE_NONE
    return make_message(magic, "filterload", payload)


def parse_addr_payload(payload: bytes) -> list[dict]:
    """Parse an addr message payload. Returns list of peer dicts."""
    if not payload:
        return []
    count, offset = decode_varint(payload)
    peers = []
    for _ in range(count):
        if offset + 30 > len(payload):
            break
        # 4 bytes timestamp + 26 bytes net_addr
        offset += 4  # skip timestamp
        services = struct.unpack_from("<Q", payload, offset)[0]
        offset += 8
        # IPv6 address: 16 bytes. Last 4 are IPv4 if starts with ffff
        ip_bytes = payload[offset:offset + 16]
        offset += 16
        port = struct.unpack_from(">H", payload, offset)[0]
        offset += 2

        # Extract IPv4 from IPv4-mapped IPv6
        if ip_bytes[:12] == b"\x00" * 10 + b"\xff\xff":
            ip = socket.inet_ntoa(ip_bytes[12:16])
        else:
            continue  # skip IPv6 peers for now

        # Skip private/reserved IPs
        if ip.startswith("0.") or ip.startswith("127.") or ip.startswith("10.") or ip.startswith("192.168."):
            continue

        peers.append({"ip": ip, "port": port, "services": services})
    return peers
