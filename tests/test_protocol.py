# tests/test_protocol.py
import struct
from seeder.protocol import (
    DGB_MAGIC, NODE_BLOOM, NODE_NETWORK, NODE_WITNESS,
    make_message, parse_message_header, build_version_payload,
    parse_version_payload, build_verack, build_getaddr,
    parse_addr_payload, encode_varint, decode_varint,
    net_addr,
)


def test_encode_varint_small():
    assert encode_varint(0) == b"\x00"
    assert encode_varint(252) == b"\xfc"


def test_encode_varint_16bit():
    assert encode_varint(253) == b"\xfd\xfd\x00"
    assert encode_varint(0xFFFF) == b"\xfd\xff\xff"


def test_encode_varint_32bit():
    assert encode_varint(0x10000) == b"\xfe\x00\x00\x01\x00"


def test_decode_varint_small():
    val, size = decode_varint(b"\x42rest")
    assert val == 0x42
    assert size == 1


def test_decode_varint_16bit():
    val, size = decode_varint(b"\xfd\x01\x01")
    assert val == 0x0101
    assert size == 3


def test_make_message_checksum():
    msg = make_message(DGB_MAGIC, "verack", b"")
    # verack has empty payload → checksum of SHA256(SHA256(""))
    assert len(msg) == 24  # 4 magic + 12 cmd + 4 len + 4 checksum
    assert msg[:4] == DGB_MAGIC
    assert msg[4:16] == b"verack\x00\x00\x00\x00\x00\x00"
    assert struct.unpack_from("<I", msg, 16)[0] == 0  # payload len = 0


def test_parse_message_header():
    msg = make_message(DGB_MAGIC, "verack", b"")
    cmd, payload_len, checksum = parse_message_header(msg[:24])
    assert cmd == "verack"
    assert payload_len == 0


def test_build_version_payload():
    payload = build_version_payload(
        protocol_version=70019,
        services=0,
        timestamp=1700000000,
        user_agent="/test:1.0/",
        start_height=0,
        relay=False,
    )
    # Parse back: protocol version at offset 0
    version = struct.unpack_from("<i", payload, 0)[0]
    assert version == 70019
    # Services at offset 4
    services = struct.unpack_from("<Q", payload, 4)[0]
    assert services == 0


def test_parse_version_payload():
    payload = build_version_payload(
        protocol_version=70019,
        services=0x0D,  # NODE_NETWORK | NODE_BLOOM | NODE_WITNESS
        timestamp=1700000000,
        user_agent="/DigiByte:8.26.0/",
        start_height=23000000,
        relay=True,
    )
    info = parse_version_payload(payload)
    assert info["protocol_version"] == 70019
    assert info["services"] == 0x0D
    assert info["user_agent"] == "/DigiByte:8.26.0/"
    assert info["start_height"] == 23000000
    assert info["relay"] is True


def test_parse_version_detects_bloom():
    payload = build_version_payload(
        protocol_version=70019,
        services=NODE_NETWORK | NODE_BLOOM,
        timestamp=1700000000,
        user_agent="/DigiByte:8.26.0/",
        start_height=0,
        relay=False,
    )
    info = parse_version_payload(payload)
    assert info["services"] & NODE_BLOOM != 0


def test_net_addr():
    addr = net_addr(0, "1.2.3.4", 12024)
    assert len(addr) == 26  # 8 services + 16 ip + 2 port


def test_build_verack():
    msg = build_verack(DGB_MAGIC)
    assert msg[:4] == DGB_MAGIC
    cmd, plen, _ = parse_message_header(msg[:24])
    assert cmd == "verack"
    assert plen == 0


def test_build_getaddr():
    msg = build_getaddr(DGB_MAGIC)
    cmd, plen, _ = parse_message_header(msg[:24])
    assert cmd == "getaddr"
    assert plen == 0


def test_parse_addr_payload_empty():
    # varint 0 = no addresses
    peers = parse_addr_payload(b"\x00")
    assert peers == []


def test_parse_addr_payload_one_peer():
    # Build a single addr entry: 4 bytes timestamp + 26 bytes net_addr
    ts = struct.pack("<I", 1700000000)
    addr = net_addr(NODE_NETWORK | NODE_BLOOM, "8.8.8.8", 12024)
    payload = encode_varint(1) + ts + addr
    peers = parse_addr_payload(payload)
    assert len(peers) == 1
    assert peers[0]["ip"] == "8.8.8.8"
    assert peers[0]["port"] == 12024
    assert peers[0]["services"] & NODE_BLOOM != 0


def test_node_compact_filters_constant():
    from seeder.protocol import NODE_COMPACT_FILTERS
    assert NODE_COMPACT_FILTERS == 0x40


def test_build_getcfheaders_default():
    from seeder.protocol import build_getcfheaders, parse_message_header, DGB_MAGIC
    msg = build_getcfheaders(DGB_MAGIC)
    cmd, plen, _ = parse_message_header(msg[:24])
    assert cmd == "getcfheaders"
    # Payload: 1 byte filter_type + 4 bytes start_height + 32 bytes stop_hash = 37 bytes
    assert plen == 37
    payload = msg[24:]
    assert payload[0] == 0   # filter_type = 0 (basic)
    # start_height default = 1, little-endian uint32
    assert struct.unpack_from("<I", payload, 1)[0] == 1
    # stop_hash default = all zeros
    assert payload[5:37] == b"\x00" * 32


def test_build_getcfheaders_explicit_args():
    from seeder.protocol import build_getcfheaders, DGB_MAGIC
    stop = bytes(range(32))
    msg = build_getcfheaders(DGB_MAGIC, filter_type=0, start_height=12345, stop_hash=stop)
    payload = msg[24:]
    assert payload[0] == 0
    assert struct.unpack_from("<I", payload, 1)[0] == 12345
    assert payload[5:37] == stop
