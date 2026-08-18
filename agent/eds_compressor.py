#!/usr/bin/env python3
"""
eds_compressor.py — Semantic compressor on the router side (middlebox).

Runs INSIDE the router container, intercepts the UDP packets forwarded
towards the destination via iptables NFQUEUE, applies the payload reduction
in proportion to the current congestion state and to the traffic class
(TOS/DSCP), and re-injects the modified packets into the kernel.

Architecture faithful to eFRAC (Abate, Sacco, Fiore, Esposito):
compression happens at the MIDDLEBOX (router), not at the sources.
The packets on the bottleneck link actually have reduced bytes.

Dependencies (installed by Net.start_compressor via deploy):
    apk add libnetfilter_queue libmnl
    pip3 install NetfilterQueue

Current state: /tmp/eds_comp_state  (written by the control-plane, integer 0..4)

Usage:
    python3 eds_compressor.py [queue_num]   # default queue_num=1
"""
from __future__ import annotations
import os
import signal
import struct
import sys

# ── Application header (eds_node.py) ──────────────────────────────────────────
# flow_id (uint32) + seq (uint32) + send_ts (double) = 16 bytes  big-endian
APP_HDR_FMT = "!IId"
APP_HDR_SIZE = struct.calcsize(APP_HDR_FMT)   # 16

# ── Ratio table (compressed/original) ─────────────────────────────────────────
# Identical to simulator/control/compressor.py _RATIOS
# (state_value, priority) → ratio
_RATIOS: dict[tuple[int, int], float] = {
    # NORMAL — no compression
    (0, 0): 1.00, (0, 1): 1.00, (0, 2): 1.00,
    # HEADER_COMPRESSION — fixed 24 B saving (28 B→4 B IP/UDP header)
    (1, 0): 0.760,   # (100-24)/100  CONTROL
    (1, 1): 0.904,   # (250-24)/250  TELEMETRY
    (1, 2): 0.983,   # (1450-24)/1450 VIDEO
    # DELTA_COMPRESSION — HC + XOR + zlib, paper Table 1: ~1.5×
    (2, 0): 0.550,
    (2, 1): 0.500,
    (2, 2): 0.667,
    # INCREMENTAL_COMPRESSION — HC + semantic field-diff, paper: 6.1× avg on CoT XML
    # VIDEO (binary) → falls back to Delta ratio (semantic parser not applicable)
    (3, 0): 0.500,
    (3, 1): 0.250,   # structured TELEMETRY → ~4×
    (3, 2): 0.667,   # VIDEO → same as Delta
    # DROP_LOW_PRIORITY — priority>0 already dropped by tc filter before reaching here
    # priority=0 (CONTROL) survives with INCREMENTAL ratio
    (4, 0): 0.500,
    (4, 1): 1.000,   # never reached (tc filter DROP active)
    (4, 2): 1.000,   # never reached
}

# TOS (DSCP byte, mask 0xFC) → priority level
# Mirror of eds_emulator.py: VIDEO=0x28, TELEMETRY=0x40, CONTROL=0xc0
_TOS_TO_PRIORITY: dict[int, int] = {0xc0: 0, 0x40: 1, 0x28: 2}

STATE_FILE = "/tmp/eds_comp_state"
STATS_FILE = "/tmp/eds_comp_stats"   # diagnostic counters (pkts/bytes_in/out/compressed)


def _read_state() -> int:
    """Reads the current congestion state (0-4) from the shared file."""
    try:
        return max(0, min(4, int(open(STATE_FILE).read().strip())))
    except Exception:
        return 0  # NORMAL if the file has not been written yet


def _ip_checksum(header: bytes) -> int:
    """RFC 791: 16-bit one's complement sum of the IP header."""
    if len(header) % 2:
        header += b"\x00"
    words = struct.unpack("!" + "H" * (len(header) // 2), header)
    total = sum(words)
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF


def _compress(raw: bytes) -> bytes:
    """
    Truncates the UDP payload to the ratio corresponding to (state, traffic_class).

    Guarantees that the first APP_HDR_SIZE bytes of the payload (application
    header flow_id+seq+ts) survive intact; the padding is reduced.
    Updates IP total_len, UDP length and recomputes the IP checksum.
    UDP checksum set to 0 (optional in IPv4, RFC 768).
    """
    # Minimum: IP(20) + UDP(8) + app_header(16) = 44 bytes
    if len(raw) < 44:
        return raw

    ihl = (raw[0] & 0x0F) * 4
    if raw[9] != 17:      # not UDP — passed through unchanged
        return raw
    if len(raw) < ihl + 8:
        return raw

    tos_masked = raw[1] & 0xFC
    priority = _TOS_TO_PRIORITY.get(tos_masked, 1)  # default TELEMETRY
    state = _read_state()
    ratio = _RATIOS.get((state, priority), 1.0)

    payload_offset = ihl + 8
    original_payload = raw[payload_offset:]
    n_orig = len(original_payload)

    # Protection: the app header must survive
    if n_orig <= APP_HDR_SIZE:
        return raw

    n_new = max(APP_HDR_SIZE, int(n_orig * ratio))
    if n_new >= n_orig:
        return raw   # NORMAL state or ratio=1.0 → no change

    # Packet reconstruction
    new_payload = original_payload[:n_new]
    new_udp_len = 8 + n_new
    new_ip_total = ihl + new_udp_len

    # IP header: update total_len (offset 2), zero the checksum (offset 10)
    ip_hdr = bytearray(raw[:ihl])
    struct.pack_into("!H", ip_hdr, 2, new_ip_total)
    struct.pack_into("!H", ip_hdr, 10, 0)
    struct.pack_into("!H", ip_hdr, 10, _ip_checksum(bytes(ip_hdr)))

    # UDP header: update length (offset +4), disable checksum (offset +6)
    udp_hdr = bytearray(raw[ihl:ihl + 8])
    struct.pack_into("!H", udp_hdr, 4, new_udp_len)
    struct.pack_into("!H", udp_hdr, 6, 0)

    return bytes(ip_hdr) + bytes(udp_hdr) + new_payload


def main():
    queue_num = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    try:
        from netfilterqueue import NetfilterQueue  # type: ignore
    except ImportError:
        sys.stderr.write(
            "[compressor] NetfilterQueue not found.\n"
            "  Install: apk add libnetfilter_queue libmnl && "
            "pip3 install NetfilterQueue\n"
        )
        sys.exit(1)

    # Diagnostic counters (scenario 1/5: understand whether compression bites).
    #   pkts       = packets actually processed in userspace
    #   bytes_in   = total IP bytes received (pre-compression)
    #   bytes_out  = total IP bytes after compression
    #   compressed = packets whose payload was actually truncated
    # Comparing pkts with the iptables rule counter (control-plane side) measures
    # the BYPASS (--queue-bypass) under load; bytes_in/bytes_out gives the REAL
    # on-the-wire ratio (independent of the class mix).
    stats = {"pkts": 0, "bytes_in": 0, "bytes_out": 0, "compressed": 0}

    def _flush_stats():
        try:
            with open(STATS_FILE, "w") as fh:
                fh.write("{pkts} {bytes_in} {bytes_out} {compressed}".format(**stats))
        except OSError:
            pass

    def _callback(nfpkt):
        try:
            raw = nfpkt.get_payload()
            compressed = _compress(raw)
            nfpkt.set_payload(compressed)
            stats["pkts"] += 1
            stats["bytes_in"] += len(raw)
            stats["bytes_out"] += len(compressed)
            if len(compressed) < len(raw):
                stats["compressed"] += 1
            if stats["pkts"] % 200 == 0:   # periodic flush (little I/O)
                _flush_stats()
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[compressor] packet error: {exc}\n")
        nfpkt.accept()

    def _on_term(*_a):
        _flush_stats()
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _on_term)

    nfq = NetfilterQueue()
    nfq.bind(queue_num, _callback)
    print(
        f"[compressor] NFQUEUE {queue_num} active — "
        f"state from {STATE_FILE}  (PID {os.getpid()})",
        flush=True,
    )
    try:
        nfq.run()
    except KeyboardInterrupt:
        pass
    finally:
        _flush_stats()
        nfq.unbind()
        print(f"[compressor] stopped. pkts={stats['pkts']} "
              f"compressed={stats['compressed']} "
              f"bytes_in={stats['bytes_in']} bytes_out={stats['bytes_out']}",
              flush=True)


if __name__ == "__main__":
    main()
