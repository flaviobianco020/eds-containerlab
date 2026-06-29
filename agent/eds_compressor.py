#!/usr/bin/env python3
"""
eds_compressor.py — Compressore semantico lato router (middlebox).

Gira DENTRO il container router, intercetta i pacchetti UDP inoltrati
verso la destinazione tramite iptables NFQUEUE, applica la riduzione del
payload in proporzione allo stato di congestione corrente e alla classe
di traffico (TOS/DSCP), e re-inietta i pacchetti modificati nel kernel.

Architettura fedele a eFRAC (Abate, Sacco, Fiore, Esposito):
la compressione avviene al MIDDLEBOX (router), non alle sorgenti.
I pacchetti sul link bottleneck hanno effettivamente byte ridotti.

Dipendenze (installate da Net.start_compressor via deploy):
    apk add libnetfilter_queue libmnl
    pip3 install NetfilterQueue

Stato corrente: /tmp/eds_comp_state  (scritto dal control-plane, intero 0..4)

Uso:
    python3 eds_compressor.py [queue_num]   # default queue_num=1
"""
from __future__ import annotations
import os
import struct
import sys

# ── Header applicativo (eds_node.py) ─────────────────────────────────────────
# flow_id (uint32) + seq (uint32) + send_ts (double) = 16 bytes  big-endian
APP_HDR_FMT = "!IId"
APP_HDR_SIZE = struct.calcsize(APP_HDR_FMT)   # 16

# ── Tabella ratio (compressed/original) ──────────────────────────────────────
# Identica a simulator/control/compressor.py _RATIOS
# (state_value, priority) → ratio
_RATIOS: dict[tuple[int, int], float] = {
    # NORMAL — nessuna compressione
    (0, 0): 1.00, (0, 1): 1.00, (0, 2): 1.00,
    # HEADER_COMPRESSION — risparmio 24 B fissi (28 B→4 B header IP/UDP)
    (1, 0): 0.760,   # (100-24)/100  CONTROL
    (1, 1): 0.904,   # (250-24)/250  TELEMETRY
    (1, 2): 0.983,   # (1450-24)/1450 VIDEO
    # DELTA_COMPRESSION — HC + XOR + zlib, paper Table 1: ~1.5×
    (2, 0): 0.550,
    (2, 1): 0.500,
    (2, 2): 0.667,
    # INCREMENTAL_COMPRESSION — HC + semantic field-diff, paper: 6.1× avg su CoT XML
    # VIDEO (binario) → fallback a Delta ratio (parser semantico non applicabile)
    (3, 0): 0.500,
    (3, 1): 0.250,   # TELEMETRY strutturato → ~4×
    (3, 2): 0.667,   # VIDEO → stesso di Delta
    # DROP_LOW_PRIORITY — priority>0 già scartati da tc filter prima di arrivare qui
    # priority=0 (CONTROL) sopravvive con ratio INCREMENTAL
    (4, 0): 0.500,
    (4, 1): 1.000,   # mai raggiunto (tc filter DROP attivo)
    (4, 2): 1.000,   # mai raggiunto
}

# TOS (DSCP byte, maschera 0xFC) → priority level
# Specchio di eds_emulator.py: VIDEO=0x28, TELEMETRY=0x40, CONTROL=0xc0
_TOS_TO_PRIORITY: dict[int, int] = {0xc0: 0, 0x40: 1, 0x28: 2}

STATE_FILE = "/tmp/eds_comp_state"


def _read_state() -> int:
    """Legge stato di congestione corrente (0-4) dal file condiviso."""
    try:
        return max(0, min(4, int(open(STATE_FILE).read().strip())))
    except Exception:
        return 0  # NORMAL se file non ancora scritto


def _ip_checksum(header: bytes) -> int:
    """RFC 791: one's complement sum a 16 bit dell'header IP."""
    if len(header) % 2:
        header += b"\x00"
    words = struct.unpack("!" + "H" * (len(header) // 2), header)
    total = sum(words)
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)
    return ~total & 0xFFFF


def _compress(raw: bytes) -> bytes:
    """
    Tronca il payload UDP al ratio corrispondente a (stato, classe_traffico).

    Garantisce che i primi APP_HDR_SIZE byte del payload (header applicativo
    flow_id+seq+ts) sopravvivano intatti; il padding viene ridotto.
    Aggiorna IP total_len, UDP length e ricalcola IP checksum.
    UDP checksum impostato a 0 (opzionale in IPv4, RFC 768).
    """
    # Minimo: IP(20) + UDP(8) + app_header(16) = 44 byte
    if len(raw) < 44:
        return raw

    ihl = (raw[0] & 0x0F) * 4
    if raw[9] != 17:      # non UDP — passa invariato
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

    # Protezione: app header deve sopravvivere
    if n_orig <= APP_HDR_SIZE:
        return raw

    n_new = max(APP_HDR_SIZE, int(n_orig * ratio))
    if n_new >= n_orig:
        return raw   # stato NORMAL o ratio=1.0 → nessuna modifica

    # Ricostruzione pacchetto
    new_payload = original_payload[:n_new]
    new_udp_len = 8 + n_new
    new_ip_total = ihl + new_udp_len

    # IP header: aggiorna total_len (offset 2), azzera checksum (offset 10)
    ip_hdr = bytearray(raw[:ihl])
    struct.pack_into("!H", ip_hdr, 2, new_ip_total)
    struct.pack_into("!H", ip_hdr, 10, 0)
    struct.pack_into("!H", ip_hdr, 10, _ip_checksum(bytes(ip_hdr)))

    # UDP header: aggiorna length (offset +4), disabilita checksum (offset +6)
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
            "[compressor] NetfilterQueue non trovato.\n"
            "  Installa: apk add libnetfilter_queue libmnl && "
            "pip3 install NetfilterQueue\n"
        )
        sys.exit(1)

    def _callback(nfpkt):
        try:
            compressed = _compress(nfpkt.get_payload())
            nfpkt.set_payload(compressed)
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(f"[compressor] errore pacchetto: {exc}\n")
        nfpkt.accept()

    nfq = NetfilterQueue()
    nfq.bind(queue_num, _callback)
    print(
        f"[compressor] NFQUEUE {queue_num} attivo — "
        f"stato da {STATE_FILE}  (PID {os.getpid()})",
        flush=True,
    )
    try:
        nfq.run()
    except KeyboardInterrupt:
        pass
    finally:
        nfq.unbind()
        print("[compressor] terminato.", flush=True)


if __name__ == "__main__":
    main()
