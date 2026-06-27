#!/usr/bin/env python3
"""
eds_node.py - Agente in-container per l'emulatore Event-Driven Simulator (EDS).

Gira DENTRO i container ContainerLab (montato in /opt/eds) e usa solo la
standard library di Python 3. Due modalita':

  recv : ricevitore UDP. Conta pacchetti/byte per flusso e misura la latenza
         end-to-end (i container condividono il clock dell'host, quindi i
         timestamp time.time() sono confrontabili tra sorgente e destinazione).

  send : generatore UDP che riproduce i FlowModel del simulatore
         (CBR / POISSON / BURSTY / PERIODIC_TELEMETRY / VIDEO / CONTROL) con le
         STESSE formule di inter-arrivo di simulator/traffic/flow.py. Marca i
         pacchetti con il DSCP (IP_TOS) corrispondente alla priorita' della
         classe di traffico.

I parametri si passano via variabili d'ambiente (comodo con `docker exec -e`).
Alla terminazione l'agente stampa su stdout una riga JSON con i risultati.
"""
import sys
import os
import socket
import struct
import time
import random
import json
import signal

# Header applicativo: flow_id (uint32), seq (uint32), send_ts (double)
HDR = struct.Struct("!IId")


def _env(name, default=None, cast=str):
    v = os.environ.get(name)
    if v is None:
        return default
    try:
        return cast(v)
    except (TypeError, ValueError):
        return default


def run_recv():
    port = _env("EDS_PORT", 5000, int)
    duration = _env("EDS_DURATION", 60.0, float)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    except OSError:
        pass
    sock.bind(("0.0.0.0", port))
    sock.settimeout(0.5)

    # flow_id -> contatori
    flows = {}
    running = {"on": True}

    def _stop(*_a):
        running["on"] = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    # grace extra per ricevere gli ultimi pacchetti in coda
    stop_at = time.time() + duration + 3.0
    while running["on"] and time.time() < stop_at:
        try:
            data, _addr = sock.recvfrom(2048)
        except socket.timeout:
            continue
        except OSError:
            break
        if len(data) < HDR.size:
            continue
        fid, seq, ts = HDR.unpack(data[:HDR.size])
        now = time.time()
        s = flows.get(fid)
        if s is None:
            s = {"recv": 0, "bytes": 0, "lat_sum": 0.0, "lat_n": 0, "max_seq": -1}
            flows[fid] = s
        s["recv"] += 1
        s["bytes"] += len(data)
        lat = now - ts
        if lat >= 0.0:
            s["lat_sum"] += lat
            s["lat_n"] += 1
        if seq > s["max_seq"]:
            s["max_seq"] = seq

    print(json.dumps({"role": "recv", "flows": flows}), flush=True)


def run_send():
    dst = _env("EDS_DST")
    port = _env("EDS_PORT", 5000, int)
    fid = _env("EDS_FLOW_ID", 0, int)
    model = _env("EDS_MODEL", "cbr")
    rate = _env("EDS_RATE", 100.0, float)        # pacchetti al secondo
    size_lo = _env("EDS_SIZE_LO", 100, int)
    size_hi = _env("EDS_SIZE_HI", 100, int)
    tos = _env("EDS_TOS", 0, int)
    duration = _env("EDS_DURATION", 60.0, float)
    seed = _env("EDS_SEED", 42, int)

    if not dst:
        sys.stderr.write("eds_node.py send: EDS_DST mancante\n")
        sys.exit(2)

    rng = random.Random(seed + fid)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos)
    except OSError:
        pass

    addr = (dst, port)
    sent = 0
    nbytes = 0
    seq = 0

    def inter_arrival():
        # Identico a Flow.inter_arrival() del simulatore.
        if model in ("poisson", "video"):
            return rng.expovariate(rate)
        if model == "bursty":
            if rng.random() < 0.1:
                return rng.expovariate(rate * 5.0)
            return rng.expovariate(rate * 0.5)
        # cbr, control, periodic_telemetry -> costante
        return 1.0 / rate

    end = time.time() + duration
    while time.time() < end:
        size = size_lo if size_lo == size_hi else rng.randint(size_lo, size_hi)
        if size < HDR.size:
            size = HDR.size
        payload = HDR.pack(fid, seq, time.time()) + (b"\x00" * (size - HDR.size))
        try:
            sock.sendto(payload, addr)
            sent += 1
            nbytes += len(payload)
            seq += 1
        except OSError:
            pass
        d = inter_arrival()
        if d > 0:
            time.sleep(d)

    print(json.dumps({"role": "send", "flow_id": fid, "sent": sent, "bytes": nbytes}), flush=True)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else _env("EDS_MODE", "recv")
    if mode == "recv":
        run_recv()
    elif mode == "send":
        run_send()
    else:
        sys.stderr.write("uso: eds_node.py [recv|send]\n")
        sys.exit(2)


if __name__ == "__main__":
    main()
