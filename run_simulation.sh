#!/usr/bin/env bash
#
# run_simulation.sh - Genera traffico di test sulle topologie ContainerLab
#                     e misura latenza/throughput (analogo agli scenari EDS).
#
# Richiede che la topologia sia gia' stata avviata con:
#   ./deploy.sh <topologia>
#
# Uso:
#   ./run_simulation.sh <topologia> [durata_sec]
#
#   topologia: single_bottleneck | multi_hop | mesh
#   durata_sec: durata dei test iperf3 (default 15)
#
set -euo pipefail

TOPOLOGY="${1:-}"
DURATION="${2:-15}"

case "$TOPOLOGY" in
  single_bottleneck) LAB="single-bottleneck" ;;
  multi_hop)         LAB="multi-hop" ;;
  mesh)              LAB="mesh" ;;
  *) echo "Uso: $0 <single_bottleneck|multi_hop|mesh> [durata_sec]" >&2; exit 1 ;;
esac

# cexec <node> "<comando>"  (primo piano)
cexec()    { local node="$1"; shift; docker exec "clab-${LAB}-${node}" sh -c "$*"; }
# cexec_bg <node> "<comando>"  (in background nel container)
cexec_bg() { local node="$1"; shift; docker exec -d "clab-${LAB}-${node}" sh -c "$*"; }

run_single_bottleneck() {
  local dst_ip=10.0.30.1
  echo "== single_bottleneck: collo di bottiglia da 10 Mbps =="
  echo "-- Latenza src0 -> dst --"
  cexec src0 "ping -c 4 $dst_ip" || true
  echo
  echo "-- Throughput: due flussi simultanei (src0, src1) -> dst --"
  echo "   (la somma attesa e' circa 10 Mbps, condivisi sul collo di bottiglia)"
  cexec_bg dst "iperf3 -s -1 -p 5201"
  cexec_bg dst "iperf3 -s -1 -p 5202"
  sleep 1
  cexec_bg src0 "iperf3 -c $dst_ip -p 5201 -t $DURATION > /tmp/eds_src0.txt 2>&1"
  cexec     src1 "iperf3 -c $dst_ip -p 5202 -t $DURATION"
  echo "-- risultato flusso src0 --"
  cexec src0 "cat /tmp/eds_src0.txt" || true
}

run_multi_hop() {
  local dst_ip=10.0.3.2
  echo "== multi_hop: catena n0 -> n1 -> n2 -> n3 (3 hop da 10 Mbps) =="
  echo "-- Latenza n0 -> n3 (l'RTT cresce con il numero di hop) --"
  cexec n0 "ping -c 4 $dst_ip" || true
  echo
  echo "-- Throughput n0 -> n3 (atteso ~10 Mbps) --"
  cexec_bg n3 "iperf3 -s -1 -p 5201"
  sleep 1
  cexec n0 "iperf3 -c $dst_ip -p 5201 -t $DURATION"
}

run_mesh() {
  local dst_ip=10.1.7.2
  echo "== mesh: percorso angolo-angolo n00 -> n12 =="
  echo "-- Latenza n00 -> n12 --"
  cexec n00 "ping -c 4 $dst_ip" || true
  echo
  echo "-- Throughput n00 -> n12 (atteso ~10 Mbps) --"
  cexec_bg n12 "iperf3 -s -1 -p 5201"
  sleep 1
  cexec n00 "iperf3 -c $dst_ip -p 5201 -t $DURATION"
}

"run_${TOPOLOGY}"
