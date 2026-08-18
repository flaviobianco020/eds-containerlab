#!/usr/bin/env bash
#
# run_simulation.sh - Generates test traffic on the ContainerLab topologies
#                     and measures latency/throughput (analogous to the EDS
#                     scenarios).
#
# Requires the topology to have already been started with:
#   ./deploy.sh <topology>
#
# Usage:
#   ./run_simulation.sh <topology> [duration_sec]
#
#   topology: single_bottleneck | multi_hop | mesh
#   duration_sec: duration of the iperf3 tests (default 15)
#
set -euo pipefail

TOPOLOGY="${1:-}"
DURATION="${2:-15}"

case "$TOPOLOGY" in
  single_bottleneck) LAB="single-bottleneck" ;;
  multi_hop)         LAB="multi-hop" ;;
  mesh)              LAB="mesh" ;;
  *) echo "Usage: $0 <single_bottleneck|multi_hop|mesh> [duration_sec]" >&2; exit 1 ;;
esac

# cexec <node> "<command>"  (foreground)
cexec()    { local node="$1"; shift; docker exec "clab-${LAB}-${node}" sh -c "$*"; }
# cexec_bg <node> "<command>"  (in background inside the container)
cexec_bg() { local node="$1"; shift; docker exec -d "clab-${LAB}-${node}" sh -c "$*"; }

run_single_bottleneck() {
  local dst_ip=10.0.30.1
  echo "== single_bottleneck: 10 Mbps bottleneck =="
  echo "-- Latency src0 -> dst --"
  cexec src0 "ping -c 4 $dst_ip" || true
  echo
  echo "-- Throughput: two simultaneous flows (src0, src1) -> dst --"
  echo "   (the expected sum is about 10 Mbps, shared on the bottleneck)"
  cexec_bg dst "iperf3 -s -1 -p 5201"
  cexec_bg dst "iperf3 -s -1 -p 5202"
  sleep 1
  cexec_bg src0 "iperf3 -c $dst_ip -p 5201 -t $DURATION > /tmp/eds_src0.txt 2>&1"
  cexec     src1 "iperf3 -c $dst_ip -p 5202 -t $DURATION"
  echo "-- src0 flow result --"
  cexec src0 "cat /tmp/eds_src0.txt" || true
}

run_multi_hop() {
  local dst_ip=10.0.3.2
  echo "== multi_hop: chain n0 -> n1 -> n2 -> n3 (3 hops of 10 Mbps) =="
  echo "-- Latency n0 -> n3 (the RTT grows with the number of hops) --"
  cexec n0 "ping -c 4 $dst_ip" || true
  echo
  echo "-- Throughput n0 -> n3 (expected ~10 Mbps) --"
  cexec_bg n3 "iperf3 -s -1 -p 5201"
  sleep 1
  cexec n0 "iperf3 -c $dst_ip -p 5201 -t $DURATION"
}

run_mesh() {
  local dst_ip=10.1.7.2
  echo "== mesh: corner-to-corner path n00 -> n12 =="
  echo "-- Latency n00 -> n12 --"
  cexec n00 "ping -c 4 $dst_ip" || true
  echo
  echo "-- Throughput n00 -> n12 (expected ~10 Mbps) --"
  cexec_bg n12 "iperf3 -s -1 -p 5201"
  sleep 1
  cexec n00 "iperf3 -c $dst_ip -p 5201 -t $DURATION"
}

"run_${TOPOLOGY}"
