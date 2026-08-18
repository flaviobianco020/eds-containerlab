#!/usr/bin/env bash
#
# deploy.sh - Deploy and configuration of the ContainerLab topologies
#             for the Event-Driven Simulator (EDS) project.
#
# Deploys with ContainerLab and then configures, on each container:
#   - IP addresses on the interfaces
#   - IP forwarding on the routing nodes
#   - SUBNET-SPECIFIC routes (the default gateway is NOT touched)
#   - traffic shaping with tc:
#       * 10 Mbps links  -> tbf with "burst 1mbit" (burst fix) + netem,
#         with a finite queue "limit 20" (drop-tail) that reproduces
#         the simulator's QueueManager(max_size=20)
#       * access links   -> netem only (no tbf, high capacity)
#
# Usage:
#   ./deploy.sh <topology>            # deploy + configuration
#   ./deploy.sh <topology> destroy    # tear down the lab
#
#   topology: single_bottleneck | multi_hop | mesh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPO_DIR="${SCRIPT_DIR}/topologies"

# Queue size (drop-tail) on the 10 Mbps links, in packets.
# Corresponds to QueueManager(max_size=...) in the simulator (default 20).
QUEUE_LIMIT="${QUEUE_LIMIT:-20}"

TOPOLOGY="${1:-}"
ACTION="${2:-deploy}"

usage() {
  echo "Usage: $0 <single_bottleneck|multi_hop|mesh> [deploy|destroy]" >&2
  exit 1
}

[ -n "$TOPOLOGY" ] || usage

case "$TOPOLOGY" in
  single_bottleneck) LAB="single-bottleneck"; CLAB_FILE="${TOPO_DIR}/single_bottleneck.clab.yml" ;;
  multi_hop)         LAB="multi-hop";         CLAB_FILE="${TOPO_DIR}/multi_hop.clab.yml" ;;
  mesh)              LAB="mesh";              CLAB_FILE="${TOPO_DIR}/mesh.clab.yml" ;;
  *) usage ;;
esac

# ── helper ───────────────────────────────────────────────────────────────────

# cexec <node> "<shell command>"
cexec() {
  local node="$1"; shift
  docker exec "clab-${LAB}-${node}" sh -c "$*"
}

# set_ip <node> <iface> <cidr>
set_ip() {
  cexec "$1" "ip addr add $3 dev $2 2>/dev/null || true; ip link set $2 up"
}

# enable_forward <node>
enable_forward() {
  cexec "$1" "sysctl -w net.ipv4.ip_forward=1 >/dev/null"
}

# add_route <node> <subnet> <gateway>   (specific route, does NOT touch the default gw)
add_route() {
  cexec "$1" "ip route replace $2 via $3"
}

# shape_bottleneck <node> <iface> [rate] [delay]
# 10 Mbps link: tbf root (rate + burst fix "burst 1mbit") and netem as the child
# qdisc for the finite queue "limit $QUEUE_LIMIT" (drop-tail), which reproduces
# the drops of QueueManager.enqueue() when the queue is full.
#
# IMPORTANT — delay=0 by default (BYTE-limited bottleneck). With a delay on the
# netem, by Little's law the "in flight" packets are rate*delay: with `limit 20`
# the delay eats the buffer and the bottleneck saturates at limit/delay pkt/s
# INDEPENDENTLY of the bytes, so compression never empties the queue (the service
# becomes ∝ packets instead of ∝ bytes as in the simulator). With delay=0 the
# buffer is a pure drop-tail and the constraint goes back to the bandwidth (tbf):
# compression reduces the bytes -> more packets pass -> the PDR responds, on par
# with the simulator. The link latency is modeled on the access links.
shape_bottleneck() {
  local node="$1" iface="$2" rate="${3:-10mbit}" delay="${4:-0ms}"
  cexec "$node" "tc qdisc replace dev $iface root handle 1: tbf rate $rate burst 1mbit limit 1m"
  cexec "$node" "tc qdisc replace dev $iface parent 1:1 handle 10: netem delay $delay limit $QUEUE_LIMIT"
}

# shape_access <node> <iface> [delay]
# High-capacity access link: only netem, no tbf. Here lives the path LATENCY
# (default 5ms), moved here from the bottleneck so the bottleneck drop-tail stays
# pure (see shape_bottleneck). High capacity + wide netem limit => no drop introduced.
shape_access() {
  local node="$1" iface="$2" delay="${3:-5ms}"
  cexec "$node" "tc qdisc replace dev $iface root netem delay $delay"
}

# ── per-topology configurations ──────────────────────────────────────────────

config_single_bottleneck() {
  # addresses
  set_ip src0   eth1 10.0.10.1/24
  set_ip router eth1 10.0.10.254/24
  set_ip src1   eth1 10.0.20.1/24
  set_ip router eth2 10.0.20.254/24
  set_ip src2   eth1 10.0.40.1/24
  set_ip router eth3 10.0.40.254/24
  set_ip router eth4 10.0.30.254/24
  set_ip dst    eth1 10.0.30.1/24

  enable_forward router

  # subnet-specific routes: each source towards dst, dst towards the sources
  add_route src0 10.0.30.0/24 10.0.10.254
  add_route src1 10.0.30.0/24 10.0.20.254
  add_route src2 10.0.30.0/24 10.0.40.254
  add_route dst  10.0.10.0/24 10.0.30.254
  add_route dst  10.0.20.0/24 10.0.30.254
  add_route dst  10.0.40.0/24 10.0.30.254

  # high-capacity access links -> netem only
  shape_access src0   eth1
  shape_access src1   eth1
  shape_access src2   eth1
  shape_access router eth1
  shape_access router eth2
  shape_access router eth3

  # 10 Mbps bottleneck (both directions) -> tbf + netem (drop-tail)
  shape_bottleneck router eth4
  shape_bottleneck dst    eth1
}

config_multi_hop() {
  # addresses
  set_ip n0 eth1 10.0.1.1/24
  set_ip n1 eth1 10.0.1.2/24
  set_ip n1 eth2 10.0.2.1/24
  set_ip n2 eth1 10.0.2.2/24
  set_ip n2 eth2 10.0.3.1/24
  set_ip n3 eth1 10.0.3.2/24

  enable_forward n1
  enable_forward n2

  # subnet-specific routes (chain n0 -> n3 and back)
  add_route n0 10.0.2.0/24 10.0.1.2
  add_route n0 10.0.3.0/24 10.0.1.2
  add_route n1 10.0.3.0/24 10.0.2.2
  add_route n2 10.0.1.0/24 10.0.2.1
  add_route n3 10.0.2.0/24 10.0.3.1
  add_route n3 10.0.1.0/24 10.0.3.1

  # all links are 10 Mbps bottlenecks (queue_size=20)
  shape_bottleneck n0 eth1
  shape_bottleneck n1 eth1
  shape_bottleneck n1 eth2
  shape_bottleneck n2 eth1
  shape_bottleneck n2 eth2
  shape_bottleneck n3 eth1
}

config_mesh() {
  # addresses - horizontal links row 0
  set_ip n00 eth1 10.1.1.1/24
  set_ip n01 eth1 10.1.1.2/24
  set_ip n01 eth2 10.1.2.1/24
  set_ip n02 eth1 10.1.2.2/24
  # addresses - horizontal links row 1
  set_ip n10 eth1 10.1.3.1/24
  set_ip n11 eth1 10.1.3.2/24
  set_ip n11 eth2 10.1.4.1/24
  set_ip n12 eth1 10.1.4.2/24
  # addresses - vertical links
  set_ip n00 eth2 10.1.5.1/24
  set_ip n10 eth2 10.1.5.2/24
  set_ip n01 eth3 10.1.6.1/24
  set_ip n11 eth3 10.1.6.2/24
  set_ip n02 eth2 10.1.7.1/24
  set_ip n12 eth2 10.1.7.2/24

  # forwarding on all mesh nodes
  for n in n00 n01 n02 n10 n11 n12; do enable_forward "$n"; done

  # demonstrative corner-to-corner path: n00 (10.1.1.1) <-> n12 (10.1.7.2)
  # forward:  n00 -> n01 -> n02 -> n12
  add_route n00 10.1.7.0/24 10.1.1.2
  add_route n01 10.1.7.0/24 10.1.2.2
  # return: n12 -> n02 -> n01 -> n00
  add_route n12 10.1.1.0/24 10.1.7.1
  add_route n02 10.1.1.0/24 10.1.2.1

  # every mesh link is 10 Mbps (queue_size=20) -> tbf + netem (drop-tail)
  shape_bottleneck n00 eth1; shape_bottleneck n01 eth1
  shape_bottleneck n01 eth2; shape_bottleneck n02 eth1
  shape_bottleneck n10 eth1; shape_bottleneck n11 eth1
  shape_bottleneck n11 eth2; shape_bottleneck n12 eth1
  shape_bottleneck n00 eth2; shape_bottleneck n10 eth2
  shape_bottleneck n01 eth3; shape_bottleneck n11 eth3
  shape_bottleneck n02 eth2; shape_bottleneck n12 eth2
}

# ── main ─────────────────────────────────────────────────────────────────────

if [ "$ACTION" = "destroy" ]; then
  echo ">> Tearing down the lab '$LAB'..."
  containerlab destroy -t "$CLAB_FILE" --cleanup
  exit 0
fi

echo ">> Deploying the lab '$LAB' from $CLAB_FILE ..."
containerlab deploy -t "$CLAB_FILE" --reconfigure

echo ">> Configuring addresses, routing and tc (drop-tail queue = $QUEUE_LIMIT packets) ..."
"config_${TOPOLOGY}"

echo ">> Done. Topology '$TOPOLOGY' ready."
echo "   Quick test:       ./run_simulation.sh $TOPOLOGY"
if [ "$TOPOLOGY" = "single_bottleneck" ]; then
  echo "   EDS scenarios 1-6:  ./scenarios.sh <1-6>"
fi
