#!/usr/bin/env bash
#
# deploy.sh - Deploy e configurazione delle topologie ContainerLab
#             per il progetto Event-Driven Simulator (EDS).
#
# Esegue il deploy con ContainerLab e poi configura, su ogni container:
#   - indirizzi IP sulle interfacce
#   - IP forwarding sui nodi che instradano
#   - route SPECIFICHE PER SUBNET (il default gateway NON viene toccato)
#   - shaping del traffico con tc:
#       * link da 10 Mbps  -> tbf con "burst 1mbit" (fix del burst) + netem,
#         con coda finita "limit 20" (drop-tail) che riproduce
#         QueueManager(max_size=20) del simulatore
#       * link di accesso  -> solo netem (nessun tbf, alta capacità)
#
# Uso:
#   ./deploy.sh <topologia>            # deploy + configurazione
#   ./deploy.sh <topologia> destroy    # smontaggio del lab
#
#   topologia: single_bottleneck | multi_hop | mesh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOPO_DIR="${SCRIPT_DIR}/topologies"

# Dimensione della coda (drop-tail) sui link da 10 Mbps, in pacchetti.
# Corrisponde a QueueManager(max_size=...) nel simulatore (default 20).
QUEUE_LIMIT="${QUEUE_LIMIT:-20}"

TOPOLOGY="${1:-}"
ACTION="${2:-deploy}"

usage() {
  echo "Uso: $0 <single_bottleneck|multi_hop|mesh> [deploy|destroy]" >&2
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

# cexec <node> "<comando shell>"
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

# add_route <node> <subnet> <gateway>   (route specifica, NON tocca il default gw)
add_route() {
  cexec "$1" "ip route replace $2 via $3"
}

# shape_bottleneck <node> <iface> [rate] [delay]
# Link da 10 Mbps: tbf root (rate + fix del burst "burst 1mbit") e netem come
# qdisc figlia per il ritardo e per la coda finita "limit $QUEUE_LIMIT"
# (drop-tail), che riproduce i drop di QueueManager.enqueue() quando la coda
# è piena. Il tbf ha un limit ampio così i drop avvengono nel netem (a numero
# di pacchetti), esattamente come nel simulatore.
shape_bottleneck() {
  local node="$1" iface="$2" rate="${3:-10mbit}" delay="${4:-5ms}"
  cexec "$node" "tc qdisc replace dev $iface root handle 1: tbf rate $rate burst 1mbit limit 1m"
  cexec "$node" "tc qdisc replace dev $iface parent 1:1 handle 10: netem delay $delay limit $QUEUE_LIMIT"
}

# shape_access <node> <iface> [delay]
# Link di accesso ad alta capacità: solo netem, nessun tbf.
shape_access() {
  local node="$1" iface="$2" delay="${3:-1ms}"
  cexec "$node" "tc qdisc replace dev $iface root netem delay $delay"
}

# ── configurazioni per topologia ─────────────────────────────────────────────

config_single_bottleneck() {
  # indirizzi
  set_ip src0   eth1 10.0.10.1/24
  set_ip router eth1 10.0.10.254/24
  set_ip src1   eth1 10.0.20.1/24
  set_ip router eth2 10.0.20.254/24
  set_ip src2   eth1 10.0.40.1/24
  set_ip router eth3 10.0.40.254/24
  set_ip router eth4 10.0.30.254/24
  set_ip dst    eth1 10.0.30.1/24

  enable_forward router

  # route specifiche per subnet: ogni sorgente verso dst, dst verso le sorgenti
  add_route src0 10.0.30.0/24 10.0.10.254
  add_route src1 10.0.30.0/24 10.0.20.254
  add_route src2 10.0.30.0/24 10.0.40.254
  add_route dst  10.0.10.0/24 10.0.30.254
  add_route dst  10.0.20.0/24 10.0.30.254
  add_route dst  10.0.40.0/24 10.0.30.254

  # link di accesso ad alta capacità -> solo netem
  shape_access src0   eth1
  shape_access src1   eth1
  shape_access src2   eth1
  shape_access router eth1
  shape_access router eth2
  shape_access router eth3

  # collo di bottiglia 10 Mbps (entrambe le direzioni) -> tbf + netem (drop-tail)
  shape_bottleneck router eth4
  shape_bottleneck dst    eth1
}

config_multi_hop() {
  # indirizzi
  set_ip n0 eth1 10.0.1.1/24
  set_ip n1 eth1 10.0.1.2/24
  set_ip n1 eth2 10.0.2.1/24
  set_ip n2 eth1 10.0.2.2/24
  set_ip n2 eth2 10.0.3.1/24
  set_ip n3 eth1 10.0.3.2/24

  enable_forward n1
  enable_forward n2

  # route specifiche per subnet (catena n0 -> n3 e ritorno)
  add_route n0 10.0.2.0/24 10.0.1.2
  add_route n0 10.0.3.0/24 10.0.1.2
  add_route n1 10.0.3.0/24 10.0.2.2
  add_route n2 10.0.1.0/24 10.0.2.1
  add_route n3 10.0.2.0/24 10.0.3.1
  add_route n3 10.0.1.0/24 10.0.3.1

  # tutti i link sono colli di bottiglia da 10 Mbps (queue_size=20)
  shape_bottleneck n0 eth1
  shape_bottleneck n1 eth1
  shape_bottleneck n1 eth2
  shape_bottleneck n2 eth1
  shape_bottleneck n2 eth2
  shape_bottleneck n3 eth1
}

config_mesh() {
  # indirizzi - link orizzontali riga 0
  set_ip n00 eth1 10.1.1.1/24
  set_ip n01 eth1 10.1.1.2/24
  set_ip n01 eth2 10.1.2.1/24
  set_ip n02 eth1 10.1.2.2/24
  # indirizzi - link orizzontali riga 1
  set_ip n10 eth1 10.1.3.1/24
  set_ip n11 eth1 10.1.3.2/24
  set_ip n11 eth2 10.1.4.1/24
  set_ip n12 eth1 10.1.4.2/24
  # indirizzi - link verticali
  set_ip n00 eth2 10.1.5.1/24
  set_ip n10 eth2 10.1.5.2/24
  set_ip n01 eth3 10.1.6.1/24
  set_ip n11 eth3 10.1.6.2/24
  set_ip n02 eth2 10.1.7.1/24
  set_ip n12 eth2 10.1.7.2/24

  # forwarding su tutti i nodi della mesh
  for n in n00 n01 n02 n10 n11 n12; do enable_forward "$n"; done

  # percorso dimostrativo angolo-angolo: n00 (10.1.1.1) <-> n12 (10.1.7.2)
  # andata:  n00 -> n01 -> n02 -> n12
  add_route n00 10.1.7.0/24 10.1.1.2
  add_route n01 10.1.7.0/24 10.1.2.2
  # ritorno: n12 -> n02 -> n01 -> n00
  add_route n12 10.1.1.0/24 10.1.7.1
  add_route n02 10.1.1.0/24 10.1.2.1

  # ogni link della mesh e' da 10 Mbps (queue_size=20) -> tbf + netem (drop-tail)
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
  echo ">> Smontaggio del lab '$LAB'..."
  containerlab destroy -t "$CLAB_FILE" --cleanup
  exit 0
fi

echo ">> Deploy del lab '$LAB' da $CLAB_FILE ..."
containerlab deploy -t "$CLAB_FILE" --reconfigure

echo ">> Configurazione di indirizzi, routing e tc (coda drop-tail = $QUEUE_LIMIT pacchetti) ..."
"config_${TOPOLOGY}"

echo ">> Fatto. Topologia '$TOPOLOGY' pronta."
echo "   Test rapido:      ./run_simulation.sh $TOPOLOGY"
if [ "$TOPOLOGY" = "single_bottleneck" ]; then
  echo "   Scenari EDS 1-6:  ./scenarios.sh <1-6>"
fi
