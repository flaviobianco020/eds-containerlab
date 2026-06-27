#!/usr/bin/env bash
#
# scenarios.sh - Riproduce i 6 scenari di congestione di examples/scenarios.py
#                del simulatore Event-Driven Simulator, sulla topologia
#                single_bottleneck (3 sorgenti).
#
# Prerequisito:
#   ./deploy.sh single_bottleneck
#
# Uso:
#   ./scenarios.sh <1-6>
#
#   1  single_bottleneck     - overload di base (load 13 > cap 10), la coda si
#                              riempie e iniziano i drop
#   2  flash_crowd           - flusso "surge" extra da t=20 a t=50
#   3  bandwidth_degradation - banda del collo di bottiglia 10->4 a t=30, 10 a t=60
#   4  link_failure_recovery - link router->dst giù a t=30, su a t=55
#   5  persistent_overload   - overload sostenuto per tutta la durata (load 15)
#   6  mixed_telemetry_video - 3 classi (control/telemetry/video) con priorità
#                              (HTB + filtri DSCP): il control resta protetto
#
# Gli scenari usano traffico UDP (iperf3 -u) così i drop dovuti alla coda piena
# sono visibili nel report (datagrammi persi), come il drop_count del simulatore.
#
set -euo pipefail

LAB="single-bottleneck"
DST=10.0.30.1
BNECK_IF=eth4          # interfaccia del collo di bottiglia su 'router'
SCN="${1:-}"

cexec()    { local n="$1"; shift; docker exec "clab-${LAB}-${n}" sh -c "$*"; }
cexec_bg() { local n="$1"; shift; docker exec -d "clab-${LAB}-${n}" sh -c "$*"; }

# server iperf3 UDP (uno per porta), -1 = si chiude dopo un test
srv() { cexec_bg dst "iperf3 -s -1 -p $1"; }

# Cambia la banda del collo di bottiglia a runtime (mantiene il netem figlio).
set_bottleneck_rate() {
  echo "   [t=$(date +%s)] banda collo di bottiglia -> $1"
  cexec router "tc qdisc change dev $BNECK_IF root handle 1: tbf rate $1 burst 1mbit limit 1m"
}
link_down() { echo "   [evento] link router->dst GIÙ";  cexec router "ip link set dev $BNECK_IF down"; }
link_up()   { echo "   [evento] link router->dst SU";   cexec router "ip link set dev $BNECK_IF up";   }

# ── Scenario 1: single bottleneck overload ────────────────────────────────────
scenario_1() {
  echo "== Scenario 1 - Single Bottleneck (load=13 > cap=10) =="
  srv 5201; srv 5202; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 8M -t 60 > /tmp/s1_src0.txt 2>&1"
  cexec    src1 "iperf3 -u -c $DST -p 5202 -b 5M -t 60"
  echo "-- flusso src0 --"; cexec src0 "cat /tmp/s1_src0.txt" || true
}

# ── Scenario 2: flash crowd ───────────────────────────────────────────────────
scenario_2() {
  echo "== Scenario 2 - Flash Crowd (surge da t=20 a t=50) =="
  srv 5201; srv 5202; sleep 1
  # flusso normale per tutta la durata
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 4M -t 80 > /tmp/s2_src0.txt 2>&1"
  # flusso surge: parte a t=20, dura 30s (fino a t=50)
  ( sleep 20; cexec src1 "iperf3 -u -c $DST -p 5202 -b 6M -t 30" ) &
  wait
  echo "-- flusso normale src0 --"; cexec src0 "cat /tmp/s2_src0.txt" || true
}

# ── Scenario 3: bandwidth degradation ─────────────────────────────────────────
scenario_3() {
  echo "== Scenario 3 - Bandwidth Degradation (10->4 a t=30, 10 a t=60) =="
  srv 5201; srv 5202; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 7M -t 80 > /tmp/s3_src0.txt 2>&1"
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 2M -t 80 > /tmp/s3_src1.txt 2>&1"
  ( sleep 30; set_bottleneck_rate 4mbit ) &
  ( sleep 60; set_bottleneck_rate 10mbit ) &
  sleep 82
  set_bottleneck_rate 10mbit   # ripristino di sicurezza
  echo "-- flusso src0 --"; cexec src0 "cat /tmp/s3_src0.txt" || true
}

# ── Scenario 4: link failure & recovery ───────────────────────────────────────
scenario_4() {
  echo "== Scenario 4 - Link Failure & Recovery (giù t=30, su t=55) =="
  srv 5201; srv 5202; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 6M -t 90 > /tmp/s4_src0.txt 2>&1"
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 3M -t 90 > /tmp/s4_src1.txt 2>&1"
  ( sleep 30; link_down ) &
  ( sleep 55; link_up; set_bottleneck_rate 10mbit ) &
  sleep 92
  link_up
  echo "-- flusso src0 --"; cexec src0 "cat /tmp/s4_src0.txt" || true
}

# ── Scenario 5: persistent overload ───────────────────────────────────────────
scenario_5() {
  echo "== Scenario 5 - Persistent Overload (load=15 >> cap=10) =="
  srv 5201; srv 5202; srv 5203; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 7M -t 60 > /tmp/s5_src0.txt 2>&1"
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 5M -t 60 > /tmp/s5_src1.txt 2>&1"
  cexec    src2 "iperf3 -u -c $DST -p 5203 -b 3M -t 60"
  echo "-- flusso src0 --"; cexec src0 "cat /tmp/s5_src0.txt" || true
  echo "-- flusso src1 --"; cexec src1 "cat /tmp/s5_src1.txt" || true
}

# ── Scenario 6: mixed traffic con priorità ────────────────────────────────────
# Riconfigura il collo di bottiglia (router:$BNECK_IF) con HTB a 3 classi di
# priorità e filtri per DSCP. Le sorgenti marcano il traffico con iperf3 -S:
#   control   -> DSCP CS6 (0xc0)  classe 1:10 (prio 0, protetta)
#   telemetry -> DSCP CS2 (0x40)  classe 1:20 (prio 1)
#   video     -> best-effort      classe 1:30 (prio 2, default)
setup_priority_qdisc() {
  cexec router "
    tc qdisc replace dev $BNECK_IF root handle 1: htb default 30
    tc class add dev $BNECK_IF parent 1:  classid 1:1  htb rate 10mbit ceil 10mbit burst 125k
    tc class add dev $BNECK_IF parent 1:1 classid 1:10 htb rate 4mbit  ceil 10mbit prio 0
    tc class add dev $BNECK_IF parent 1:1 classid 1:20 htb rate 3mbit  ceil 10mbit prio 1
    tc class add dev $BNECK_IF parent 1:1 classid 1:30 htb rate 1mbit  ceil 10mbit prio 2
    tc qdisc add dev $BNECK_IF parent 1:10 handle 110: netem delay 5ms limit 20
    tc qdisc add dev $BNECK_IF parent 1:20 handle 120: netem delay 5ms limit 20
    tc qdisc add dev $BNECK_IF parent 1:30 handle 130: netem delay 5ms limit 20
    tc filter add dev $BNECK_IF parent 1: protocol ip prio 1 u32 match ip tos 0xc0 0xfc flowid 1:10
    tc filter add dev $BNECK_IF parent 1: protocol ip prio 2 u32 match ip tos 0x40 0xfc flowid 1:20
  "
}
scenario_6() {
  echo "== Scenario 6 - Mixed Telemetry & Video (priorità: control > telemetry > video) =="
  echo ">> Riconfiguro il collo di bottiglia con HTB + filtri DSCP..."
  setup_priority_qdisc
  srv 5201; srv 5202; srv 5203; sleep 1
  # video (best-effort, pri 2)
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 5M -S 0x00 -t 60 > /tmp/s6_video.txt 2>&1"
  # telemetry (CS2, pri 1)
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 4M -S 0x40 -t 60 > /tmp/s6_telemetry.txt 2>&1"
  # control (CS6, pri 0, protetto)
  cexec    src2 "iperf3 -u -c $DST -p 5203 -b 2M -S 0xc0 -t 60"
  echo "-- VIDEO (priorità bassa) --";     cexec src0 "cat /tmp/s6_video.txt"     || true
  echo "-- TELEMETRY (priorità media) --"; cexec src1 "cat /tmp/s6_telemetry.txt" || true
  echo ">> Nota: per ripristinare il drop-tail semplice ridai './deploy.sh single_bottleneck'."
}

case "$SCN" in
  1|2|3|4|5|6) "scenario_${SCN}" ;;
  *) echo "Uso: $0 <1-6>   (richiede './deploy.sh single_bottleneck')" >&2; exit 1 ;;
esac
