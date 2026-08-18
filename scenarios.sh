#!/usr/bin/env bash
#
# scenarios.sh - Reproduces the 6 congestion scenarios of examples/scenarios.py
#                from the Event-Driven Simulator, on the single_bottleneck
#                topology (3 sources).
#
# Prerequisite:
#   ./deploy.sh single_bottleneck
#
# Usage:
#   ./scenarios.sh <1-6>
#
#   1  single_bottleneck     - basic overload (load 13 > cap 10), the queue
#                              fills up and drops begin
#   2  flash_crowd           - extra "surge" flow from t=20 to t=50
#   3  bandwidth_degradation - bottleneck bandwidth 10->4 at t=30, 10 at t=60
#   4  link_failure_recovery - router->dst link down at t=30, up at t=55
#   5  persistent_overload   - sustained overload for the whole run (load 15)
#   6  mixed_telemetry_video - 3 classes (control/telemetry/video) with priority
#                              (HTB + DSCP filters): control stays protected
#
# The scenarios use UDP traffic (iperf3 -u) so drops caused by the full queue
# are visible in the report (lost datagrams), like the simulator's drop_count.
#
set -euo pipefail

LAB="single-bottleneck"
DST=10.0.30.1
BNECK_IF=eth4          # bottleneck interface on 'router'
SCN="${1:-}"

cexec()    { local n="$1"; shift; docker exec "clab-${LAB}-${n}" sh -c "$*"; }
cexec_bg() { local n="$1"; shift; docker exec -d "clab-${LAB}-${n}" sh -c "$*"; }

# UDP iperf3 server (one per port), -1 = closes after a single test
srv() { cexec_bg dst "iperf3 -s -1 -p $1"; }

# Change the bottleneck bandwidth at runtime (keeps the netem child).
set_bottleneck_rate() {
  echo "   [t=$(date +%s)] bottleneck bandwidth -> $1"
  cexec router "tc qdisc change dev $BNECK_IF root handle 1: tbf rate $1 burst 1mbit limit 1m"
}
link_down() { echo "   [event] router->dst link DOWN"; cexec router "ip link set dev $BNECK_IF down"; }
link_up()   { echo "   [event] router->dst link UP";   cexec router "ip link set dev $BNECK_IF up";   }

# ── Scenario 1: single bottleneck overload ────────────────────────────────────
scenario_1() {
  echo "== Scenario 1 - Single Bottleneck (load=13 > cap=10) =="
  srv 5201; srv 5202; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 8M -t 60 > /tmp/s1_src0.txt 2>&1"
  cexec    src1 "iperf3 -u -c $DST -p 5202 -b 5M -t 60"
  echo "-- flow src0 --"; cexec src0 "cat /tmp/s1_src0.txt" || true
}

# ── Scenario 2: flash crowd ───────────────────────────────────────────────────
scenario_2() {
  echo "== Scenario 2 - Flash Crowd (surge from t=20 to t=50) =="
  srv 5201; srv 5202; sleep 1
  # normal flow for the whole run
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 4M -t 80 > /tmp/s2_src0.txt 2>&1"
  # surge flow: starts at t=20, lasts 30s (until t=50)
  ( sleep 20; cexec src1 "iperf3 -u -c $DST -p 5202 -b 6M -t 30" ) &
  wait
  echo "-- normal flow src0 --"; cexec src0 "cat /tmp/s2_src0.txt" || true
}

# ── Scenario 3: bandwidth degradation ─────────────────────────────────────────
scenario_3() {
  echo "== Scenario 3 - Bandwidth Degradation (10->4 at t=30, 10 at t=60) =="
  srv 5201; srv 5202; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 7M -t 80 > /tmp/s3_src0.txt 2>&1"
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 2M -t 80 > /tmp/s3_src1.txt 2>&1"
  ( sleep 30; set_bottleneck_rate 4mbit ) &
  ( sleep 60; set_bottleneck_rate 10mbit ) &
  sleep 82
  set_bottleneck_rate 10mbit   # safety restore
  echo "-- flow src0 --"; cexec src0 "cat /tmp/s3_src0.txt" || true
}

# ── Scenario 4: link failure & recovery ───────────────────────────────────────
scenario_4() {
  echo "== Scenario 4 - Link Failure & Recovery (down t=30, up t=55) =="
  srv 5201; srv 5202; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 6M -t 90 > /tmp/s4_src0.txt 2>&1"
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 3M -t 90 > /tmp/s4_src1.txt 2>&1"
  ( sleep 30; link_down ) &
  ( sleep 55; link_up; set_bottleneck_rate 10mbit ) &
  sleep 92
  link_up
  echo "-- flow src0 --"; cexec src0 "cat /tmp/s4_src0.txt" || true
}

# ── Scenario 5: persistent overload ───────────────────────────────────────────
scenario_5() {
  echo "== Scenario 5 - Persistent Overload (load=15 >> cap=10) =="
  srv 5201; srv 5202; srv 5203; sleep 1
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 7M -t 60 > /tmp/s5_src0.txt 2>&1"
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 5M -t 60 > /tmp/s5_src1.txt 2>&1"
  cexec    src2 "iperf3 -u -c $DST -p 5203 -b 3M -t 60"
  echo "-- flow src0 --"; cexec src0 "cat /tmp/s5_src0.txt" || true
  echo "-- flow src1 --"; cexec src1 "cat /tmp/s5_src1.txt" || true
}

# ── Scenario 6: mixed traffic with priority ───────────────────────────────────
# Reconfigures the bottleneck (router:$BNECK_IF) with a 3-class-priority HTB
# and DSCP filters. The sources mark the traffic with iperf3 -S:
#   control   -> DSCP CS6 (0xc0)  class 1:10 (prio 0, protected)
#   telemetry -> DSCP CS2 (0x40)  class 1:20 (prio 1)
#   video     -> best-effort      class 1:30 (prio 2, default)
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
  echo "== Scenario 6 - Mixed Telemetry & Video (priority: control > telemetry > video) =="
  echo ">> Reconfiguring the bottleneck with HTB + DSCP filters..."
  setup_priority_qdisc
  srv 5201; srv 5202; srv 5203; sleep 1
  # video (best-effort, pri 2)
  cexec_bg src0 "iperf3 -u -c $DST -p 5201 -b 5M -S 0x00 -t 60 > /tmp/s6_video.txt 2>&1"
  # telemetry (CS2, pri 1)
  cexec_bg src1 "iperf3 -u -c $DST -p 5202 -b 4M -S 0x40 -t 60 > /tmp/s6_telemetry.txt 2>&1"
  # control (CS6, pri 0, protected)
  cexec    src2 "iperf3 -u -c $DST -p 5203 -b 2M -S 0xc0 -t 60"
  echo "-- VIDEO (low priority) --";     cexec src0 "cat /tmp/s6_video.txt"     || true
  echo "-- TELEMETRY (medium priority) --"; cexec src1 "cat /tmp/s6_telemetry.txt" || true
  echo ">> Note: to restore the plain drop-tail, run './deploy.sh single_bottleneck' again."
}

case "$SCN" in
  1|2|3|4|5|6) "scenario_${SCN}" ;;
  *) echo "Usage: $0 <1-6>   (requires './deploy.sh single_bottleneck')" >&2; exit 1 ;;
esac
