#!/usr/bin/env bash
set -euo pipefail

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "NAPAKA: zahtevan ukaz ni najden: $1" >&2
        exit 1
    }
}

CP1_GRPC=${CP1_GRPC:-"127.0.0.1:9999"}
CP2_GRPC=${CP2_GRPC:-"127.0.0.1:9998"}
CP3_GRPC=${CP3_GRPC:-"127.0.0.1:9997"}

CP1_RAFT=${CP1_RAFT:-"127.0.0.1:10000"}
CP2_RAFT=${CP2_RAFT:-"127.0.0.1:10001"}
CP3_RAFT=${CP3_RAFT:-"127.0.0.1:10002"}

N1=${N1:-"127.0.0.1:9876"}
N2=${N2:-"127.0.0.1:9877"}
N3=${N3:-"127.0.0.1:9878"}
N4=${N4:-"127.0.0.1:9879"}

TOKEN_SECRET=${TOKEN_SECRET:-"devsecret"}
HB_TIMEOUT=${HB_TIMEOUT:-"2s"}

CONTROL_ADDRS="$CP1_GRPC,$CP2_GRPC,$CP3_GRPC"

# ------------------------------
# Paths
# ------------------------------
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

LOG_DIR="$ROOT_DIR/logs"
DATA_DIR="$ROOT_DIR/data"
BIN_DIR="$ROOT_DIR/out"

mkdir -p "$LOG_DIR" "$DATA_DIR" "$BIN_DIR"

# ------------------------------
# Preflight: ensure a CLEAN demo run (avoid stale processes / stale Raft data)
# ------------------------------
need_cmd nc
need_cmd lsof
need_cmd kill

# Kill any processes listening on our demo ports (prevents "old node still running" issues).
kill_port() {
    local port="$1"
    # macOS: lsof -t returns only PIDs
    local pids
    pids=$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
        echo "Priprava: pobijam prejšnje procese na portu $port: $pids"
        # shellcheck disable=SC2086
        kill -9 $pids >/dev/null 2>&1 || true
    fi
}

for p in 9999 9998 9997 10000 10001 10002 9876 9877 9878 9879; do
    kill_port "$p"
done

# Wipe Raft data dir (fresh control plane each run). Disable with WIPE_DATA=0.
WIPE_DATA="${WIPE_DATA:-1}"
if [[ "$WIPE_DATA" == "1" ]]; then
    echo "Priprava: brišem podatkovni direktorij: $DATA_DIR"
    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
fi

# ------------------------------
# Helpers
# ------------------------------
section() {
    echo
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
}

wait_for_port() {
    local addr="$1"
    local label="$2"
    local timeout_s=${3:-20}
    local host=${addr%:*}
    local port=${addr##*:}
    local start
    start=$(date +%s)

    while true; do
        if nc -z "$host" "$port" >/dev/null 2>&1; then
            echo "OK: $label posluša na $addr"
            return 0
        fi
        local now
        now=$(date +%s)
        if ((now - start > timeout_s)); then
            echo "NAPAKA: časovna prekoračitev pri čakanju na $label na $addr" >&2
            return 1
        fi
        sleep 0.2
    done
}

wait_for_raft_leader() {
    # Wait until ANY control node reports a leader (or at least knows the leaderId).
    local timeout_s=${1:-10}
    local start
    start=$(date +%s)

    while true; do
        for a in "$CP1_GRPC" "$CP2_GRPC" "$CP3_GRPC"; do
            local out
            out=$("$BIN_DIR/raftstate" "$a" 2>/dev/null || true)
            if echo "$out" | grep -q 'isLeader=true'; then
                echo "OK: Raft leader izvoljen (poročal $a)"
                return 0
            fi
            if echo "$out" | grep -q 'leaderId=' && ! echo "$out" | grep -q 'leaderId=$'; then
                echo "OK: Raft leader znan (poročal $a)"
                return 0
            fi
        done

        local now
        now=$(date +%s)
        if ((now - start > timeout_s)); then
            echo "OPOZORILO: Raft leader ni bil zaznan v ${timeout_s}s. Vseeno nadaljujemo)." >&2
            return 1
        fi
        sleep 0.2
    done
}

run_client() {
    # Usage: run_client <control_grpc_addr> <args...>
    local caddr="$1"
    shift
    "$BIN_DIR/client" --naslov "$caddr" "$@"
}

kill_pid() {
    local pid="$1"
    local name="$2"
    if kill -0 "$pid" >/dev/null 2>&1; then
        echo "Ustavljam $name (pid=$pid)"
        kill "$pid" >/dev/null 2>&1 || true
    fi
}

PIDS=()
cleanup() {
    if [[ "${KEEP_RUNNING:-0}" == "1" ]]; then
        section "POSPRAVLJANJE"
        return 0
    fi
    section "POSPRAVLJANJE"
    for entry in "${PIDS[@]:-}"; do
        # entry format: pid:name
        local pid=${entry%%:*}
        local name=${entry#*:}
        kill_pid "$pid" "$name"
    done
    # Final cleanup: best-effort kill any remaining listeners on demo ports (in case a PID wasn't tracked)
    for p in 9999 9998 9997 10000 10001 10002 9876 9877 9878 9879; do
        kill_port "$p" || true
    done

}

trap cleanup EXIT INT TERM

# ------------------------------
# Preconditions
# ------------------------------
need_cmd go
need_cmd nc
need_cmd awk
need_cmd sed

# ------------------------------
# Build
# ------------------------------
section "GRADNJA (go build)"
(
    cd "$ROOT_DIR"
    echo "Uporabljamo Go: $(go version)"

    # If your go.mod uses 'go 1.24', ensure you're running Go 1.24+.
    # The build will fail otherwise.

    go build -o "$BIN_DIR/control" ./cmd/control
    go build -o "$BIN_DIR/server" ./cmd/server
    go build -o "$BIN_DIR/client" ./cmd/client

    # Tools (optional but used in this demo)
    go build -o "$BIN_DIR/raftstate" ./tools/raftstate.go
    go build -o "$BIN_DIR/mbcall" ./tools/mbcall/mbcall.go
)

# ------------------------------
# Start control plane (Raft)
# ------------------------------
section "ZAGON NADZORNE RAVNINE (3x Raft vozlišča)"

# Clean old state for a fresh demo
rm -rf "$DATA_DIR/control1" "$DATA_DIR/control2" "$DATA_DIR/control3" || true

# Seed peers list (compact format supported by your parser):
PEERS="control1=$CP1_RAFT=$CP1_GRPC,control2=$CP2_RAFT=$CP2_GRPC,control3=$CP3_RAFT=$CP3_GRPC"

"$BIN_DIR/control" --raft \
    --node-id control1 \
    --naslov "$CP1_GRPC" \
    --raft-addr "$CP1_RAFT" \
    --data-dir "$DATA_DIR/control1" \
    --bootstrap \
    --peers "$PEERS" \
    --hb-timeout "$HB_TIMEOUT" \
    --token-secret "$TOKEN_SECRET" \
    >"$LOG_DIR/control1.log" 2>&1 &
PIDS+=("$!:control1")

"$BIN_DIR/control" --raft \
    --node-id control2 \
    --naslov "$CP2_GRPC" \
    --raft-addr "$CP2_RAFT" \
    --data-dir "$DATA_DIR/control2" \
    --peers "$PEERS" \
    --hb-timeout "$HB_TIMEOUT" \
    --token-secret "$TOKEN_SECRET" \
    >"$LOG_DIR/control2.log" 2>&1 &
PIDS+=("$!:control2")

"$BIN_DIR/control" --raft \
    --node-id control3 \
    --naslov "$CP3_GRPC" \
    --raft-addr "$CP3_RAFT" \
    --data-dir "$DATA_DIR/control3" \
    --peers "$PEERS" \
    --hb-timeout "$HB_TIMEOUT" \
    --token-secret "$TOKEN_SECRET" \
    >"$LOG_DIR/control3.log" 2>&1 &
PIDS+=("$!:control3")

wait_for_port "$CP1_GRPC" "control1 gRPC"
wait_for_port "$CP2_GRPC" "control2 gRPC"
wait_for_port "$CP3_GRPC" "control3 gRPC"

wait_for_raft_leader 10 || true

section "STANJE RAFT (kdo je leader?)"
"$BIN_DIR/raftstate" "$CP1_GRPC" || true
"$BIN_DIR/raftstate" "$CP2_GRPC" || true
"$BIN_DIR/raftstate" "$CP3_GRPC" || true

# ------------------------------
# Start data nodes
# ------------------------------
section "ZAGON PODATKOVNIH VOZLIŠČ (verižna replikacija)"

"$BIN_DIR/server" --naslov "$N1" --node-id node1 --control "$CONTROL_ADDRS" --token-secret "$TOKEN_SECRET" >"$LOG_DIR/node1.log" 2>&1 &
PIDS+=("$!:node1")
sleep 2.0
"$BIN_DIR/server" --naslov "$N2" --node-id node2 --control "$CONTROL_ADDRS" --token-secret "$TOKEN_SECRET" >"$LOG_DIR/node2.log" 2>&1 &
PIDS+=("$!:node2")
sleep 2.0
"$BIN_DIR/server" --naslov "$N3" --node-id node3 --control "$CONTROL_ADDRS" --token-secret "$TOKEN_SECRET" >"$LOG_DIR/node3.log" 2>&1 &
PIDS+=("$!:node3")

wait_for_port "$N1" "node1"
wait_for_port "$N2" "node2"
wait_for_port "$N3" "node3"

# Give the control plane a moment to form the chain
sleep 1

# ------------------------------
# TEST 1: cluster-state (head/tail)
# ------------------------------
section "TEST 1 — Stanje gruče (HEAD piše, TAIL bere)"
run_client "$CP1_GRPC" cluster-state

# ------------------------------
# TEST 2: create users + topics
# ------------------------------
section "TEST 2 — Ustvari uporabnike + teme (pisanja gredo na HEAD)"
run_client "$CP1_GRPC" create-user "Niko" | tee "$LOG_DIR/t2_create_user_niko.out"
run_client "$CP1_GRPC" create-user "Fedja" | tee "$LOG_DIR/t2_create_user_fedja.out"

run_client "$CP1_GRPC" create-topic "PS Razgovor" | tee "$LOG_DIR/t2_create_topic_splosno.out"
run_client "$CP1_GRPC" create-topic "Porazdeljeni sistemi" | tee "$LOG_DIR/t2_create_topic_go.out"

# ------------------------------
# TEST 3: post messages + read from tail
# ------------------------------
section "TEST 3 — Objavi sporočila v teme + beri iz TAIL"
run_client "$CP1_GRPC" post --tema 1 --uporabnik 1 --besedilo "Opravljamo razgovor pri PS!"
run_client "$CP1_GRPC" post --tema 1 --uporabnik 2 --besedilo "Super nam gre!"
run_client "$CP1_GRPC" post --tema 2 --uporabnik 1 --besedilo "Go mi ne gre preveč dobro."

echo
echo "(ListTopics + GetMessages gresta po zasnovi na TAIL)"
run_client "$CP1_GRPC" list-topics
run_client "$CP1_GRPC" get-messages --tema 1 --od 0 --limit 10
run_client "$CP1_GRPC" get-messages --tema 2 --od 0 --limit 10

# ------------------------------
# TEST 4: likes (including dedup)
# ------------------------------
section "TEST 4 — LikeMessage (deduplikacija: isti uporabnik ne more 2x všečkati)"
# Like message 1 in topic 1 by user 2
run_client "$CP1_GRPC" like --tema 1 --sporocilo 1 --uporabnik 2
# Same user likes again -> should NOT increase likes
run_client "$CP1_GRPC" like --tema 1 --sporocilo 1 --uporabnik 2
# Another user likes -> should increase
run_client "$CP1_GRPC" like --tema 1 --sporocilo 1 --uporabnik 1

run_client "$CP1_GRPC" get-messages --tema 1 --od 0 --limit 10

# ------------------------------
# TEST 5: update/delete permissions
# ------------------------------
section "TEST 5 — Dovoljenja Update/Delete (samo avtor sporočila)"

echo "Posodobi sporočilo 2 (tema 1) kot avtor (uporabnik 2):"
run_client "$CP1_GRPC" update --tema 1 --sporocilo 2 --uporabnik 2 --besedilo "Go mi gre super! (edit)"

echo
echo "Poskus posodobitve istega sporočila kot NE-avtor (uporabnik 1) — mora pasti:"
set +e
run_client "$CP1_GRPC" update --tema 1 --sporocilo 2 --uporabnik 1 --besedilo "Fedji probam spremenit sporočilo :)"
echo "(pričakovano: neuspeh zgoraj)"
set -e

echo
echo "Poskus brisanja sporočila 2 kot NE-avtor (uporabnik 1) — mora pasti:"
set +e
run_client "$CP1_GRPC" delete --tema 1 --sporocilo 2 --uporabnik 1
echo "(pričakovano: neuspeh zgoraj)"
set -e

echo
echo "Brisanje sporočila 2 kot AVTOR (uporabnik 2) — mora uspeti:"
run_client "$CP1_GRPC" delete --tema 1 --sporocilo 2 --uporabnik 2

run_client "$CP1_GRPC" get-messages --tema 1 --od 0 --limit 10

# ------------------------------
# TEST 6: subscription stream (OP_POST/LIKE/UPDATE/DELETE)
# ------------------------------
section "TEST 6 — Naročnine (stream MessageEvent)"

# Start a subscription for user 1 on topic 1.
# We run it in background and log output.
"$BIN_DIR/client" --naslov "$CP1_GRPC" subscribe --uporabnik 1 --teme "1" --od 0 >"$LOG_DIR/t6_subscribe_u1_t1.log" 2>&1 &
SUB_PID=$!
echo "Naročnina začeta (pid=$SUB_PID). Izhod: $LOG_DIR/t6_subscribe_u1_t1.log"

# Generate events
sleep 0.6

# 1) Post a NEW message in topic 1 and capture its ID (so that LIKE/UPDATE/DELETE match the topic).
POST_OUT_FILE="$LOG_DIR/t6_post_capture.log"
# NOTE: client/server may print to STDERR; capture both and keep it visible in the terminal
POST_OUT=$(run_client "$CP1_GRPC" post --tema 1 --uporabnik 1 --besedilo "Novo sporočilo za stream." 2>&1 | tee "$POST_OUT_FILE")
# Robust ID extraction (handles commas, extra text, etc.)
STREAM_MSG_ID=$(printf "%s\n" "$POST_OUT" | grep -Eo "id=[0-9]+" | head -n1 | cut -d= -f2)
# Fallback: also accept formats like "id: 123"
if [[ -z "${STREAM_MSG_ID:-}" ]]; then
    STREAM_MSG_ID=$(printf "%s\n" "$POST_OUT" | grep -Eo "id:[[:space:]]*[0-9]+" | head -n1 | grep -Eo "[0-9]+" || true)
fi

if [[ -z "${STREAM_MSG_ID:-}" ]]; then
    echo "NAPAKA: Ni bilo možno parsati message ID od: $POST_OUT" >&2
    exit 1
fi

# 2) Like by another user (user 2) -> OP_LIKE
run_client "$CP1_GRPC" like --tema 1 --sporocilo "$STREAM_MSG_ID" --uporabnik 2

# 3) Update by owner (user 1) -> OP_UPDATE
run_client "$CP1_GRPC" update --tema 1 --sporocilo "$STREAM_MSG_ID" --uporabnik 1 --besedilo "Novo sporočilo (edit)."

# 4) Delete by owner (user 1) -> OP_DELETE
run_client "$CP1_GRPC" delete --tema 1 --sporocilo "$STREAM_MSG_ID" --uporabnik 1

sleep 1.0
echo
echo "Dnevnik naročnika (mora vsebovati OP_POST, OP_LIKE, OP_UPDATE, OP_DELETE):"
sed -n '1,120p' "$LOG_DIR/t6_subscribe_u1_t1.log" || true

kill "$SUB_PID" >/dev/null 2>&1 || true

# ------------------------------
# TEST 7: subscription load balancing (head assigns a node)
# ------------------------------
section "TEST 7 — Dodelitev vozlišča za naročnino (uravnoteženje obremenitve)"

# We call GetSubscriptionNode directly on the HEAD (MessageBoard API).
# First, discover HEAD address via cluster-state.
RAW_CLUSTER="$(run_client "$CP1_GRPC" cluster-state 2>&1)"
echo "$RAW_CLUSTER" >"$LOG_DIR/t7_cluster_state.raw"
# Extract host:port from: HEAD: nodeX (127.0.0.1:9876)
HEAD_ADDR="$(printf '%s\n' "$RAW_CLUSTER" | sed -n 's/^HEAD:.*(\(.*\)).*/\1/p' | head -n 1 | tr -d ' ')"
TAIL_ADDR="$(printf '%s\n' "$RAW_CLUSTER" | sed -n 's/^TAIL:.*(\(.*\)).*/\1/p' | head -n 1 | tr -d ' ')"

echo "HEAD_ADDR=$HEAD_ADDR"
echo "TAIL_ADDR=$TAIL_ADDR"

# Ask head to assign subscription nodes for different users.
# You should see that assigned_node may differ (depending on your hashing policy).
"$BIN_DIR/mbcall" --addr "$HEAD_ADDR" get-subnode --user 1 --topics "1" | tee "$LOG_DIR/t7_subnode_u1.out"
"$BIN_DIR/mbcall" --addr "$HEAD_ADDR" get-subnode --user 2 --topics "1" | tee "$LOG_DIR/t7_subnode_u2.out"
"$BIN_DIR/mbcall" --addr "$HEAD_ADDR" get-subnode --user 1 --topics "2" | tee "$LOG_DIR/t7_subnode_u1_t2.out"

# ------------------------------
# TEST 8: fail a middle node in the chain, verify reconfiguration + consistency
# ------------------------------
section "TEST 8 — Prevezava verige po odpovedi vozlišča (ubij srednje vozlišče)"

# Choose a victim that is NOT the current HEAD or TAIL (so writes can still go to HEAD).
CS0="$(run_client "$CP1_GRPC" cluster-state 2>&1)"
echo "$CS0" >"$LOG_DIR/t8_cluster_state_before.raw"

HEAD_NODE="$(printf '%s\n' "$CS0" | sed -n 's/^HEAD: \([^ ]*\).*/\1/p' | head -n 1)"
HEAD_ADDR="$(printf '%s\n' "$CS0" | sed -n 's/^HEAD:.*(\(.*\)).*/\1/p' | head -n 1 | tr -d ' ')"
TAIL_NODE="$(printf '%s\n' "$CS0" | sed -n 's/^TAIL: \([^ ]*\).*/\1/p' | head -n 1)"
TAIL_ADDR="$(printf '%s\n' "$CS0" | sed -n 's/^TAIL:.*(\(.*\)).*/\1/p' | head -n 1 | tr -d ' ')"

echo "Trenutni HEAD: $HEAD_NODE ($HEAD_ADDR)"
echo "Trenutni TAIL: $TAIL_NODE ($TAIL_ADDR)"

VICTIM_NODE=""
for n in node1 node2 node3; do
    if [[ "$n" != "$HEAD_NODE" && "$n" != "$TAIL_NODE" ]]; then
        VICTIM_NODE="$n"
        break
    fi
done

VICTIM_PID=$(printf '%s\n' "${PIDS[@]}" | awk -F: -v n="$VICTIM_NODE" '$2==n{print $1; exit}')
echo "Ubijam $VICTIM_NODE pid=$VICTIM_PID"
kill "$VICTIM_PID" >/dev/null 2>&1 || true

# Wait for control plane to detect failure and rewire the chain.
echo "Čakam na prevezavo verige (HEAD/TAIL se morata umakniti od $VICTIM_NODE)..."
ok=0
for i in $(seq 1 80); do
    CS_OUT="$(run_client "$CP1_GRPC" cluster-state 2>&1)"
    echo "$CS_OUT" >"$LOG_DIR/t8_cluster_state_iter_${i}.raw"

    NEW_HEAD_NODE="$(printf '%s\n' "$CS_OUT" | sed -n 's/^HEAD: \([^ ]*\).*/\1/p' | head -n 1)"
    NEW_HEAD_ADDR="$(printf '%s\n' "$CS_OUT" | sed -n 's/^HEAD:.*(\(.*\)).*/\1/p' | head -n 1 | tr -d ' ')"
    NEW_TAIL_NODE="$(printf '%s\n' "$CS_OUT" | sed -n 's/^TAIL: \([^ ]*\).*/\1/p' | head -n 1)"
    NEW_TAIL_ADDR="$(printf '%s\n' "$CS_OUT" | sed -n 's/^TAIL:.*(\(.*\)).*/\1/p' | head -n 1 | tr -d ' ')"

    head_ok=0
    tail_ok=0
    if [[ -n "$NEW_HEAD_ADDR" ]] && [[ "$NEW_HEAD_NODE" != "$VICTIM_NODE" ]] && nc -z "${NEW_HEAD_ADDR%:*}" "${NEW_HEAD_ADDR##*:}" >/dev/null 2>&1; then
        head_ok=1
    fi
    if [[ -n "$NEW_TAIL_ADDR" ]] && [[ "$NEW_TAIL_NODE" != "$VICTIM_NODE" ]] && nc -z "${NEW_TAIL_ADDR%:*}" "${NEW_TAIL_ADDR##*:}" >/dev/null 2>&1; then
        tail_ok=1
    fi

    if [[ $head_ok -eq 1 && $tail_ok -eq 1 ]]; then
        echo "OK: head=$NEW_HEAD_NODE ($NEW_HEAD_ADDR), tail=$NEW_TAIL_NODE ($NEW_TAIL_ADDR)"
        ok=1
        break
    fi

    sleep 0.25
done

if [[ $ok -ne 1 ]]; then
    echo "OPOZORILO: veriga se ni pravočasno v celoti prevezala; vseeno nadaljujem."
fi

echo "Stanje gruče po odpovedi:"
run_client "$CP1_GRPC" cluster-state

sleep 3.0

run_client "$CP1_GRPC" cluster-state

echo "Objavi novo sporočilo po odpovedi (mora se replicirati head→tail):"
echo "(Če se prevezava še propagira, lahko prvi poskus ne uspe; zato nekaj sekund izvajamo ponovne poskuse.)"
POST_OK=0
for i in $(seq 1 30); do
    set +e
    OUT="$(run_client "$CP1_GRPC" post --tema 2 --uporabnik 2 --besedilo "Sporočilo po odpovedi middle node." 2>&1)"
    RC=$?
    set -e
    if [[ $RC -eq 0 ]]; then
        echo "$OUT"
        POST_OK=1
        break
    fi
    echo "Poskus $i ni uspel: $OUT"
    # common transient error during neighbor update
    sleep 0.3
done
if [[ "$POST_OK" != "1" ]]; then
    echo "NAPAKA: objava še vedno ne uspe po ponovnih poskusih. Za diagnostiko izpisujem zadnje vrstice dnevnika glave (HEAD):"
    tail -n 60 "$LOG_DIR/node1.log" || true
    exit 1
fi

echo
echo "Branje iz tail-a (mora vsebovati novo sporočilo):"
run_client "$CP1_GRPC" get-messages --tema 2 --od 0 --limit 20

# ------------------------------
# TEST 9: add a new node (scale out) and observe new tail
# ------------------------------
section "TEST 9 — Dodaj novo vozlišče (node4 se pridruži; tail se lahko premakne)"

"$BIN_DIR/server" --naslov "$N4" --node-id node4 --control "$CONTROL_ADDRS" --token-secret "$TOKEN_SECRET" >"$LOG_DIR/node4.log" 2>&1 &
PIDS+=("$!:node4")
wait_for_port "$N4" "node4"

sleep 2
run_client "$CP1_GRPC" cluster-state

echo
echo "Pisanje po skaliranju (mora biti še vedno berljivo z (novega) repa):"
run_client "$CP1_GRPC" post --tema 1 --uporabnik 1 --besedilo "Sporočilo po dodanem node4."
run_client "$CP1_GRPC" get-messages --tema 1 --od 0 --limit 50

# ------------------------------
# TEST 10: Raft failover (kill control leader)
# ------------------------------
section "TEST 10 — Failover nadzorne ravnine (odpoved Raft leaderja)"

echo "Stanje Raft PRED ubojem leaderja:"
# Show raft state on surviving control nodes (skip dead leader gRPC endpoint).
set +e
for addr in "$CP2_GRPC" "$CP3_GRPC"; do
    "$BIN_DIR/raftstate" "$addr" 2>/dev/null || true
done
# Optionally show CP1 only if it's still reachable.
if nc -z "${CP1_GRPC%:*}" "${CP1_GRPC##*:}" >/dev/null 2>&1; then
    "$BIN_DIR/raftstate" "$CP1_GRPC" 2>/dev/null || true
else
    echo "OK: $CP1_GRPC ni dosegljiv (pričakovano, če je bil to ubit leader)"
fi
set -e
# Identify leader by asking each control node.
LEADER_GRPC=$(
    (
        "$BIN_DIR/raftstate" "$CP1_GRPC" 2>/dev/null || true
        "$BIN_DIR/raftstate" "$CP2_GRPC" 2>/dev/null || true
        "$BIN_DIR/raftstate" "$CP3_GRPC" 2>/dev/null || true
    ) |
        awk '/isLeader=true/ {print $1; exit}'
)

if [[ -z "${LEADER_GRPC:-}" ]]; then
    echo "OPOZORILO: leaderja ni bilo mogoče samodejno zaznati; kot rezervno možnost ubijem control1."
    LEADER_GRPC="$CP1_GRPC"
fi

echo "Zaznan naslov gRPC leaderja: $LEADER_GRPC"

LEADER_PID=$(printf '%s\n' "${PIDS[@]}" | awk -F: -v addr="$LEADER_GRPC" '
  $2=="control1" && addr=="'"$CP1_GRPC"'" {print $1}
  $2=="control2" && addr=="'"$CP2_GRPC"'" {print $1}
  $2=="control3" && addr=="'"$CP3_GRPC"'" {print $1}
' | head -n1)

if [[ -n "${LEADER_PID:-}" ]]; then
    echo "Ubijam control leaderja pid=$LEADER_PID"
    kill "$LEADER_PID" >/dev/null 2>&1 || true
else
    echo "OPOZORILO: naslova leaderja ni bilo mogoče povezati s PID-jem; po potrebi ga lahko ročno ustavite."
fi

# Wait for election
sleep 2

echo "Stanje Raft po uboju leaderja (pričakujem novega leaderja):"
# Skip calling raftstate on the killed leader endpoint (tool panics if unreachable).
set +e
for addr in "$CP1_GRPC" "$CP2_GRPC" "$CP3_GRPC"; do
    if [[ "$addr" == "$LEADER_GRPC" ]]; then
        echo "Preskočim ubit endpoint leaderja: $addr"
        continue
    fi
    if nc -z "${addr%:*}" "${addr##*:}" >/dev/null 2>&1; then
        "$BIN_DIR/raftstate" "$addr" || true
    else
        echo "Control endpoint ni dosegljiv (preskočeno): $addr"
    fi
done
set -e

echo
echo "Preverim, da sistem še deluje z uporabo preživelega control node-a (poskusi CP2, nato CP3):"
set +e
run_client "$CP2_GRPC" cluster-state || run_client "$CP3_GRPC" cluster-state
set -e

echo
echo "Izvedem write + read po odpovedi (strežniki naj samodejno sledijo novemu leaderju):"
set +e
run_client "$CP2_GRPC" post --tema 1 --uporabnik 2 --besedilo "Pisanje po odpovedi control leaderja." ||
    run_client "$CP3_GRPC" post --tema 1 --uporabnik 2 --besedilo "Pisanje po odpovedi control leaderja."
set -e

set +e
run_client "$CP2_GRPC" get-messages --tema 1 --od 0 --limit 200 ||
    run_client "$CP3_GRPC" get-messages --tema 1 --od 0 --limit 200
set -e

section "KONEC"
echo "Logs so v: $LOG_DIR"
echo "Če poganjaš interaktivno, lahko zdaj CTRL+C; pospravljanje bo ustavilo vse procese."
