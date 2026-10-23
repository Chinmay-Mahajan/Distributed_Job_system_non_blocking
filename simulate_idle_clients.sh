#!/usr/bin/env bash
# ==============================================================================
# Idle Client Connection & Thread Exhaustion Simulator
# ==============================================================================
# Purpose:
#   Empirically verifies the architectural claim:
#   "Users who have NOT submitted any jobs and simply connect via 'nc' still
#    hog an entire OS thread and allocate memory to themselves."
#
# Methodology:
#   1. Locates the running broker server process PID on the target port.
#   2. Measures baseline OS thread count and memory usage (RSS and VSZ).
#   3. Spawns N concurrent idle connections using 'nc' that hold TCP connections
#      open without sending any bytes.
#   4. Measures the step-by-step increase in OS threads and memory on the broker.
#   5. Submits an active probe job to test broker responsiveness under thread load.
#   6. Gracefully terminates all idle connections and verifies thread cleanup.
# ==============================================================================

set -uo pipefail

HOST="127.0.0.1"
PORT=7000
CONNECTIONS=2000
HOLD_SECS=5
RAMP=0

# ANSI color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

usage() {
    cat << EOF
${BOLD}Usage:${NC} $0 [options]

${BOLD}Options:${NC}
  --host <ip>            Broker host IP (default: 127.0.0.1)
  --port <port>          Broker port (default: 7000)
  --connections <num>    Number of idle 'nc' instances to spawn (default: 50)
  --hold <sec>           Seconds to hold idle connections before closing (default: 5)
  --ramp                 Step through a ramp-up (e.g. 10 -> 25 -> 50 -> 100)
  -h, --help             Show this help message

${BOLD}Examples:${NC}
  $0 --connections 50
  $0 --ramp
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --connections)
            CONNECTIONS="$2"
            shift 2
            ;;
        --hold)
            HOLD_SECS="$2"
            shift 2
            ;;
        --ramp)
            RAMP=1
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

if ! command -v nc >/dev/null 2>&1; then
    echo -e "${RED}Error: 'nc' (netcat) is not installed or not in PATH.${NC}"
    exit 1
fi

echo -e "${CYAN}======================================================================${NC}"
echo -e "${BOLD}       BROKER IDLE CONNECTION & OS THREAD CONSUMPTION TEST${NC}"
echo -e "${CYAN}======================================================================${NC}"
echo -e "Target Server: ${BOLD}${HOST}:${PORT}${NC}"

# Find Broker PID
find_broker_pid() {
    # Method 1: Check port listener
    local pid=$(lsof -ti :"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1)
    if [ -n "$pid" ]; then
        echo "$pid"
        return
    fi
    # Method 2: pgrep for distributed_jobv2
    pgrep -f "distributed_jobv2" | head -n 1
}

BROKER_PID=$(find_broker_pid)

if [ -z "$BROKER_PID" ]; then
    echo -e "${RED}Error: Could not locate running broker server on port ${PORT}.${NC}"
    echo -e "${YELLOW}Please start the broker in another terminal first:${NC}"
    echo -e "  cargo run -- --local ${HOST}:${PORT}"
    exit 1
fi

echo -e "${GREEN}✓ Located Broker Process: PID ${BOLD}${BROKER_PID}${NC}"

# Helper to get thread count on macOS / Linux
get_thread_count() {
    local pid=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS: ps -M lists each thread as a row
        local lines=$(ps -M "$pid" 2>/dev/null | wc -l)
        if [ "$lines" -le 1 ]; then echo 0; else echo $(( lines - 1 )); fi
    else
        # Linux: /proc/<pid>/status or ps -eLf
        ls -1 "/proc/$pid/task" 2>/dev/null | wc -l || ps -o thcount -p "$pid" | tail -n 1
    fi
}

# Helper to get memory (RSS and VSZ in KB)
get_memory_kb() {
    local pid=$1
    ps -o rss,vsz -p "$pid" 2>/dev/null | tail -n 1 | awk '{print $1, $2}'
}

# Verify connectivity via nc
PING_CHECK=$(printf "PING\n" | nc -w 3 "$HOST" "$PORT" 2>/dev/null | head -n 1 | tr -d '\r\n')
if [[ "$PING_CHECK" != *PONG* ]]; then
    echo -e "${RED}Error: Broker at ${HOST}:${PORT} did not respond to PING.${NC}"
    exit 1
fi

# Baseline measurement
BASE_THREADS=$(get_thread_count "$BROKER_PID")
read -r BASE_RSS BASE_VSZ <<< "$(get_memory_kb "$BROKER_PID")"

echo ""
echo -e "${PURPLE}--- BASELINE MEASUREMENTS (Before Idle Connections) ---${NC}"
echo -e "  • Active OS Threads:     ${BOLD}${BASE_THREADS}${NC} threads (1 Main + 1 Reaper + 4 Workers + any active conn)"
echo -e "  • Resident Memory (RSS): ${BOLD}$(( BASE_RSS / 1024 )) MB${NC} (${BASE_RSS} KB) [Physical RAM in use]"
echo -e "  • Virtual Memory (VSZ):  ${BOLD}$(( BASE_VSZ / 1024 / 1024 )) GB${NC} (${BASE_VSZ} KB) [Reserved address space]"
echo ""

# Setup named pipe to keep nc stdin open without writing any data
FIFO_PIPE="/tmp/idle_nc_keeper_$$.fifo"
rm -f "$FIFO_PIPE"
mkfifo "$FIFO_PIPE"
exec 3<> "$FIFO_PIPE"

NC_PIDS=()

cleanup() {
    echo -e "\n${YELLOW}[!] Cleaning up idle connections and closing pipes...${NC}"
    if [ ${#NC_PIDS[@]} -gt 0 ]; then
        kill "${NC_PIDS[@]}" 2>/dev/null || true
    fi
    exec 3>&- 2>/dev/null || true
    rm -f "$FIFO_PIPE" 2>/dev/null || true
    sleep 1
    FINAL_THREADS=$(get_thread_count "$BROKER_PID")
    read -r FINAL_RSS FINAL_VSZ <<< "$(get_memory_kb "$BROKER_PID")"
    echo -e "${GREEN}✓ Cleanup complete.${NC}"
    echo -e "  • Restored OS Threads: ${BOLD}${FINAL_THREADS}${NC} (Baseline was ${BASE_THREADS})"
    echo -e "  • Restored RSS Memory: ${BOLD}$(( FINAL_RSS / 1024 )) MB${NC} (${FINAL_RSS} KB)"
}
trap cleanup EXIT INT TERM

# Function to spawn a batch of idle connections
spawn_idle_batch() {
    local target_total=$1
    local current_count=${#NC_PIDS[@]}
    local to_add=$(( target_total - current_count ))

    if [ "$to_add" -le 0 ]; then return; fi

    for (( i=1; i<=to_add; i++ )); do
        nc "$HOST" "$PORT" < "$FIFO_PIPE" >/dev/null 2>&1 &
        NC_PIDS+=($!)
    done

    # Give OS and broker event loop a moment to accept sockets
    sleep 0.8
}

print_stats_row() {
    local label=$1
    local cur_conns=$2
    local threads=$(get_thread_count "$BROKER_PID")
    read -r rss vsz <<< "$(get_memory_kb "$BROKER_PID")"

    local d_th=$(( threads - BASE_THREADS ))
    local d_rss=$(( rss - BASE_RSS ))
    local d_vsz=$(( vsz - BASE_VSZ ))

    local avg_stack="0 KB"
    if [ "$cur_conns" -gt 0 ]; then
        avg_stack="$(( d_rss / cur_conns )) KB"
    fi

    printf "%-18s | %-10s | %-12s | %-12s | %-14s | %-12s\n" \
        "$label" "$cur_conns" "${threads} (+${d_th})" "${rss} KB (+${d_rss} KB)" "$(( vsz / 1024 )) MB" "$avg_stack"
}

echo -e "${BOLD}Spawning Idle Clients (0 jobs submitted, connections held open)...${NC}"
echo ""
printf "${CYAN}%-18s | %-10s | %-12s | %-12s | %-14s | %-12s${NC}\n" \
    "Stage" "Idle Conns" "Broker Threads" "Broker RSS" "Broker VSZ" "RSS / Conn"
echo "-----------------------------------------------------------------------------------------------"

print_stats_row "Baseline" 0

if [ "$RAMP" -eq 1 ]; then
    STEPS=(10 25 50 100 150)
    for target in "${STEPS[@]}"; do
        spawn_idle_batch "$target"
        print_stats_row "Ramp ${target}" "$target"
    done
else
    spawn_idle_batch "$CONNECTIONS"
    print_stats_row "Active Idle" "$CONNECTIONS"
fi

echo "-----------------------------------------------------------------------------------------------"
echo ""

# Probe test while idle connections are held open
echo -e "${PURPLE}--- BROKER FUNCTIONALITY PROBE UNDER THREAD LOAD ---${NC}"
echo -e "Testing if broker can still process jobs while ${#NC_PIDS[@]} idle threads are blocked..."

T0=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000' 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')
PROBE_KEY=$(printf "REGISTER\n" | nc -w 3 "$HOST" "$PORT" 2>/dev/null | head -n 1 | tr -d '+\r\n')
PROBE_JOB=$(printf "SUBMIT %s python print('probe_ok')\n" "$PROBE_KEY" | nc -w 3 "$HOST" "$PORT" 2>/dev/null | head -n 1 | tr -d ':\r\n')

if [[ "$PROBE_JOB" == -ERR* ]] || [ -z "$PROBE_JOB" ]; then
    echo -e "${RED}✗ Probe submission failed: ${PROBE_JOB}${NC}"
else
    while true; do
        POLL_RESP=$(printf "POLL %s %s\n" "$PROBE_KEY" "$PROBE_JOB" | nc -w 3 "$HOST" "$PORT" 2>/dev/null | head -n 1 | tr -d '\r')
        if [[ "$POLL_RESP" == +Done* ]]; then
            T1=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000' 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))')
            echo -e "${GREEN}✓ Probe job succeeded in $(( T1 - T0 )) ms!${NC} (Worker pool still functions normally)"
            break
        elif [[ "$POLL_RESP" == -ERR* ]]; then
            echo -e "${RED}✗ Probe poll failed: ${POLL_RESP}${NC}"
            break
        fi
        sleep 0.05
    done
fi

echo ""
echo -e "Holding ${BOLD}${#NC_PIDS[@]}${NC} idle connections open for ${BOLD}${HOLD_SECS} seconds${NC} to observe memory stability..."
sleep "$HOLD_SECS"

echo -e "${GREEN}Closing all idle connections now...${NC}"
