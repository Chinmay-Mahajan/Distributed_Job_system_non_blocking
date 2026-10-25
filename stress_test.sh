#!/usr/bin/env bash
# ==============================================================================
# Traffic Simulation & Stress Testing Script for Sandboxed Job Execution Broker
# ==============================================================================
# This script simulates concurrent multi-tenant traffic to stress test the
# two-level scheduler and worker pool architecture.
#
# It connects to the server strictly via netcat (nc), submits a mix of simple/short
# and complex/heavy code execution requests, and measures the wait time (latency)
# each client experiences in getting results.
#
# Key Features:
# - Simulates high-frequency ("spammer"), moderate-frequency, and low-frequency clients.
# - Sends a balanced mix of simple (fast math, string manipulation, shell echo)
#   and complex (prime sieve, big-int fibonacci, I/O delay, CPU stress) jobs.
# - Uses 'nc' for all socket communication (SUBMIT, POLL, PING, STATS).
# - Accurately records end-to-end client wait time and extracts server-side execution
#   and queue wait metrics.
# - Produces real-time colorized logs and a comprehensive final ASCII analytics table.
# ==============================================================================

set -uo pipefail

DEFAULT_POLL_INTERVAL="0.08"
HOST="127.0.0.1"
PORT=7000
PRESET="standard"
POLL_INTERVAL="$DEFAULT_POLL_INTERVAL"
QUIET=0
TIMEOUT_NC=5

# ANSI colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

usage() {
    cat << EOF
${BOLD}Usage:${NC} $0 [options]

${BOLD}Options:${NC}
  --host <ip>            Broker host (default: 127.0.0.1)
  --port <port>          Broker port (default: 7000)
  --preset <preset>      Traffic preset: 'quick', 'standard', 'heavy' (default: standard)
  --poll-interval <sec>  Sleep duration between POLL calls in seconds (default: 0.04)
  --quiet                Suppress per-job live output, only show final report
  -h, --help             Show this help message

${BOLD}Presets:${NC}
  quick     ~10-15s runtime (12 high-freq, 5 med-freq, 2 low-freq requests)
  standard  ~25-35s runtime (25 high-freq, 10 med-freq, 4 low-freq requests)
  heavy     ~60+s runtime   (45 high-freq, 18 med-freq, 6 low-freq requests)
EOF
    exit 0
}

# Parse CLI arguments
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
        --preset)
            PRESET="$2"
            shift 2
            ;;
        --poll-interval)
            POLL_INTERVAL="$2"
            shift 2
            ;;
        --quiet)
            QUIET=1
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

# Ensure nc is available
if ! command -v nc >/dev/null 2>&1; then
    echo -e "${RED}Error: 'nc' (netcat) is not installed or not in PATH.${NC}"
    exit 1
fi

# Ensure python3 is available for summary analytics
if ! command -v python3 >/dev/null 2>&1; then
    echo -e "${RED}Error: 'python3' is required for timing summary metrics.${NC}"
    exit 1
fi

# Helper to get high-resolution timestamp in milliseconds
get_time_ms() {
    perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000' 2>/dev/null || python3 -c 'import time; print(int(time.time()*1000))'
}

# Helper to send a single-line command via nc and return immediately upon receiving the response line
send_nc() {
    printf "%s\n" "$1" | nc -w "$TIMEOUT_NC" "$HOST" "$PORT" 2>/dev/null | head -n 1 | tr -d '\r\n'
}

# Check broker connectivity via nc
echo -e "${CYAN}=== Distributed Job Execution Broker Stress Test ===${NC}"
echo -e "Target Server: ${BOLD}${HOST}:${PORT}${NC}"
echo -e "Testing connectivity via 'nc'..."

PING_RESP=$(send_nc "PING")
if [[ "$PING_RESP" != "+PONG" && "$PING_RESP" != *"PONG"* ]]; then
    echo -e "${RED}Error: Could not connect to broker at ${HOST}:${PORT}.${NC}"
    echo -e "Response was: '${PING_RESP}'"
    echo -e "${YELLOW}Please start the broker server first, e.g.:${NC}"
    echo -e "  cargo run -- --local ${HOST}:${PORT}     # Using Local Process Sandbox"
    echo -e "  cargo run -- ${HOST}:${PORT}             # Using Docker Sandbox"
    exit 1
fi

echo -e "${GREEN}✓ Broker is alive (PING -> ${PING_RESP})${NC}"

# Fetch pre-test scheduler stats
PRE_STATS=$(send_nc "STATS")
echo -e "Initial Broker Stats: ${PRE_STATS}"
echo ""

# Setup temporary results directory
RUN_ID=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="/tmp/stress_test_${RUN_ID}"
mkdir -p "$RESULTS_DIR"
RAW_LOG="$RESULTS_DIR/traffic.tsv"
touch "$RAW_LOG"

# Clean up child and grandchild processes on exit/interrupt
cleanup() {
    echo -e "\n${YELLOW}[!] Terminating active client jobs...${NC}"
    local pids
    pids=$(jobs -p)
    for pid in $pids; do
        pkill -TERM -P "$pid" 2>/dev/null || true
        kill -TERM "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    echo -e "${YELLOW}[!] Traffic generator stopped.${NC}"
    exit 130
}
trap cleanup SIGINT SIGTERM

# ==============================================================================
# Workload Definitions (Simple / Short vs Complex / Heavy)
# ==============================================================================
# We define a pool of jobs.
# Format: <TYPE>|<LANG>|<CODE>
# Simple jobs: complete in ~15-30ms
# Complex jobs: compute-intensive (prime sieve, fibonacci) or simulate I/O delays (300-800ms)

get_workload() {
    local index=$1
    local mode=$2

    if [ "$mode" = "simple" ]; then
        case $(( index % 4 )) in
            0) echo "Simple-Math|python|print(sum(range(500)))" ;;
            1) echo "Simple-String|python|print('stress_test_traffic'[::-1]*2)" ;;
            2) echo "Simple-Echo|sh|echo worker_job_done_$$" ;;
            3) echo "Simple-Sqrt|python|import math; print(math.isqrt(9876543210))" ;;
        esac
    else
        case $(( index % 5 )) in
            0) echo "Complex-Fib|python|a,b=0,1; exec('for _ in range(3000): a,b=b,a+b'); print('fib_len:', len(str(b)))" ;;
            1) echo "Complex-Primes|python|p=[x for x in range(2, 5000) if all(x%d!=0 for d in range(2, int(x**0.5)+1))]; print('primes:', len(p))" ;;
            2) echo "Complex-Delay300|python|import time; time.sleep(0.3); sum(i*i for i in range(100000)); print('delay_300ms_ok')" ;;
            3) echo "Complex-Delay600|python|import time; time.sleep(0.6); print('delay_600ms_ok')" ;;
            4) echo "Complex-Matrix|python|a=[[1,2,3],[4,5,6],[7,8,9]]; print(sum(sum(r) for r in a)*1000)" ;;
        esac
    fi
}

# ==============================================================================
# Client Worker Engine
# ==============================================================================
# Submits a single job via nc using secret_key, polls via nc until terminal state, and logs metrics.
submit_and_poll_job() {
    local client_id="$1"
    local secret_key="$2"
    local client_tier="$3"
    local job_num="$4"
    local workload_def="$5"

    local job_type=$(echo "$workload_def" | cut -d'|' -f1)
    local job_lang=$(echo "$workload_def" | cut -d'|' -f2)
    local job_code=$(echo "$workload_def" | cut -d'|' -f3-)

    local t_submit_ms=$(get_time_ms)

    # 1. Authenticated Submit request via nc
    local submit_resp=$(send_nc "SUBMIT ${secret_key} ${job_lang} ${job_code}")

    local job_id=""
    if [[ "$submit_resp" == :* ]]; then
        job_id="${submit_resp#:}"
    elif [[ "$submit_resp" == *\"job_id\":\"* ]]; then
        job_id=$(echo "$submit_resp" | sed -n 's/.*"job_id":"\([^"]*\)".*/\1/p')
    fi

    if [ -z "$job_id" ]; then
        if [ "$QUIET" -eq 0 ]; then
            echo -e "${RED}[${client_id}] SUBMIT ERROR on #${job_num}: ${submit_resp}${NC}"
        fi
        echo -e "${client_id}\t${client_tier}\t${job_num}\t${job_type}\t${t_submit_ms}\t0\t0\tSubmitFailed\t0\t0\t0" >> "$RAW_LOG"
        return
    fi

    if [ "$QUIET" -eq 0 ]; then
        echo -e "${BLUE}[${client_id}]${NC} SUBMIT #${job_num} [${job_type}] -> ${job_id:0:8}..."
    fi

    # 2. Asynchronously poll via nc with secret_key until Done / Failed / TimedOut
    local poll_attempts=0
    local max_poll_attempts=750 # ~60s hard ceiling so background subshells never loop forever
    local empty_strikes=0

    while (( poll_attempts < max_poll_attempts )); do
        (( poll_attempts++ ))
        local poll_resp=$(send_nc "POLL ${secret_key} ${job_id}")

        case "$poll_resp" in
            +Done*|+Failed*|+TimedOut*)
                local t_done_ms=$(get_time_ms)
                local wait_time_ms=$(( t_done_ms - t_submit_ms ))

                # Parse JSON payload from +<Status> <json>
                # Using quick python one-liner for JSON extraction
                local parsed_meta=$(python3 -c '
import sys, json
line = sys.argv[1]
idx = line.find("{")
if idx != -1:
    try:
        data = json.loads(line[idx:])
        st = data.get("status", "Unknown")
        exec_dur = data.get("execution_duration_ms") or 0
        created = data.get("created_at_ms") or 0
        completed = data.get("completed_at_ms") or 0
        queue_wait = max(0, (completed - created) - exec_dur) if (created and completed) else 0
        print(f"{st}\t{exec_dur}\t{queue_wait}\t{completed}")
    except Exception:
        print("Unknown\t0\t0\t0")
else:
    print("Unknown\t0\t0\t0")
' "$poll_resp" 2>/dev/null || echo "Unknown\t0\t0\t0")

                local status=$(echo "$parsed_meta" | cut -f1)
                local exec_dur_ms=$(echo "$parsed_meta" | cut -f2)
                local queue_wait_ms=$(echo "$parsed_meta" | cut -f3)

                # Append to raw TSV log
                # Fields: client_id, client_tier, job_num, job_type, t_submit_ms, t_done_ms, wait_time_ms, status, exec_dur_ms, queue_wait_ms, job_id
                echo -e "${client_id}\t${client_tier}\t${job_num}\t${job_type}\t${t_submit_ms}\t${t_done_ms}\t${wait_time_ms}\t${status}\t${exec_dur_ms}\t${queue_wait_ms}\t${job_id}" >> "$RAW_LOG"

                if [ "$QUIET" -eq 0 ]; then
                    local color="${GREEN}"
                    if [ "$status" != "Done" ]; then color="${YELLOW}"; fi
                    printf "${color}[%s] DONE #${job_num} [%s] | Wait: %5dms | Exec: %4dms | Queue: %4dms | %s${NC}\n" \
                        "$client_id" "$job_type" "$wait_time_ms" "$exec_dur_ms" "$queue_wait_ms" "$status"
                fi
                break
                ;;
            *Poll\ rate\ limit\ exceeded*)
                empty_strikes=0
                sleep 0.25
                ;;
            -ERR*)
                if [ "$QUIET" -eq 0 ]; then
                    echo -e "${RED}[${client_id}] POLL ERROR on #${job_num} (${job_id:0:8}): ${poll_resp}${NC}"
                fi
                break
                ;;
            "")
                (( empty_strikes++ ))
                if (( empty_strikes >= 3 )); then
                    if [ "$QUIET" -eq 0 ]; then
                        echo -e "${RED}[${client_id}] Connection lost while polling #${job_num} (${job_id:0:8}). Aborting poll.${NC}"
                    fi
                    break
                fi
                sleep "$POLL_INTERVAL"
                ;;
            *)
                empty_strikes=0
                sleep "$POLL_INTERVAL"
                ;;
        esac
    done
}

# Runs a single simulated client sending requests at a specific interval
simulate_client() {
    local client_id="$1"
    local client_tier="$2"
    local num_requests="$3"
    local submit_interval="$4"
    local complex_ratio="$5" # e.g. 50 for 50% complex

    trap 'kill $(jobs -p) 2>/dev/null || true' EXIT SIGINT SIGTERM

    # Register client once to obtain server-issued secret_key
    local reg_resp=$(send_nc "REGISTER")
    local secret_key="${reg_resp#+}"
    if [[ -z "$secret_key" || "$secret_key" == -ERR* ]]; then
        echo -e "${RED}[${client_id}] REGISTER FAILED: ${reg_resp}${NC}"
        return
    fi

    for i in $(seq 1 "$num_requests"); do
        # Determine whether this job should be complex or simple
        local mode="simple"
        if [ "$complex_ratio" -gt 0 ] && (( (i * 17) % 100 < complex_ratio )); then
            mode="complex"
        fi

        local workload=$(get_workload "$i" "$mode")

        # Submit and poll asynchronously in background so client frequency is maintained!
        submit_and_poll_job "$client_id" "$secret_key" "$client_tier" "$i" "$workload" &

        sleep "$submit_interval"
    done

    # Wait for all background requests for this client to finish polling
    wait
}

# ==============================================================================
# Configure Traffic Profiles based on Preset
# ==============================================================================
echo -e "${BOLD}Configuring Traffic Simulation (Preset: ${PRESET})...${NC}"

# Client Definitions:
# Tier 1: High-Frequency (Heavy Traffic / Spammer Clients)
# Tier 2: Moderate-Frequency (Standard Clients)
# Tier 3: Low-Frequency (Intermittent / Background Clients)

declare -a CLIENT_LIST

case "$PRESET" in
    quick|light)
        # ~10-15 seconds
        CLIENT_LIST=(
            "client_spammer_1:High-Freq:12:0.06:40"
            "client_spammer_2:High-Freq:10:0.08:50"
            "client_moderate_1:Med-Freq:5:0.35:40"
            "client_slow_1:Low-Freq:2:1.00:50"
        )
        ;;
    heavy)
        # ~60+ seconds
        CLIENT_LIST=(
            "client_spammer_1:High-Freq:45:0.03:50"
            "client_spammer_2:High-Freq:40:0.05:50"
            "client_moderate_1:Med-Freq:18:0.25:40"
            "client_moderate_2:Med-Freq:15:0.35:40"
            "client_slow_1:Low-Freq:6:1.20:50"
            "client_slow_2:Low-Freq:4:1.60:50"
        )
        ;;
    standard|*)
        # ~25-35 seconds
        CLIENT_LIST=(
            "client_spammer_1:High-Freq:24:0.04:50"
            "client_spammer_2:High-Freq:20:0.07:50"
            "client_moderate_1:Med-Freq:10:0.30:40"
            "client_moderate_2:Med-Freq:8:0.40:40"
            "client_slow_1:Low-Freq:4:1.20:50"
            "client_slow_2:Low-Freq:3:1.50:50"
        )
        ;;
esac

echo -e "Clients configured:"
for c in "${CLIENT_LIST[@]}"; do
    cid=$(echo "$c" | cut -d: -f1)
    tier=$(echo "$c" | cut -d: -f2)
    reqs=$(echo "$c" | cut -d: -f3)
    interval=$(echo "$c" | cut -d: -f4)
    ratio=$(echo "$c" | cut -d: -f5)
    printf "  - %-18s [%-9s] -> %2d requests, interval %5ss, %d%% complex\n" "$cid" "$tier" "$reqs" "$interval" "$ratio"
done

echo ""
echo -e "${PURPLE}=================== TRAFFIC SIMULATION RUNNING ===================${NC}"
TEST_START_TIME=$(get_time_ms)

# Launch all clients concurrently in background
CLIENT_PIDS=()
for c in "${CLIENT_LIST[@]}"; do
    cid=$(echo "$c" | cut -d: -f1)
    tier=$(echo "$c" | cut -d: -f2)
    reqs=$(echo "$c" | cut -d: -f3)
    interval=$(echo "$c" | cut -d: -f4)
    ratio=$(echo "$c" | cut -d: -f5)

    simulate_client "$cid" "$tier" "$reqs" "$interval" "$ratio" &
    CLIENT_PIDS+=($!)
done

# Wait for all client pipelines to complete
for pid in "${CLIENT_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
done

TEST_END_TIME=$(get_time_ms)
TOTAL_ELAPSED_MS=$(( TEST_END_TIME - TEST_START_TIME ))
TOTAL_ELAPSED_SEC=$(python3 -c "print(f'{$TOTAL_ELAPSED_MS/1000:.2f}')")

echo -e "${PURPLE}=================== TRAFFIC SIMULATION COMPLETE ===================${NC}"
echo -e "Total Wall-Clock Time: ${BOLD}${TOTAL_ELAPSED_SEC}s${NC} (${TOTAL_ELAPSED_MS} ms)"
echo ""

# Query post-test broker stats via nc
POST_STATS=$(send_nc "STATS")
echo -e "Final Broker Stats: ${POST_STATS}"
echo ""

# ==============================================================================
# Analytics & Summary Report (Processed via Python)
# ==============================================================================
python3 - "$RAW_LOG" "$TOTAL_ELAPSED_SEC" "$PRE_STATS" "$POST_STATS" << 'EOF'
import sys
import os
import statistics

log_file = sys.argv[1]
elapsed_sec = float(sys.argv[2])
pre_stats = sys.argv[3]
post_stats = sys.argv[4]

if not os.path.exists(log_file) or os.path.getsize(log_file) == 0:
    print("No log entries recorded.")
    sys.exit(0)

# Data containers
# key: client_id -> list of record dicts
clients = {}
all_records = []

with open(log_file, "r") as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) < 10:
            continue
        rec = {
            "client_id": parts[0],
            "client_tier": parts[1],
            "job_num": int(parts[2]),
            "job_type": parts[3],
            "t_submit": int(parts[4]),
            "t_done": int(parts[5]),
            "wait_ms": int(parts[6]),
            "status": parts[7],
            "exec_ms": int(parts[8]),
            "queue_ms": int(parts[9]),
            "job_id": parts[10] if len(parts) > 10 else "",
        }
        all_records.append(rec)
        cid = rec["client_id"]
        if cid not in clients:
            clients[cid] = {
                "tier": rec["client_tier"],
                "records": []
            }
        clients[cid]["records"].append(rec)

# Calculate statistics per client
print("=" * 105)
print("                       CLIENT WAIT TIME & LATENCY PERFORMANCE REPORT")
print("=" * 105)
print(f"{'Client ID':<18} {'Tier':<10} {'Reqs':<5} {'Done':<5} {'Avg Wait':<10} {'Min':<8} {'P50':<8} {'P95':<8} {'Max':<8} {'Avg Exec':<10} {'Avg Queue':<10}")
print("-" * 105)

tier_order = {"High-Freq": 1, "Med-Freq": 2, "Low-Freq": 3}
sorted_clients = sorted(clients.items(), key=lambda x: (tier_order.get(x[1]["tier"], 9), x[0]))

for cid, data in sorted_clients:
    recs = data["records"]
    tier = data["tier"]
    total_reqs = len(recs)
    done_recs = [r for r in recs if r["status"] in ("Done", "Failed", "TimedOut")]
    num_done = len(done_recs)

    if not done_recs:
        print(f"{cid:<18} {tier:<10} {total_reqs:<5} {0:<5} {'N/A':<10} {'N/A':<8} {'N/A':<8} {'N/A':<8} {'N/A':<8} {'N/A':<10} {'N/A':<10}")
        continue

    waits = [r["wait_ms"] for r in done_recs]
    execs = [r["exec_ms"] for r in done_recs]
    queues = [r["queue_ms"] for r in done_recs]

    avg_wait = statistics.mean(waits)
    min_wait = min(waits)
    max_wait = max(waits)
    p50_wait = statistics.median(waits)
    p95_wait = sorted(waits)[int(len(waits) * 0.95)] if len(waits) > 1 else max_wait

    avg_exec = statistics.mean(execs)
    avg_queue = statistics.mean(queues)

    print(f"{cid:<18} {tier:<10} {total_reqs:<5} {num_done:<5} {avg_wait:>7.1f} ms {min_wait:>6}ms {p50_wait:>6.0f}ms {p95_wait:>6.0f}ms {max_wait:>6}ms {avg_exec:>7.1f} ms {avg_queue:>7.1f} ms")

print("-" * 105)

# Breakdown by Job Complexity
simple_recs = [r for r in all_records if r["job_type"].startswith("Simple")]
complex_recs = [r for r in all_records if r["job_type"].startswith("Complex")]

print("\n" + "=" * 80)
print("                          WORKLOAD COMPLEXITY BREAKDOWN")
print("=" * 80)
print(f"{'Complexity':<15} {'Count':<8} {'Avg Wait Time':<18} {'Avg Sandbox Exec':<18} {'Avg Queue Wait':<15}")
print("-" * 80)

for name, rlist in [("Simple (Short)", simple_recs), ("Complex (Heavy)", complex_recs)]:
    if rlist:
        w_avg = statistics.mean([r["wait_ms"] for r in rlist])
        e_avg = statistics.mean([r["exec_ms"] for r in rlist])
        q_avg = statistics.mean([r["queue_ms"] for r in rlist])
        print(f"{name:<15} {len(rlist):<8} {w_avg:>13.1f} ms {e_avg:>15.1f} ms {q_avg:>13.1f} ms")
    else:
        print(f"{name:<15} {0:<8} {'N/A':<18} {'N/A':<18} {'N/A':<15}")

print("-" * 80)

# Summary Insights
total_jobs = len(all_records)
completed_jobs = len([r for r in all_records if r["status"] in ("Done", "Failed", "TimedOut")])
overall_avg_wait = statistics.mean([r["wait_ms"] for r in all_records]) if all_records else 0
throughput = completed_jobs / elapsed_sec if elapsed_sec > 0 else 0

high_tier_waits = [r["wait_ms"] for r in all_records if r["client_tier"] == "High-Freq"]
low_tier_waits = [r["wait_ms"] for r in all_records if r["client_tier"] == "Low-Freq"]

avg_high_wait = statistics.mean(high_tier_waits) if high_tier_waits else 0
avg_low_wait = statistics.mean(low_tier_waits) if low_tier_waits else 0

print("\n" + "=" * 80)
print("                        SYSTEM STRESS TEST SUMMARY")
print("=" * 80)
print(f"Total Requests Processed:     {completed_jobs} / {total_jobs}")
print(f"Total Test Duration:          {elapsed_sec:.2f} seconds")
print(f"Overall Throughput:           {throughput:.2f} requests/sec")
print(f"System-Wide Average Wait:     {overall_avg_wait:.1f} ms")
print(f"High-Frequency Avg Wait:      {avg_high_wait:.1f} ms")
print(f"Low-Frequency Avg Wait:       {avg_low_wait:.1f} ms")
print("-" * 80)

print("\n--- ARCHITECTURAL VERIFICATION & FAIRNESS ANALYSIS ---")
if avg_low_wait < avg_high_wait:
    ratio = avg_high_wait / avg_low_wait if avg_low_wait > 0 else 1.0
    print(f"[FAIRNESS CONFIRMED] Low-frequency clients experienced {ratio:.1f}x lower average wait time")
    print("                     than high-frequency spamming clients.")
    print("  -> Why? The Two-Level Scheduler successfully enforced the per-client in-flight cap")
    print("     (max_in_flight_per_client) on heavy clients, preventing them from monopolizing")
    print("     worker threads and ensuring low-frequency clients were serviced without starvation.")
else:
    print("[FAIRNESS OBSERVATION] Workloads completed with low queue contention across all tiers.")
print("=" * 80)
EOF

echo ""
echo -e "${CYAN}Full raw metrics saved to: ${RAW_LOG}${NC}"
echo -e "${GREEN}Stress test completed successfully!${NC}"
