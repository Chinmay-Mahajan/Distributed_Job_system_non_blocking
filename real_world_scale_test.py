#!/usr/bin/env python3
"""
Real-World Multi-Tenant Traffic & Drastic Client Scaling Test Suite.
For Sandboxed Job Execution Broker (v2) [Supports Sync & Async Tokio Models].

Simulates realistic heterogeneous traffic patterns across drastically scaling client populations:
  - Multi-Stage Ramp-up: 50 -> 150 -> 300 -> 600 -> 1,000+ concurrent clients
  - Realistic Client Tiers:
      * PoliteDev (40%): Real human developer typing & reading output (1.0s - 2.5s think time)
      * RapidCI (35%): Automated test pipelines submitting frequent test batches (100ms - 300ms think time)
      * BurstContestant (15%): Competitive coders bursting 3-5 submissions simultaneously
      * HeavyCompute (10%): Data scientists submitting compute-intensive algorithms
      * IdleLurkers: Persistent background connections holding open TCP sockets
  - Millisecond-Precision Metrics:
      * Turnaround Time (P50, P90, P95, P99, Min, Mean, Max)
      * Phase Latency Decomposition: Queue Wait (W_q), Execution (T_s), Network/Polling Overhead
      * Completed Jobs/sec vs. Raw Broker Requests/sec (SUBMIT + POLL)
      * Live Server Health: OS Threads, Physical RSS (MB), Virtual Address Space VSZ (GB)
"""

import argparse
import csv
import json
import math
import os
import random
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ANSI Colors
BOLD = "\033[1m"
GREEN = "\033[32m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
RED = "\033[31m"
MAGENTA = "\033[35m"
RESET = "\033[0m"

# ==============================================================================
# REALISTIC ALGORITHMIC WORKLOAD LIBRARY
# ==============================================================================
WORKLOADS = {
    "easy": [
        ("TwoSum", "python", "nums = list(range(2000)); target = 3997; seen = {}; print([seen[target-x] for x in nums if target-x in seen or not seen.setdefault(x, len(seen))][:1])"),
        ("StringReverse", "python", "s = 'distributed_job_v2_' * 100; print(s[::-1][:20])"),
        ("DictComprehension", "python", "d = {x: x**2 for x in range(500)}; print(len(d), sum(d.values()))"),
        ("SimpleMath", "python", "import math; print(sum(math.isqrt(i) for i in range(10000)))"),
    ],
    "medium": [
        ("QuickSort", "python", "import random; a = list(range(3000)); random.seed(42); random.shuffle(a); a.sort(); print('sorted_len:', len(a))"),
        ("PrimeSieve", "python", "n = 15000; p = [True]*n; p[0]=p[1]=False\nfor i in range(2, int(n**0.5)+1):\n if p[i]: p[i*i:n:i] = [False]*len(p[i*i:n:i])\nprint('primes:', sum(p))"),
        ("FibonacciBigInt", "python", "a, b = 0, 1\nfor _ in range(4000): a, b = b, a + b\nprint('fib_digits:', len(str(b)))"),
    ],
    "heavy": [
        ("MatrixMultiplication", "python", "N = 80; A = [[i+j for j in range(N)] for i in range(N)]; B = [[i*j for j in range(N)] for i in range(N)]; C = [[sum(A[i][k]*B[k][j] for k in range(N)) for j in range(N)] for i in range(N)]; print('matrix_trace:', sum(C[i][i] for i in range(N)))"),
        ("RecursiveKnapsack", "python", "weights = [2, 3, 4, 5, 9, 7, 8, 12, 14, 11]; values = [3, 4, 8, 8, 10, 11, 13, 15, 18, 20]; W = 35\ndef knap(i, w):\n if i < 0 or w <= 0: return 0\n if weights[i] > w: return knap(i-1, w)\n return max(knap(i-1, w), values[i] + knap(i-1, w - weights[i]))\nprint('max_val:', knap(len(weights)-1, W))"),
    ],
}


@dataclass
class JobRecord:
    client_id: str
    tier: str
    job_id: str
    difficulty: str
    turnaround_ms: float
    queue_wait_ms: float
    exec_duration_ms: float
    net_poll_overhead_ms: float
    polls: int
    exit_code: Optional[int]


def get_server_metrics(pid: int) -> Tuple[int, float, float]:
    """Returns (threads, rss_mb, vsz_gb) for the target server process."""
    if pid <= 0:
        return 0, 0.0, 0.0
    threads = 0
    rss_mb = 0.0
    vsz_gb = 0.0
    try:
        if sys.platform == "darwin":
            out_threads = subprocess.check_output(["ps", "-M", str(pid)], text=True, stderr=subprocess.DEVNULL)
            threads = max(0, len(out_threads.strip().split("\n")) - 1)
        else:
            threads = len(os.listdir(f"/proc/{pid}/task"))
    except Exception:
        pass

    try:
        out_mem = subprocess.check_output(["ps", "-o", "rss,vsz", "-p", str(pid)], text=True, stderr=subprocess.DEVNULL)
        lines = out_mem.strip().split("\n")
        if len(lines) > 1:
            parts = lines[1].split()
            rss_mb = float(parts[0]) / 1024.0
            vsz_gb = float(parts[1]) / (1024.0 * 1024.0)
    except Exception:
        pass
    return threads, rss_mb, vsz_gb


def find_server_pid(port: int) -> int:
    try:
        out = subprocess.check_output(["lsof", "-ti", f":{port}", "-sTCP:LISTEN"], text=True, stderr=subprocess.DEVNULL)
        pids = out.strip().split()
        if pids:
            return int(pids[0])
    except Exception:
        pass
    return 0


class TenantWorker(threading.Thread):
    def __init__(
        self,
        client_id: str,
        tier: str,
        host: str,
        port: int,
        stop_event: threading.Event,
        poll_interval: float,
        records_list: List[JobRecord],
        records_lock: threading.Lock,
        poll_counter: List[int],
    ):
        super().__init__()
        self.daemon = True
        self.client_id = client_id
        self.tier = tier
        self.host = host
        self.port = port
        self.stop_event = stop_event
        self.poll_interval = poll_interval
        self.records_list = records_list
        self.records_lock = records_lock
        self.poll_counter = poll_counter

    def run(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(8.0)
            s.connect((self.host, self.port))
            reader = s.makefile("r")

            # Register to obtain server-issued secret_key
            reg_req = json.dumps({"action": "REGISTER"}) + "\n"
            s.sendall(reg_req.encode())
            reg_line = reader.readline()
            if not reg_line:
                s.close()
                return
            reg_resp = json.loads(reg_line)
            secret_key = reg_resp.get("secret_key")
            if not secret_key:
                s.close()
                return
        except Exception:
            return

        while not self.stop_event.is_set():
            # 1. Determine workload and think time based on client tier
            if self.tier == "PoliteDev":
                diff = random.choices(["easy", "medium"], weights=[0.8, 0.2])[0]
                name, lang, code = random.choice(WORKLOADS[diff])
                think_time = random.uniform(0.8, 2.0)
                burst_size = 1
            elif self.tier == "RapidCI":
                diff = random.choices(["easy", "medium"], weights=[0.9, 0.1])[0]
                name, lang, code = random.choice(WORKLOADS[diff])
                think_time = random.uniform(0.05, 0.25)
                burst_size = 1
            elif self.tier == "BurstContestant":
                diff = random.choices(["easy", "medium"], weights=[0.7, 0.3])[0]
                name, lang, code = random.choice(WORKLOADS[diff])
                think_time = random.uniform(1.0, 2.5)
                burst_size = random.randint(2, 4)
            elif self.tier == "HeavyCompute":
                diff = random.choices(["medium", "heavy"], weights=[0.3, 0.7])[0]
                name, lang, code = random.choice(WORKLOADS[diff])
                think_time = random.uniform(0.4, 1.2)
                burst_size = 1
            else:
                diff = "easy"
                name, lang, code = random.choice(WORKLOADS["easy"])
                think_time = 1.0
                burst_size = 1

            for _ in range(burst_size):
                if self.stop_event.is_set():
                    break

                t_submit = time.time()
                submit_req = json.dumps({
                    "action": "SUBMIT",
                    "secret_key": secret_key,
                    "language": lang,
                    "code": code,
                    "timeout_secs": 15,
                }) + "\n"

                try:
                    s.sendall(submit_req.encode())
                    resp_line = reader.readline()
                    if not resp_line:
                        s.close()
                        return
                    resp = json.loads(resp_line)
                    if resp.get("status") != "ok":
                        time.sleep(0.05)
                        continue
                    job_id = resp["job_id"]
                except Exception:
                    s.close()
                    return

                # Polling loop: once submitted, poll until Done or timeout, even if stop_event is set!
                polls = 0
                t_poll_start = time.time()
                while time.time() - t_poll_start < 45.0:
                    time.sleep(self.poll_interval)
                    polls += 1

                    poll_req = json.dumps({"action": "POLL", "secret_key": secret_key, "job_id": job_id}) + "\n"
                    try:
                        s.sendall(poll_req.encode())
                        line = reader.readline()
                        if not line:
                            s.close()
                            return
                        poll_resp = json.loads(line)
                        if poll_resp.get("status") == "error" and "Poll rate limit" in poll_resp.get("error", ""):
                            time.sleep(0.20)
                            continue
                        st = poll_resp.get("status")
                        if st in ("Done", "Failed", "TimedOut"):
                            t_done = time.time()
                            turnaround_ms = (t_done - t_submit) * 1000.0
                            created_at = poll_resp.get("created_at_ms", 0)
                            completed_at = poll_resp.get("completed_at_ms", 0)
                            exec_dur = poll_resp.get("execution_duration_ms") or 0

                            server_turnaround = max(0, completed_at - created_at) if (created_at and completed_at) else 0
                            queue_wait = max(0, server_turnaround - exec_dur)
                            net_poll_overhead = max(0.0, turnaround_ms - server_turnaround)

                            record = JobRecord(
                                client_id=self.client_id,
                                tier=self.tier,
                                job_id=job_id,
                                difficulty=diff,
                                turnaround_ms=turnaround_ms,
                                queue_wait_ms=queue_wait,
                                exec_duration_ms=exec_dur,
                                net_poll_overhead_ms=net_poll_overhead,
                                polls=polls,
                                exit_code=poll_resp.get("exit_code"),
                            )
                            with self.records_lock:
                                self.records_list.append(record)
                                self.poll_counter[0] += polls
                            break
                    except Exception:
                        s.close()
                        return

            # Realistic Think Time between bursts / submissions
            if not self.stop_event.is_set() and think_time > 0:
                time.sleep(think_time)

        try:
            s.close()
        except Exception:
            pass


def run_stage(
    host: str,
    port: int,
    num_clients: int,
    idle_lurkers: int,
    duration: float,
    poll_interval: float,
    server_pid: int,
) -> Dict[str, Any]:
    print(f"\n{BOLD}{CYAN}--------------------------------------------------------------------------------{RESET}")
    print(f"{BOLD}STAGE: {num_clients} Active Real-World Tenants + {idle_lurkers} Idle Lurkers ({duration}s test){RESET}")
    print(f"{CYAN}--------------------------------------------------------------------------------{RESET}")

    # 1. Establish idle lurker sockets
    lurker_sockets = []
    if idle_lurkers > 0:
        for _ in range(idle_lurkers):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5.0)
                s.connect((host, port))
                lurker_sockets.append(s)
            except Exception:
                pass

    # 2. Divide active clients into realistic tiers
    # 40% PoliteDev, 35% RapidCI, 15% BurstContestant, 10% HeavyCompute
    polite = max(1, int(round(num_clients * 0.40)))
    rapid = max(1, int(round(num_clients * 0.35)))
    burst = max(1, int(round(num_clients * 0.15)))
    heavy = max(1, num_clients - (polite + rapid + burst))

    stop_event = threading.Event()
    records: List[JobRecord] = []
    records_lock = threading.Lock()
    poll_counter = [0]
    threads = []

    client_id_counter = 0
    for _ in range(polite):
        client_id_counter += 1
        t = TenantWorker(f"polite-{client_id_counter}", "PoliteDev", host, port, stop_event, poll_interval, records, records_lock, poll_counter)
        threads.append(t)
    for _ in range(rapid):
        client_id_counter += 1
        t = TenantWorker(f"rapid-{client_id_counter}", "RapidCI", host, port, stop_event, poll_interval, records, records_lock, poll_counter)
        threads.append(t)
    for _ in range(burst):
        client_id_counter += 1
        t = TenantWorker(f"burst-{client_id_counter}", "BurstContestant", host, port, stop_event, poll_interval, records, records_lock, poll_counter)
        threads.append(t)
    for _ in range(heavy):
        client_id_counter += 1
        t = TenantWorker(f"heavy-{client_id_counter}", "HeavyCompute", host, port, stop_event, poll_interval, records, records_lock, poll_counter)
        threads.append(t)

    t_start = time.time()
    for t in threads:
        t.start()
        time.sleep(0.002)

    # Sample server metrics during peak traffic
    time.sleep(duration / 2.0)
    srv_threads, srv_rss, srv_vsz = get_server_metrics(server_pid)

    time.sleep(duration / 2.0)
    stop_event.set()
    print("  [i] Waiting for in-flight jobs to complete...")
    for t in threads:
        t.join(timeout=25.0)

    # Close lurkers
    for s in lurker_sockets:
        try:
            s.close()
        except Exception:
            pass

    elapsed = time.time() - t_start
    total_jobs = len(records)
    total_polls = poll_counter[0]
    total_commands = total_jobs + total_polls
    jobs_per_sec = total_jobs / elapsed if elapsed > 0 else 0
    reqs_per_sec = total_commands / elapsed if elapsed > 0 else 0
    avg_polls = total_polls / total_jobs if total_jobs > 0 else 0

    # Percentiles
    def calc_percentiles(vals: List[float]) -> Dict[str, float]:
        if not vals:
            return {"min": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0, "max": 0, "mean": 0}
        vals.sort()
        n = len(vals)
        def p(pct):
            k = (n - 1) * (pct / 100.0)
            f = int(k)
            c = min(f + 1, n - 1)
            return vals[f] * (1 - (k - f)) + vals[c] * (k - f)
        return {
            "min": round(vals[0], 1),
            "p50": round(p(50), 1),
            "p90": round(p(90), 1),
            "p95": round(p(95), 1),
            "p99": round(p(99), 1),
            "max": round(vals[-1], 1),
            "mean": round(sum(vals) / n, 1),
        }

    turnarounds = calc_percentiles([r.turnaround_ms for r in records])
    queues = calc_percentiles([r.queue_wait_ms for r in records])
    execs = calc_percentiles([r.exec_duration_ms for r in records])
    net_polls = calc_percentiles([r.net_poll_overhead_ms for r in records])

    # Print summary
    print(f"  {BOLD}Throughput & Capacity:{RESET}")
    print(f"    • Completed Jobs:       {GREEN}{total_jobs}{RESET} ({jobs_per_sec:.1f} jobs/sec)")
    print(f"    • Raw Broker Commands:  {CYAN}{total_commands}{RESET} ({reqs_per_sec:.1f} reqs/sec)")
    print(f"    • Avg Polls per Job:    {avg_polls:.1f} polls")
    print(f"  {BOLD}Latency Decomposition (ms):{RESET}")
    print(f"    • Turnaround (Client):  P50={turnarounds['p50']}ms | P90={turnarounds['p90']}ms | P99={turnarounds['p99']}ms | Max={turnarounds['max']}ms")
    print(f"    • Queue Wait (W_q):     P50={queues['p50']}ms | P90={queues['p90']}ms | P99={queues['p99']}ms")
    print(f"    • Sandbox Exec (T_s):   P50={execs['p50']}ms | P90={execs['p90']}ms | P99={execs['p99']}ms")
    print(f"    • Network & Polling:    P50={net_polls['p50']}ms | P90={net_polls['p90']}ms | P99={net_polls['p99']}ms")
    if server_pid > 0:
        print(f"  {BOLD}Server Health under Load (PID {server_pid}):{RESET}")
        print(f"    • OS Threads:           {MAGENTA}{srv_threads}{RESET} threads")
        print(f"    • Resident RAM (RSS):   {srv_rss:.1f} MB")
        print(f"    • Virtual Address Space:{srv_vsz:.2f} GB")

    return {
        "clients": num_clients,
        "lurkers": idle_lurkers,
        "total_jobs": total_jobs,
        "jobs_per_sec": jobs_per_sec,
        "reqs_per_sec": reqs_per_sec,
        "avg_polls": avg_polls,
        "turnaround": turnarounds,
        "queue_wait": queues,
        "exec_duration": execs,
        "server_threads": srv_threads,
        "server_rss_mb": srv_rss,
        "server_vsz_gb": srv_vsz,
        "records": records,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Drastic Multi-Tenant Client Scaling Benchmark for Distributed Job Broker (Sync vs Async)"
    )
    parser.add_argument("--host", default="127.0.0.1", help="Broker server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7000, help="Broker server port (default: 7000)")
    parser.add_argument(
        "--stages",
        default="50,150,300,600",
        help="Comma-separated list of active client scales (e.g. '50,150,300,600,1000')",
    )
    parser.add_argument(
        "--lurkers",
        type=int,
        default=50,
        help="Persistent idle connections maintained alongside active clients (default: 50)",
    )
    parser.add_argument("--duration", type=float, default=10.0, help="Seconds to test each client scale (default: 10s)")
    parser.add_argument("--poll-interval", type=float, default=0.05, help="Polling interval in seconds (default: 0.05s = 50ms)")
    parser.add_argument("--server-pid", type=int, default=0, help="Broker process PID for tracking threads/memory (auto-detected if 0)")
    parser.add_argument("--csv", default="", help="Optional CSV file path to export all individual job latency records")

    args = parser.parse_args()

    server_pid = args.server_pid
    if server_pid == 0:
        server_pid = find_server_pid(args.port)

    # Verify server connectivity
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3.0)
        s.connect((args.host, args.port))
        s.sendall(b"PING\n")
        resp = s.recv(1024).decode()
        s.close()
        if "PONG" not in resp:
            print(f"{RED}[!] Server at {args.host}:{args.port} did not reply with PONG.{RESET}")
            sys.exit(1)
    except Exception as e:
        print(f"{RED}[!] Failed to connect to broker at {args.host}:{args.port}: {e}{RESET}")
        sys.exit(1)

    print(f"{BOLD}{GREEN}================================================================================{RESET}")
    print(f"{BOLD}{GREEN}          REAL-WORLD CLIENT SCALING & HIGH-CONCURRENCY BENCHMARK                {RESET}")
    print(f"{BOLD}{GREEN}================================================================================{RESET}")
    print(f"  Target Server:     {BOLD}{args.host}:{args.port}{RESET}")
    if server_pid > 0:
        init_threads, init_rss, init_vsz = get_server_metrics(server_pid)
        print(f"  Target PID:        {BOLD}{server_pid}{RESET} (Initial: {init_threads} threads, {init_rss:.1f} MB RSS, {init_vsz:.2f} GB VSZ)")
    print(f"  Scaling Stages:    {BOLD}{args.stages}{RESET} concurrent active tenants")
    print(f"  Persistent Lurkers:{BOLD}{args.lurkers}{RESET} idle connections")
    print(f"  Stage Duration:    {BOLD}{args.duration}s{RESET} per stage")
    print(f"  Client Mix:        40% PoliteDev, 35% RapidCI, 15% BurstContestant, 10% HeavyCompute")

    stages = [int(x.strip()) for x in args.stages.split(",") if x.strip()]
    stage_results = []
    all_records: List[JobRecord] = []

    for stage_clients in stages:
        res = run_stage(
            host=args.host,
            port=args.port,
            num_clients=stage_clients,
            idle_lurkers=args.lurkers,
            duration=args.duration,
            poll_interval=args.poll_interval,
            server_pid=server_pid,
        )
        stage_results.append(res)
        all_records.extend(res["records"])
        time.sleep(1.0)  # Cool down between stages

    # ==============================================================================
    # FINAL COMPARATIVE STAGE MATRIX
    # ==============================================================================
    print(f"\n{BOLD}{GREEN}=================================================================================================================={RESET}")
    print(f"{BOLD}{GREEN}                                       SCALING SUMMARY MATRIX ACROSS ALL STAGES                                   {RESET}")
    print(f"{BOLD}{GREEN}=================================================================================================================={RESET}")
    print(f"{BOLD}{'CLIENTS':>8} | {'JOBS/S':>8} | {'REQ/S':>8} | {'P50 (ms)':>9} | {'P90 (ms)':>9} | {'P99 (ms)':>9} | {'MAX (ms)':>9} | {'THREADS':>8} | {'RSS (MB)':>9}{RESET}")
    print("------------------------------------------------------------------------------------------------------------------")
    for r in stage_results:
        t = r["turnaround"]
        print(f"{r['clients']:>8} | {r['jobs_per_sec']:>8.1f} | {r['reqs_per_sec']:>8.1f} | {t['p50']:>9.1f} | {t['p90']:>9.1f} | {t['p99']:>9.1f} | {t['max']:>9.1f} | {r['server_threads']:>8} | {r['server_rss_mb']:>8.1f}M")
    print("------------------------------------------------------------------------------------------------------------------")

    # Export CSV if requested
    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["client_id", "tier", "job_id", "difficulty", "turnaround_ms", "queue_wait_ms", "exec_duration_ms", "net_poll_overhead_ms", "polls", "exit_code"])
            for rec in all_records:
                writer.writerow([rec.client_id, rec.tier, rec.job_id, rec.difficulty, rec.turnaround_ms, rec.queue_wait_ms, rec.exec_duration_ms, rec.net_poll_overhead_ms, rec.polls, rec.exit_code])
        print(f"\n[✓] Exported {len(all_records)} job records to {args.csv}")

if __name__ == "__main__":
    main()
