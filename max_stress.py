#!/usr/bin/env python3
"""
max_stress.py - High-Throughput, Ultra-Low-CPU Stress Testing Harness

Features:
  - Generates massive multi-tenant load to saturate workers and sandboxes.
  - Zero-overhead async I/O with pre-encoded byte buffers (no JSON serialization in hot loop).
  - Pipelined client workers: keeps per-client in-flight slots 100% full at steady state.
  - Monitors client CPU usage via getrusage() to verify generator uses < 1-2% CPU.
  - Measures server-side and client-side p50, p90, p95, p99 latency & goodput.

Usage:
  python3 max_stress.py --clients 32 --duration 15
  python3 max_stress.py --clients 64 --duration 30 --workload mixed
"""

import argparse
import asyncio
import json
import math
import os
import resource
import sys
import time
from typing import List, Tuple

# Pre-compiled workloads for minimal string formatting overhead
WORKLOADS = {
    "micro": [
        (b"python", b"print(123456789 * 987654321)"),
        (b"python", b"print(sum(range(2000)))"),
        (b"sh", b"echo worker_job_done"),
        (b"python", b"import math; print(math.isqrt(9876543210123456))"),
    ],
    "mixed": [
        (b"python", b"print(sum(range(5000)))"),
        (b"python", b"p=[x for x in range(2, 3000) if all(x%d!=0 for d in range(2, int(x**0.5)+1))]; print(len(p))"),
        (b"python", b"import numpy as np; a = np.ones((50, 50)); print(int(np.sum(a)))"),
        (b"python", b"a,b=0,1; exec('for _ in range(1500): a,b=b,a+b'); print(len(str(b)))"),
        (b"sh", b"echo 'nsjail_load_bench' | tr 'a-z' 'A-Z'"),
    ],
    "heavy": [
        (b"python", b"import numpy as np; a = np.random.rand(100, 100); print(float(np.trace(a)))"),
        (b"python", b"p=[x for x in range(2, 8000) if all(x%d!=0 for d in range(2, int(x**0.5)+1))]; print(len(p))"),
        (b"python", b"sum(i*i for i in range(300000)); print('cpu_burn_done')"),
    ],
}

TERMINAL_STATUSES = (b"+Done", b"+Failed", b"+TimedOut")
DEFAULT_STRESS_LOG_FILE = "stress_job_results.log"


def init_or_reset_stress_log(path: str = DEFAULT_STRESS_LOG_FILE):
    """Deletes previous stress test logs if they exist so this run starts fresh."""
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def log_stress_job(f, job_id: str, status: str, code: bytes, stderr: str):
    """Logs job outcome recording only stderr and stdin/code (explicitly no time/timestamps)."""
    if f is None:
        return
    code_str = code.decode("utf-8", errors="replace")
    f.write(
        f"================================================================================\n"
        f"Job ID:       {job_id}\n"
        f"Status:       {status}\n"
        f"--- STDIN / CODE ---\n{code_str}\n"
        f"--- STDERR ---\n{stderr.strip()}\n"
        f"================================================================================\n\n"
    )


class LatencyBreakdown:
    def __init__(self):
        self.client: List[float] = []
        self.server: List[float] = []
        self.queue: List[float] = []
        self.exec: List[float] = []

    def sort_all(self):
        self.client.sort()
        self.server.sort()
        self.queue.sort()
        self.exec.sort()


def calculate_percentile(sorted_list: List[float], p: float) -> float:
    if not sorted_list:
        return 0.0
    idx = max(0, math.ceil(p * len(sorted_list)) - 1)
    return sorted_list[idx]


def calculate_average(lst: List[float]) -> float:
    return sum(lst) / len(lst) if lst else 0.0


class StressClient:
    def __init__(self, client_idx: int, host: str, port: int, workload: List[Tuple[bytes, bytes]], in_flight_cap: int):
        self.idx = client_idx
        self.host = host
        self.port = port
        self.workload = workload
        self.in_flight_cap = in_flight_cap
        self.key: bytes = b""
        self.reader: asyncio.StreamReader = None
        self.writer: asyncio.StreamWriter = None
        self.lock = asyncio.Lock()
        self.next_req_id = 0

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        # Register to receive secret_key
        self.writer.write(b"REGISTER\n")
        await self.writer.drain()
        line = await self.reader.readline()
        if not line.startswith(b"+sk_"):
            raise RuntimeError(f"Client {self.idx} failed to register: {line.decode(errors='ignore')}")
        self.key = line.strip()[1:]  # strip '+' and trailing newline

    async def send_cmd(self, cmd_bytes: bytes) -> bytes:
        async with self.lock:
            self.writer.write(cmd_bytes)
            await self.writer.drain()
            return await self.reader.readline()

    async def run_worker_loop(self, stop_time: float, latencies: LatencyBreakdown, stats: dict, log_f=None):
        while time.monotonic() < stop_time:
            self.next_req_id += 1
            lang, code = self.workload[self.next_req_id % len(self.workload)]

            # Pre-encoded fast submit command
            submit_cmd = b"SUBMIT " + self.key + b" " + lang + b" " + code + b"\n"
            t0 = time.monotonic()
            resp = await self.send_cmd(submit_cmd)

            if not resp.startswith(b":"):
                if b"rate limit" in resp.lower() or b"queue" in resp.lower():
                    stats["rejected"] += 1
                else:
                    stats["errors"] += 1
                await asyncio.sleep(0.05)
                continue

            job_id = resp.strip()[1:]
            stats["submitted"] += 1

            # Paced poll loop
            poll_cmd = b"POLL " + self.key + b" " + job_id + b"\n"
            poll_sleep = 0.010  # start at 10ms

            while time.monotonic() < stop_time + 10.0:
                await asyncio.sleep(poll_sleep)
                poll_resp = await self.send_cmd(poll_cmd)

                if any(poll_resp.startswith(term) for term in TERMINAL_STATUSES):
                    duration_ms = (time.monotonic() - t0) * 1000.0
                    latencies.client.append(duration_ms)
                    stats["completed"] += 1

                    # Extract section-wise latency breakdown from JSON (only once per terminal job)
                    first_space = poll_resp.find(b" ")
                    if first_space != -1:
                        try:
                            data = json.loads(poll_resp[first_space + 1:])
                            created_at = data.get("created_at_ms")
                            completed_at = data.get("completed_at_ms")
                            exec_dur = data.get("execution_duration_ms") or 0.0

                            if created_at is not None and completed_at is not None:
                                server_total = max(0.0, float(completed_at - created_at))
                                exec_ms = float(exec_dur)
                                queue_ms = max(0.0, server_total - exec_ms)

                                latencies.server.append(server_total)
                                latencies.exec.append(exec_ms)
                                latencies.queue.append(queue_ms)

                            # Log job diagnostics (only stdin/code and stderr; no time)
                            if log_f:
                                log_stress_job(
                                    log_f,
                                    data.get("job_id", ""),
                                    data.get("status", ""),
                                    code,
                                    data.get("stderr", "") or "",
                                )
                        except Exception:
                            pass
                    break

                if b"poll rate limit" in poll_resp.lower():
                    poll_sleep = 0.050
                    continue

                poll_sleep = min(poll_sleep * 1.5, 0.040)  # cap at 40ms (~25 polls/sec limit)

    async def close(self):
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass


async def run_stress_test(args):
    workload = WORKLOADS.get(args.workload, WORKLOADS["mixed"])
    latencies = LatencyBreakdown()
    stats = {"submitted": 0, "completed": 0, "rejected": 0, "errors": 0}

    # Reset any previous log file so this run logs fresh
    log_f = None
    if not getattr(args, "no_log", False):
        init_or_reset_stress_log(args.log_file)
        log_f = open(args.log_file, "a", encoding="utf-8", buffering=65536)

    print(f"Connecting {args.clients} concurrent tenant clients to {args.host}:{args.port}...")
    clients = [StressClient(i, args.host, args.port, workload, args.in_flight) for i in range(args.clients)]

    try:
        await asyncio.gather(*(c.connect() for c in clients))
    except Exception as e:
        print(f"\n[ERROR] Failed to connect to broker at {args.host}:{args.port}: {e}")
        print("Ensure the broker is running before starting the stress test.")
        if log_f:
            log_f.close()
        return

    print(f"All {args.clients} clients authenticated. Starting {args.duration}s max stress burst...")
    t_start = time.monotonic()
    t_stop = t_start + args.duration

    # Measure CPU consumption of the generator itself
    rusage_start = resource.getrusage(resource.RUSAGE_SELF)

    # Launch concurrent worker pipelines per client (each running in_flight concurrent tasks)
    tasks = []
    for c in clients:
        for _ in range(args.in_flight):
            tasks.append(asyncio.create_task(c.run_worker_loop(t_stop, latencies, stats, log_f)))

    # Periodic live status reporter
    while time.monotonic() < t_stop:
        await asyncio.sleep(1.0)
        elapsed = time.monotonic() - t_start
        done = stats["completed"]
        rate = done / elapsed if elapsed > 0 else 0
        sys.stdout.write(
            f"\r\033[K[Time: {elapsed:4.1f}s / {args.duration}s] Completed: {done:5d} jobs | "
            f"Throughput: {rate:6.1f} jobs/s | Rejections: {stats['rejected']:3d}"
        )
        sys.stdout.flush()

    # Wait for in-flight tasks to wrap up
    await asyncio.gather(*tasks, return_exceptions=True)
    t_end = time.monotonic()
    wall_duration = t_end - t_start

    # Flush and close the log file
    if log_f:
        log_f.flush()
        log_f.close()

    # Calculate exact CPU usage of generator
    rusage_end = resource.getrusage(resource.RUSAGE_SELF)
    user_cpu = rusage_end.ru_utime - rusage_start.ru_utime
    sys_cpu = rusage_end.ru_stime - rusage_start.ru_stime
    total_cpu = user_cpu + sys_cpu
    cpu_percent = (total_cpu / wall_duration) * 100.0 if wall_duration > 0 else 0.0

    # Cleanup connections
    await asyncio.gather(*(c.close() for c in clients), return_exceptions=True)

    latencies.sort_all()
    completed = stats["completed"]
    goodput = completed / wall_duration if wall_duration > 0 else 0.0

    def fmt_row(name: str, metric_list: List[float]) -> str:
        if not metric_list:
            return f"  {name:<26} {'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8} {'N/A':>8}"
        return (
            f"  {name:<26} "
            f"{calculate_average(metric_list):>7.1f}ms "
            f"{calculate_percentile(metric_list, 0.50):>7.1f}ms "
            f"{calculate_percentile(metric_list, 0.90):>7.1f}ms "
            f"{calculate_percentile(metric_list, 0.95):>7.1f}ms "
            f"{calculate_percentile(metric_list, 0.99):>7.1f}ms "
            f"{calculate_percentile(metric_list, 1.00):>7.1f}ms"
        )

    print("\n\n" + "=" * 80)
    print("                    MAX STRESS BENCHMARK RESULTS")
    print("=" * 80)
    print(f"  Test Duration:       {wall_duration:.2f} s")
    print(f"  Concurrent Clients:  {args.clients} tenants (x {args.in_flight} in-flight slots = {args.clients * args.in_flight} concurrent)")
    print(f"  Workload Profile:    {args.workload.upper()}")
    print("-" * 80)
    print(f"  Total Submitted:     {stats['submitted']}")
    print(f"  Total Completed:     {completed} ({goodput:.1f} jobs/s goodput)")
    print(f"  Admission Rejections:{stats['rejected']}")
    print(f"  Execution Errors:    {stats['errors']}")
    print("-" * 80)
    print("  SECTION-WISE LATENCY BREAKDOWN:")
    print(f"  {'Metric':<26} {'Avg':>8} {'p50':>8} {'p90':>8} {'p95':>8} {'p99':>8} {'Max':>8}")
    print("  " + "-" * 76)
    print(fmt_row("Queue Latency (Wait)", latencies.queue))
    print(fmt_row("Execution Latency (Run)", latencies.exec))
    print(fmt_row("Server Total (Q + Exec)", latencies.server))
    print(fmt_row("Client Turnaround (E2E)", latencies.client))
    print("-" * 80)
    print("  Latency Section Definitions:")
    print("    • Queue Latency:  Time spent waiting in scheduler FIFO queue before worker pickup.")
    print("    • Exec Latency:   Time executing inside the sandbox container/jail.")
    print("    • Server Total:   Broker lifecycle duration (completed_at_ms - created_at_ms).")
    print("    • Client E2E:     Total wall-clock time perceived by client (Submit + Q + Exec + Poll).")
    print("-" * 80)
    if not getattr(args, "no_log", False):
        print(f"  Diagnostics Log:     {args.log_file} (stdin/code + stderr recorded; no time)")
    print("  Generator Efficiency & Resource Impact:")
    print(f"    Generator CPU:     {cpu_percent:.2f}% of 1 CPU core")
    print(f"    User CPU Time:     {user_cpu:.3f} s")
    print(f"    System CPU Time:   {sys_cpu:.3f} s")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="High-Throughput Ultra-Low-CPU Stress Testing Harness")
    parser.add_argument("--host", default="127.0.0.1", help="Broker host address (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7000, help="Broker port (default: 7000)")
    parser.add_argument("--clients", type=int, default=32, help="Number of concurrent tenant connections (default: 32)")
    parser.add_argument("--in-flight", type=int, default=2, help="In-flight jobs maintained per client (default: 2)")
    parser.add_argument("--duration", type=int, default=15, help="Test duration in seconds (default: 15)")
    parser.add_argument("--workload", choices=["micro", "mixed", "heavy"], default="mixed", help="Workload profile")
    parser.add_argument("--log-file", default=DEFAULT_STRESS_LOG_FILE, help="File to log stdin/stderr for jobs (default: stress_job_results.log)")
    parser.add_argument("--no-log", action="store_true", help="Disable writing job diagnostic log")
    args = parser.parse_args()

    asyncio.run(run_stress_test(args))


if __name__ == "__main__":
    main()
