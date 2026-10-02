#!/usr/bin/env python3
"""
Long-Duration Real-World Traffic Benchmark & Multi-Tenant Soak Testing Suite
for Sandboxed Job Execution Broker.

Features:
  - High-frequency saturation modes (keeps broker workers near 100% utilization)
  - Fully customizable client populations (--clients N or granular tier flags)
  - 17 diverse algorithmic workloads (DP, Graphs, Heaps, Stacks, Math, Cache, Shell)
  - Real-time Live Worker Utilization % tracking (e.g. 100.0% 16/16 workers)
  - Full distinction between Completed Jobs/sec vs Raw Broker Commands/sec (SUBMIT + POLLs)
  - Millisecond-precision phase latency decomposition:
      * Client Turnaround Time (Perceived)
      * Queue Wait Time (W_q)
      * Sandbox Execution Duration (T_s)
      * Network & Polling Overhead
  - Comprehensive percentile metrics (Min, Mean, P50, P75, P90, P95, P99, P99.9, Max)
  - Live 15-second heartbeat telemetry
  - Raw CSV trace export
"""

import argparse
import csv
import json
import math
import os
import random
import socket
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ==============================================================================
# EXPANDED ALGORITHMIC WORKLOAD LIBRARY (17 DIVERSE PROBLEMS)
# ==============================================================================

WORKLOADS = {
    # --------------------------------------------------------------------------
    # 1. LIGHT / QUICK VALIDATIONS (10 - 40 ms)
    # --------------------------------------------------------------------------
    "quick_two_sum": {
        "difficulty": "Easy",
        "category": "Hash Table",
        "description": "LC#1 Two Sum (Hash Table, N=5,000)",
        "language": "python",
        "code": """\
nums = list(range(5000))
target = 9997
seen = {}
found = None
for i, x in enumerate(nums):
    diff = target - x
    if diff in seen:
        found = (seen[diff], i)
        break
    seen[x] = i
assert found == (4998, 4999)
print(f"PASS: TwoSum indices {found}")
""",
    },
    "quick_valid_paren": {
        "difficulty": "Easy",
        "category": "Stack",
        "description": "LC#20 Valid Parentheses (N=20,000 brackets)",
        "language": "python",
        "code": """\
brackets = "({[]})" * 3000
stack = []
mapping = {')': '(', '}': '{', ']': '['}
for c in brackets:
    if c in mapping:
        top = stack.pop() if stack else '#'
        if mapping[c] != top:
            print("FAIL")
            exit(1)
    else:
        stack.append(c)
assert not stack
print(f"PASS: Validated {len(brackets)} brackets")
""",
    },
    "quick_math_primes": {
        "difficulty": "Easy",
        "category": "Math",
        "description": "Integer Square Root & Prime Check",
        "language": "python",
        "code": """\
import math
def is_prime(n):
    if n < 2: return False
    for i in range(2, math.isqrt(n) + 1):
        if n % i == 0: return False
    return True
primes = [x for x in range(1000, 3000) if is_prime(x)]
print(f"PASS: Found {len(primes)} primes")
""",
    },
    "quick_house_robber": {
        "difficulty": "Easy",
        "category": "DP 1D",
        "description": "LC#198 House Robber (N=10,000 houses)",
        "language": "python",
        "code": """\
import random
random.seed(99)
nums = [random.randint(1, 100) for _ in range(10000)]
prev1, prev2 = 0, 0
for x in nums:
    prev1, prev2 = max(prev2 + x, prev1), prev1
print(f"PASS: Max loot = {prev1} across {len(nums)} houses")
""",
    },
    "quick_shell_log": {
        "difficulty": "Easy",
        "category": "Shell Pipeline",
        "description": "POSIX Shell Log Aggregation & Filter",
        "language": "sh",
        "code": """\
for i in $(seq 1 1000); do
    echo "2026-10-01 12:00:0$((i%10)) status=$((200 + (i%5)*100))"
done | grep -E "status=(200|404)" | wc -l | awk '{print "PASS: Filtered log lines:", $1}'
""",
    },

    # --------------------------------------------------------------------------
    # 2. MEDIUM WORKLOADS (80 - 250 ms)
    # --------------------------------------------------------------------------
    "med_coin_change": {
        "difficulty": "Medium",
        "category": "DP Knapsack",
        "description": "LC#322 Coin Change (DP, Target=25,000)",
        "language": "python",
        "code": """\
coins = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]
amount = 25000
dp = [float('inf')] * (amount + 1)
dp[0] = 0
for coin in coins:
    for x in range(coin, amount + 1):
        if dp[x - coin] + 1 < dp[x]:
            dp[x] = dp[x - coin] + 1
print(f"PASS: Min coins for {amount} is {dp[amount]}")
""",
    },
    "med_merge_k_lists": {
        "difficulty": "Medium",
        "category": "Min-Heap",
        "description": "LC#23 Merge K Sorted Lists (50k elements)",
        "language": "python",
        "code": """\
import random, heapq
random.seed(777)
lists = [sorted([random.randint(1, 500000) for _ in range(1000)]) for _ in range(50)]
merged = list(heapq.merge(*lists))
assert len(merged) == 50000
assert all(merged[i] <= merged[i+1] for i in range(len(merged)-1))
print(f"PASS: Merged {len(merged)} elements from 50 sorted lists")
""",
    },
    "med_islands_bfs": {
        "difficulty": "Medium",
        "category": "Graph BFS",
        "description": "LC#200 Number of Islands (BFS, 120x120 grid)",
        "language": "python",
        "code": """\
import random
from collections import deque
random.seed(42)
R, C = 120, 120
grid = [[1 if random.random() < 0.3 else 0 for _ in range(C)] for _ in range(R)]
visited = [[False]*C for _ in range(R)]
islands = 0
for r in range(R):
    for c in range(C):
        if grid[r][c] == 1 and not visited[r][c]:
            islands += 1
            q = deque([(r, c)])
            visited[r][c] = True
            while q:
                cr, cc = q.popleft()
                for dr, dc in ((-1,0),(1,0),(0,-1),(0,1)):
                    nr, nc = cr + dr, cc + dc
                    if 0 <= nr < R and 0 <= nc < C and grid[nr][nc] == 1 and not visited[nr][nc]:
                        visited[nr][nc] = True
                        q.append((nr, nc))
print(f"PASS: Found {islands} islands in {R}x{C} grid")
""",
    },
    "med_lis": {
        "difficulty": "Medium",
        "category": "Binary Search",
        "description": "LC#300 LIS (N=40,000, O(NlogN))",
        "language": "python",
        "code": """\
import random, bisect
random.seed(1337)
arr = [random.randint(1, 1000000) for _ in range(40000)]
tails = []
for x in arr:
    idx = bisect.bisect_left(tails, x)
    if idx == len(tails):
        tails.append(x)
    else:
        tails[idx] = x
print(f"PASS: LIS Length = {len(tails)} across 40,000 numbers")
""",
    },
    "med_edit_distance": {
        "difficulty": "Medium",
        "category": "DP 2D",
        "description": "LC#72 Edit Distance (Matrix DP, 300x300)",
        "language": "python",
        "code": """\
import random, string
random.seed(42)
w1 = "".join(random.choices(string.ascii_lowercase, k=300))
w2 = "".join(random.choices(string.ascii_lowercase, k=300))
m, n = len(w1), len(w2)
dp = [[0]*(n + 1) for _ in range(m + 1)]
for i in range(m + 1): dp[i][0] = i
for j in range(n + 1): dp[0][j] = j
for i in range(1, m + 1):
    for j in range(1, n + 1):
        if w1[i - 1] == w2[j - 1]:
            dp[i][j] = dp[i - 1][j - 1]
        else:
            dp[i][j] = 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
print(f"PASS: Edit Distance = {dp[m][n]} for len {m}x{n}")
""",
    },
    "med_lru_cache": {
        "difficulty": "Medium",
        "category": "Data Structure",
        "description": "LC#146 LRU Cache Simulation (25,000 ops)",
        "language": "python",
        "code": """\
from collections import OrderedDict
class LRUCache:
    def __init__(self, cap: int):
        self.cache = OrderedDict()
        self.cap = cap
    def get(self, k: int) -> int:
        if k not in self.cache: return -1
        self.cache.move_to_end(k)
        return self.cache[k]
    def put(self, k: int, v: int) -> None:
        if k in self.cache: self.cache.move_to_end(k)
        self.cache[k] = v
        if len(self.cache) > self.cap: self.cache.popitem(last=False)

lru = LRUCache(500)
for i in range(25000):
    lru.put(i % 1000, i)
    _ = lru.get((i * 3) % 1000)
print(f"PASS: LRU executed 25,000 operations successfully")
""",
    },
    "med_trapping_water": {
        "difficulty": "Medium",
        "category": "Two Pointers",
        "description": "LC#42 Trapping Rain Water (40,000 bars)",
        "language": "python",
        "code": """\
import random
random.seed(888)
h = [random.randint(0, 1000) for _ in range(40000)]
l, r = 0, len(h) - 1
l_max, r_max = 0, 0
water = 0
while l < r:
    if h[l] < h[r]:
        if h[l] >= l_max: l_max = h[l]
        else: water += l_max - h[l]
        l += 1
    else:
        if h[r] >= r_max: r_max = h[r]
        else: water += r_max - h[r]
        r -= 1
print(f"PASS: Trapped {water} units of water across 40,000 bars")
""",
    },
    "med_group_anagrams": {
        "difficulty": "Medium",
        "category": "Hash Categorization",
        "description": "LC#49 Group Anagrams (20,000 strings)",
        "language": "python",
        "code": """\
import random, string
from collections import defaultdict
random.seed(123)
vocab = ["".join(random.choices(string.ascii_lowercase, k=6)) for _ in range(500)]
words = [random.choice(vocab) for _ in range(20000)]
groups = defaultdict(list)
for w in words:
    groups[tuple(sorted(w))].append(w)
print(f"PASS: Grouped {len(words)} words into {len(groups)} anagram classes")
""",
    },
    "med_sieve_primes": {
        "difficulty": "Medium",
        "category": "Number Theory",
        "description": "Sieve of Eratosthenes (Up to 150,000)",
        "language": "python",
        "code": """\
limit = 150000
is_prime = [True] * (limit + 1)
is_prime[0] = is_prime[1] = False
for p in range(2, int(limit**0.5) + 1):
    if is_prime[p]:
        for i in range(p * p, limit + 1, p):
            is_prime[i] = False
primes_count = sum(is_prime)
print(f"PASS: Sieve found {primes_count} primes up to {limit}")
""",
    },

    # --------------------------------------------------------------------------
    # 3. HEAVY WORKLOADS (300 - 800 ms)
    # --------------------------------------------------------------------------
    "heavy_3sum": {
        "difficulty": "Heavy",
        "category": "Two Pointers O(N^2)",
        "description": "LC#15 3Sum (N=2,200, O(N^2))",
        "language": "python",
        "code": """\
import random
random.seed(42)
nums = [random.randint(-1500, 1500) for _ in range(2200)]
nums.sort()
res = []
for i in range(len(nums) - 2):
    if i > 0 and nums[i] == nums[i - 1]:
        continue
    l, r = i + 1, len(nums) - 1
    while l < r:
        s = nums[i] + nums[l] + nums[r]
        if s == 0:
            res.append((nums[i], nums[l], nums[r]))
            while l < r and nums[l] == nums[l + 1]: l += 1
            while l < r and nums[r] == nums[r - 1]: r -= 1
            l += 1; r -= 1
        elif s < 0:
            l += 1
        else:
            r -= 1
print(f"PASS: Found {len(res)} triplets in 2200 elements")
""",
    },
    "heavy_dijkstra": {
        "difficulty": "Heavy",
        "category": "Dense Graph",
        "description": "LC#743 Dijkstra (4k Nodes, 16k Edges)",
        "language": "python",
        "code": """\
import random, heapq
random.seed(555)
n = 4000
adj = {i: [] for i in range(n)}
for _ in range(16000):
    u = random.randint(0, n - 1)
    v = random.randint(0, n - 1)
    w = random.randint(1, 100)
    adj[u].append((v, w))
dist = {i: float('inf') for i in range(n)}
dist[0] = 0
pq = [(0, 0)]
visited = set()
while pq:
    d, u = heapq.heappop(pq)
    if u in visited: continue
    visited.add(u)
    for v, w in adj[u]:
        if dist[u] + w < dist[v]:
            dist[v] = dist[u] + w
            heapq.heappush(pq, (dist[v], v))
reachable = sum(1 for d in dist.values() if d < float('inf'))
print(f"PASS: Dijkstra completed. {reachable}/{n} nodes reachable")
""",
    },
    "heavy_matrix_mul": {
        "difficulty": "Heavy",
        "category": "Linear Algebra",
        "description": "Dense Matrix Multiplication (250x250, O(N^3))",
        "language": "python",
        "code": """\
import random
random.seed(333)
N = 250
A = [[random.randint(1, 10) for _ in range(N)] for _ in range(N)]
B = [[random.randint(1, 10) for _ in range(N)] for _ in range(N)]
C = [[0] * N for _ in range(N)]
for i in range(N):
    for k in range(N):
        r = A[i][k]
        for j in range(N):
            C[i][j] += r * B[k][j]
print(f"PASS: Matrix {N}x{N} multiplied. Trace = {sum(C[i][i] for i in range(N))}")
""",
    },
}

EASY_KEYS = [k for k, v in WORKLOADS.items() if v["difficulty"] == "Easy"]
MED_KEYS = [k for k, v in WORKLOADS.items() if v["difficulty"] == "Medium"]
HEAVY_KEYS = [k for k, v in WORKLOADS.items() if v["difficulty"] == "Heavy"]


# ==============================================================================
# DATA STRUCTURES
# ==============================================================================

@dataclass
class JobRecord:
    job_id: str
    client_id: str
    tier: str
    task_name: str
    difficulty: str
    status: str
    exit_code: Optional[int]
    submit_ts_ms: int
    completed_ts_ms: int
    turnaround_ms: float      # Client perceived latency
    exec_duration_ms: float   # Sandbox execution duration (T_s)
    queue_wait_ms: float      # Time waiting in queue (W_q)
    overhead_ms: float        # Network + polling overhead
    poll_count: int           # Number of POLL requests sent for this job
    error_message: Optional[str]


# ==============================================================================
# BROKER PROTOCOL CLIENT
# ==============================================================================

class BrokerClient:
    """Manages a persistent TCP connection to the broker with automatic reconnection."""

    def __init__(self, host: str, port: int, timeout: float = 15.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self.reader = None
        self.submits_count = 0
        self.polls_count = 0

    def connect(self):
        self.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))
        self.reader = self.sock.makefile("r", encoding="utf-8")

    def close(self):
        if self.reader:
            try:
                self.reader.close()
            except Exception:
                pass
            self.reader = None
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def ensure_connected(self):
        if self.sock is None:
            self.connect()

    def submit_job(self, client_id: str, language: str, code: str, timeout_secs: int = 15) -> Tuple[Optional[str], Optional[str]]:
        """Submits a job and returns (job_id, error_message)."""
        self.ensure_connected()
        req = {
            "action": "SUBMIT",
            "client_id": client_id,
            "language": language,
            "code": code,
            "timeout_secs": timeout_secs,
        }
        raw_cmd = json.dumps(req) + "\n"
        try:
            self.submits_count += 1
            self.sock.sendall(raw_cmd.encode("utf-8"))
            resp_line = self.reader.readline()
            if not resp_line:
                raise ConnectionResetError("Empty response on submit")
            resp_data = json.loads(resp_line)
            if "job_id" in resp_data:
                return resp_data["job_id"], None
            elif resp_data.get("status") == "error":
                return None, resp_data.get("message", "Unknown error")
            return None, f"Unexpected response: {resp_line.strip()}"
        except Exception as e:
            self.close()
            return None, str(e)

    def poll_result(self, job_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """Polls for job result and returns (result_dict, error_message)."""
        self.ensure_connected()
        req = {"action": "POLL", "job_id": job_id}
        raw_cmd = json.dumps(req) + "\n"
        try:
            self.polls_count += 1
            self.sock.sendall(raw_cmd.encode("utf-8"))
            resp_line = self.reader.readline()
            if not resp_line:
                raise ConnectionResetError("Empty response on poll")
            resp_data = json.loads(resp_line)
            return resp_data, None
        except Exception as e:
            self.close()
            return None, str(e)


# ==============================================================================
# WORKER CLIENT RUNNERS (HIGH-FREQUENCY ENGINE)
# ==============================================================================

def client_worker_loop(
    client_id: str,
    tier: str,
    host: str,
    port: int,
    end_time: float,
    poll_interval: float,
    mode: str,
    think_scale: float,
    records: List[JobRecord],
    lock: threading.Lock,
    active_counter: Dict[str, int],
    global_stats: Dict[str, int],
):
    """Executes submissions continuously with high frequency until end_time."""
    client = BrokerClient(host, port)

    try:
        while time.time() < end_time:
            # 1. Select workload based on tier
            if tier == "PoliteDev":
                r = random.random()
                if r < 0.20:
                    task_key = random.choice(EASY_KEYS)
                elif r < 0.85:
                    task_key = random.choice(MED_KEYS)
                else:
                    task_key = random.choice(HEAVY_KEYS)

                if mode == "saturation":
                    think_time = random.uniform(0.01, 0.04) * think_scale
                elif mode == "flood":
                    think_time = 0.0
                else:
                    think_time = random.uniform(1.0, 2.5) * think_scale

            elif tier == "RapidCI":
                task_key = random.choice(EASY_KEYS if random.random() < 0.70 else MED_KEYS)
                if mode in ("saturation", "flood"):
                    think_time = 0.0  # Immediate back-to-back firehose
                else:
                    think_time = random.uniform(0.15, 0.45) * think_scale

            elif tier == "HeavyCompute":
                task_key = random.choice(MED_KEYS if random.random() < 0.20 else HEAVY_KEYS)
                if mode == "saturation":
                    think_time = random.uniform(0.01, 0.05) * think_scale
                elif mode == "flood":
                    think_time = 0.0
                else:
                    think_time = random.uniform(2.0, 4.0) * think_scale

            elif tier == "BurstContestant":
                task_key = random.choice(MED_KEYS)
                if mode in ("saturation", "flood"):
                    think_time = random.uniform(0.0, 0.02) * think_scale
                else:
                    think_time = random.uniform(0.05, 0.2) * think_scale

            elif tier == "AdversarialSpammer":
                task_key = random.choice(EASY_KEYS)
                think_time = 0.0

            else:
                task_key = random.choice(MED_KEYS)
                think_time = 0.01

            workload = WORKLOADS[task_key]

            # 2. Submit Job
            with lock:
                active_counter["in_flight"] = active_counter.get("in_flight", 0) + 1
                global_stats["total_submits"] = global_stats.get("total_submits", 0) + 1

            t_submit_start = time.time()
            job_id, err = client.submit_job(client_id, workload["language"], workload["code"], timeout_secs=15)

            if not job_id:
                with lock:
                    active_counter["in_flight"] = max(0, active_counter.get("in_flight", 1) - 1)
                    records.append(
                        JobRecord(
                            job_id="n/a",
                            client_id=client_id,
                            tier=tier,
                            task_name=task_key,
                            difficulty=workload["difficulty"],
                            status="Rejected",
                            exit_code=None,
                            submit_ts_ms=int(t_submit_start * 1000),
                            completed_ts_ms=int(time.time() * 1000),
                            turnaround_ms=(time.time() - t_submit_start) * 1000,
                            exec_duration_ms=0.0,
                            queue_wait_ms=0.0,
                            overhead_ms=0.0,
                            poll_count=0,
                            error_message=err,
                        )
                    )
                if think_time > 0:
                    time.sleep(think_time)
                continue

            # 3. Poll for Completion
            done = False
            poll_start = time.time()
            max_poll_timeout = 25.0
            job_polls = 0

            while not done and (time.time() - poll_start) < max_poll_timeout:
                res_data, poll_err = client.poll_result(job_id)
                job_polls += 1
                with lock:
                    global_stats["total_polls"] = global_stats.get("total_polls", 0) + 1

                if poll_err:
                    time.sleep(poll_interval)
                    continue

                status = res_data.get("status")
                if status in ("Done", "Failed", "TimedOut"):
                    t_done = time.time()
                    turnaround_ms = (t_done - t_submit_start) * 1000.0

                    exec_ms = float(res_data.get("execution_duration_ms") or 0.0)
                    created_at_ms = res_data.get("created_at_ms")
                    completed_at_ms = res_data.get("completed_at_ms")

                    if created_at_ms and completed_at_ms:
                        total_broker_residence = float(completed_at_ms - created_at_ms)
                        queue_wait_ms = max(0.0, total_broker_residence - exec_ms)
                    else:
                        queue_wait_ms = 0.0

                    overhead_ms = max(0.0, turnaround_ms - (exec_ms + queue_wait_ms))

                    record = JobRecord(
                        job_id=job_id,
                        client_id=client_id,
                        tier=tier,
                        task_name=task_key,
                        difficulty=workload["difficulty"],
                        status=status,
                        exit_code=res_data.get("exit_code"),
                        submit_ts_ms=created_at_ms or int(t_submit_start * 1000),
                        completed_ts_ms=completed_at_ms or int(t_done * 1000),
                        turnaround_ms=round(turnaround_ms, 2),
                        exec_duration_ms=round(exec_ms, 2),
                        queue_wait_ms=round(queue_wait_ms, 2),
                        overhead_ms=round(overhead_ms, 2),
                        poll_count=job_polls,
                        error_message=res_data.get("error_message"),
                    )

                    with lock:
                        records.append(record)
                        active_counter["in_flight"] = max(0, active_counter.get("in_flight", 1) - 1)
                    done = True
                    break

                time.sleep(poll_interval)

            if not done:
                with lock:
                    active_counter["in_flight"] = max(0, active_counter.get("in_flight", 1) - 1)
                    records.append(
                        JobRecord(
                            job_id=job_id,
                            client_id=client_id,
                            tier=tier,
                            task_name=task_key,
                            difficulty=workload["difficulty"],
                            status="ClientPollTimeout",
                            exit_code=None,
                            submit_ts_ms=int(t_submit_start * 1000),
                            completed_ts_ms=int(time.time() * 1000),
                            turnaround_ms=(time.time() - t_submit_start) * 1000,
                            exec_duration_ms=0.0,
                            queue_wait_ms=0.0,
                            overhead_ms=0.0,
                            poll_count=job_polls,
                            error_message="Client poll timeout after 25s",
                        )
                    )

            # 4. High-frequency inter-job think time
            if mode == "realistic" and tier == "BurstContestant" and random.random() < 0.25:
                time.sleep(random.uniform(8.0, 14.0) * think_scale)
            elif think_time > 0.0:
                time.sleep(think_time)

    finally:
        client.close()


# ==============================================================================
# STATISTICAL ENGINE
# ==============================================================================

def calculate_percentiles(values: List[float]) -> Dict[str, float]:
    """Calculates min, mean, stddev, P50, P75, P90, P95, P99, P99.9, and max."""
    if not values:
        return {
            "count": 0, "min": 0.0, "mean": 0.0, "std": 0.0,
            "p50": 0.0, "p75": 0.0, "p90": 0.0, "p95": 0.0,
            "p99": 0.0, "p99_9": 0.0, "max": 0.0,
        }
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def p(pct: float) -> float:
        if n == 1:
            return sorted_vals[0]
        k = (n - 1) * (pct / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        d0 = sorted_vals[int(f)] * (c - k)
        d1 = sorted_vals[int(c)] * (k - f)
        return d0 + d1

    mean_val = sum(sorted_vals) / n
    std_val = math.sqrt(sum((x - mean_val) ** 2 for x in sorted_vals) / n) if n > 1 else 0.0

    return {
        "count": n,
        "min": round(sorted_vals[0], 2),
        "mean": round(mean_val, 2),
        "std": round(std_val, 2),
        "p50": round(p(50.0), 2),
        "p75": round(p(75.0), 2),
        "p90": round(p(90.0), 2),
        "p95": round(p(95.0), 2),
        "p99": round(p(99.0), 2),
        "p99_9": round(p(99.9), 2),
        "max": round(sorted_vals[-1], 2),
    }


def format_table_row(label: str, s: Dict[str, float]) -> str:
    return (
        f"  {label:<22} | "
        f"{s['count']:>5} | "
        f"{s['min']:>7.1f} | "
        f"{s['p50']:>7.1f} | "
        f"{s['p90']:>7.1f} | "
        f"{s['p95']:>7.1f} | "
        f"{s['p99']:>7.1f} | "
        f"{s['p99_9']:>7.1f} | "
        f"{s['max']:>7.1f} | "
        f"{s['mean']:>7.1f}"
    )


# ==============================================================================
# MAIN BENCHMARK ORCHESTRATOR
# ==============================================================================

def run_benchmark(
    host: str,
    port: int,
    duration_secs: int,
    poll_interval: float,
    heartbeat_interval: int,
    mode: str,
    think_scale: float,
    broker_workers: int,
    polite_count: int,
    rapid_count: int,
    burst_count: int,
    heavy_count: int,
    spammer_count: int,
    csv_file: Optional[str],
):
    total_tenants = polite_count + rapid_count + burst_count + heavy_count + spammer_count

    print("=" * 95)
    print("      HIGH-FREQUENCY REAL-WORLD TRAFFIC BENCHMARK (TARGET: ~100% WORKER UTILIZATION)     ")
    print("=" * 95)
    print(f"  Target Broker:           {host}:{port}")
    print(f"  Broker Worker Capacity:  {broker_workers} Workers")
    print(f"  Execution Mode:          {mode.upper()} (Think Time Multiplier: {think_scale}x)")
    print(f"  Target Duration:         {duration_secs} seconds ({duration_secs / 60:.1f} minutes)")
    print(f"  Poll Frequency:          Every {poll_interval * 1000:.0f} ms")
    print(f"  Heartbeat Interval:      Every {heartbeat_interval} seconds")
    print("  Client Population Breakdown:")
    print(f"    - Polite Developers:   {polite_count:>3}  (Fast algorithmic submissions)")
    print(f"    - Rapid CI Services:   {rapid_count:>3}  (Back-to-back zero-delay validation)")
    print(f"    - Burst Contestants:   {burst_count:>3}  (Continuous batch wave submissions)")
    print(f"    - Heavy Compute Jobs:  {heavy_count:>3}  (Dijkstra, 3Sum, Matrix Multiplication)")
    if spammer_count > 0:
        print(f"    - Adversarial Spammers:{spammer_count:>3}  (Pure zero-delay flood)")
    print(f"  Total Simulated Tenants: {total_tenants} Concurrent Client Sessions")
    print(f"  Workload Variations:     {len(WORKLOADS)} distinct algorithms (Easy={len(EASY_KEYS)}, Med={len(MED_KEYS)}, Heavy={len(HEAVY_KEYS)})")
    print("=" * 95)

    # 1. Connectivity check
    print("[INIT] Verifying broker connectivity...")
    test_client = BrokerClient(host, port, timeout=3.0)
    try:
        test_client.connect()
        test_client.close()
        print("[INIT] Broker is ONLINE and accepting connections.\n")
    except Exception as e:
        print(f"\n[ERROR] Could not connect to broker at {host}:{port}: {e}")
        print("Please start the broker first: cargo run -- --local 127.0.0.1:7000")
        sys.exit(1)

    records: List[JobRecord] = []
    lock = threading.Lock()
    active_counter: Dict[str, int] = {"in_flight": 0}
    global_stats: Dict[str, int] = {"total_submits": 0, "total_polls": 0}
    utilization_samples: List[float] = []

    # 2. Build client list
    clients_def = []
    for i in range(1, polite_count + 1):
        clients_def.append((f"dev_polite_{i}", "PoliteDev"))
    for i in range(1, rapid_count + 1):
        clients_def.append((f"ci_rapid_{i}", "RapidCI"))
    for i in range(1, burst_count + 1):
        clients_def.append((f"burst_user_{i}", "BurstContestant"))
    for i in range(1, heavy_count + 1):
        clients_def.append((f"heavy_user_{i}", "HeavyCompute"))
    for i in range(1, spammer_count + 1):
        clients_def.append((f"spammer_{i}", "AdversarialSpammer"))

    t_start = time.time()
    t_end = t_start + duration_secs

    threads: List[threading.Thread] = []
    for cid, tier in clients_def:
        t = threading.Thread(
            target=client_worker_loop,
            args=(
                cid, tier, host, port, t_end, poll_interval,
                mode, think_scale, records, lock, active_counter, global_stats
            ),
            daemon=True,
            name=cid,
        )
        threads.append(t)

    print(f"[START] Launching {len(threads)} client worker threads across all tiers...")
    for t in threads:
        t.start()

    # 3. Live Telemetry Loop (Now displaying LIVE WORKER UTILIZATION %)
    last_completed = 0
    last_net_ops = 0
    last_hb_time = t_start

    print("-" * 105)
    print(f" {'TIME':<13} | {'PROGRESS':<8} | {'COMPLETED':<10} | {'IN-FLIGHT':<9} | {'WORKER UTIL':<14} | {'JOBS/SEC':<10} | {'NET OPS/SEC':<11} | {'ROLLING P50':<11}")
    print("-" * 105)

    try:
        while time.time() < t_end:
            time.sleep(heartbeat_interval)
            now = time.time()
            elapsed = now - t_start
            pct = min(100.0, (elapsed / duration_secs) * 100.0)

            with lock:
                cur_records = list(records)
                in_flight = active_counter.get("in_flight", 0)
                cur_submits = global_stats.get("total_submits", 0)
                cur_polls = global_stats.get("total_polls", 0)

            cur_net_ops = cur_submits + cur_polls
            completed = len(cur_records)

            delta_time = max(0.001, now - last_hb_time)
            job_rate = (completed - last_completed) / delta_time
            net_rate = (cur_net_ops - last_net_ops) / delta_time

            last_completed = completed
            last_net_ops = cur_net_ops
            last_hb_time = now

            # Calculate Live Worker Utilization %
            util_pct = min(100.0, (in_flight / max(1, broker_workers)) * 100.0)
            utilization_samples.append(util_pct)
            util_str = f"{util_pct:>5.1f}% ({min(in_flight, broker_workers)}/{broker_workers})"

            recent_turnarounds = [r.turnaround_ms for r in cur_records[-100:] if r.status == "Done"]
            p50_str = f"{calculate_percentiles(recent_turnarounds)['p50']:.0f} ms" if recent_turnarounds else "n/a"

            elapsed_str = f"{int(elapsed // 60):02d}:{int(elapsed % 60):02d}"
            total_str = f"{int(duration_secs // 60):02d}:{int(duration_secs % 60):02d}"

            print(f" [{elapsed_str} / {total_str}] | {pct:>6.1f}% | {completed:>10} | {in_flight:>9} | {util_str:<14} | {job_rate:>10.2f} | {net_rate:>11.2f} | {p50_str:>11}")

    except KeyboardInterrupt:
        print("\n[WARN] Benchmark interrupted early by user. Tallying collected results...")

    for t in threads:
        t.join(timeout=2.0)

    actual_duration = time.time() - t_start

    # ==============================================================================
    # FINAL METRICS & REPORT
    # ==============================================================================
    print("\n" + "=" * 95)
    print("                    BENCHMARK RESULTS & METRIC BREAKDOWN                   ")
    print("=" * 95)

    done_records = [r for r in records if r.status == "Done"]
    failed_records = [r for r in records if r.status == "Failed"]
    timeout_records = [r for r in records if r.status in ("TimedOut", "ClientPollTimeout")]
    rejected_records = [r for r in records if r.status == "Rejected"]

    total_submissions = len(records)
    total_submits = global_stats.get("total_submits", 0)
    total_polls = global_stats.get("total_polls", 0)
    total_net_commands = total_submits + total_polls

    effective_job_tps = len(done_records) / actual_duration if actual_duration > 0 else 0
    raw_net_tps = total_net_commands / actual_duration if actual_duration > 0 else 0
    avg_polls_per_job = total_polls / max(1, len(done_records))

    avg_util = sum(utilization_samples) / max(1, len(utilization_samples)) if utilization_samples else 0.0

    print(f"  Benchmark Wall-Clock Time: {actual_duration:.1f} seconds ({actual_duration / 60:.2f} mins)")
    print(f"  Total Submissions:         {total_submissions}")
    print(f"  Successful Judgments:      {len(done_records)} ({len(done_records) / max(1, total_submissions) * 100:.1f}%)")
    print(f"  Server/Client Failures:    {len(failed_records)}")
    print(f"  Timeouts:                  {len(timeout_records)}")
    print(f"  Broker Rejections:         {len(rejected_records)}")
    print("-" * 95)
    print(f"  CAPACITY & UTILIZATION METRICS:")
    print(f"    - Average Worker Utilization:{avg_util:>6.1f}%  (Target: ~100% saturation across {broker_workers} workers)")
    print(f"    - Completed Jobs / sec:      {effective_job_tps:>6.2f} jobs/second  (Pure code executions completed)")
    print(f"    - Total Polling Requests:    {total_polls:>6,} polls")
    print(f"    - Raw Broker Ops / sec:      {raw_net_tps:>6.2f} req/second  (Includes ALL SUBMIT + POLL commands)")
    print(f"    - Avg Polls per Job:         {avg_polls_per_job:>6.1f} polls/job")
    print("-" * 95)

    # 1. CORE TIME DECOMPOSITION TABLE
    turnaround_stats = calculate_percentiles([r.turnaround_ms for r in done_records])
    queue_wait_stats = calculate_percentiles([r.queue_wait_ms for r in done_records])
    exec_stats = calculate_percentiles([r.exec_duration_ms for r in done_records])
    overhead_stats = calculate_percentiles([r.overhead_ms for r in done_records])

    header = f"  {'METRIC (ms)':<22} | {'COUNT':>5} | {'MIN':>7} | {'P50':>7} | {'P90':>7} | {'P95':>7} | {'P99':>7} | {'P99.9':>7} | {'MAX':>7} | {'MEAN':>7}"
    sep = "  " + "-" * (len(header) - 2)

    print("\n[PHASE LATENCY DECOMPOSITION - ALL DONE JOBS]")
    print(header)
    print(sep)
    print(format_table_row("Turnaround (Client)", turnaround_stats))
    print(format_table_row("Queue Wait (W_q)", queue_wait_stats))
    print(format_table_row("Execution (T_s)", exec_stats))
    print(format_table_row("Network & Polling", overhead_stats))
    print(sep)

    # 2. BREAKDOWN BY WORKLOAD DIFFICULTY
    print("\n[LATENCY BREAKDOWN BY WORKLOAD DIFFICULTY]")
    print(header)
    print(sep)
    for diff in ("Easy", "Medium", "Heavy"):
        diff_done = [r for r in done_records if r.difficulty == diff]
        stats = calculate_percentiles([r.turnaround_ms for r in diff_done])
        print(format_table_row(f"{diff} Turnaround", stats))
    print(sep)

    # 3. BREAKDOWN BY CLIENT TIER / PERSONA
    print("\n[LATENCY BREAKDOWN BY CLIENT TIER]")
    print(header)
    print(sep)
    active_tiers = set(r.tier for r in done_records)
    for t in sorted(active_tiers):
        tier_done = [r for r in done_records if r.tier == t]
        stats = calculate_percentiles([r.turnaround_ms for r in tier_done])
        print(format_table_row(f"{t}", stats))
    print(sep)

    # 4. OPTIONAL CSV EXPORT
    if not csv_file:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        csv_file = f"benchmark_trace_{timestamp}.csv"

    try:
        with open(csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "job_id",
                "client_id",
                "tier",
                "task_name",
                "difficulty",
                "status",
                "exit_code",
                "submit_ts_ms",
                "completed_ts_ms",
                "turnaround_ms",
                "exec_duration_ms",
                "queue_wait_ms",
                "overhead_ms",
                "poll_count",
                "error_message",
            ])
            for r in records:
                writer.writerow([
                    r.job_id,
                    r.client_id,
                    r.tier,
                    r.task_name,
                    r.difficulty,
                    r.status,
                    r.exit_code,
                    r.submit_ts_ms,
                    r.completed_ts_ms,
                    r.turnaround_ms,
                    r.exec_duration_ms,
                    r.queue_wait_ms,
                    r.overhead_ms,
                    r.poll_count,
                    r.error_message or "",
                ])
        print(f"\n[TRACE] Raw latency trace for all {len(records)} submissions exported to: {csv_file}")
    except Exception as e:
        print(f"[WARN] Failed to write trace CSV: {e}")

    print("=" * 95 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run high-frequency real-world traffic benchmark against Sandboxed Job Broker."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Broker server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7000, help="Broker server port (default: 7000)")
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="Benchmark duration in seconds (default: 300 = 5 minutes)",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=0.05,
        help="Polling interval in seconds (default: 0.05 = 50ms)",
    )
    parser.add_argument(
        "--heartbeat",
        type=int,
        default=15,
        help="Telemetry heartbeat update interval in seconds (default: 15s)",
    )
    parser.add_argument(
        "--broker-workers",
        type=int,
        default=16,
        help="Number of worker threads on the broker (default: 16, used for live utilization %%)",
    )
    parser.add_argument(
        "--mode",
        choices=["saturation", "flood", "realistic"],
        default="saturation",
        help="Traffic frequency preset: 'saturation' (near 100%% util), 'flood' (zero-delay), 'realistic' (human pauses)",
    )
    parser.add_argument(
        "--think-scale",
        type=float,
        default=1.0,
        help="Multiplier for think times (0.0 = zero delay, 0.1 = 10x faster, default: 1.0)",
    )
    parser.add_argument(
        "--output-csv",
        type=str,
        default=None,
        help="Custom path for exporting raw trace CSV",
    )

    # Client customization options
    parser.add_argument(
        "--clients",
        type=int,
        default=None,
        help="Total number of clients (default in saturation mode: 24 to pin 16 workers)",
    )
    parser.add_argument("--polite", type=int, default=None, help="Explicit count of Polite Developer clients")
    parser.add_argument("--rapid", type=int, default=None, help="Explicit count of Rapid CI clients")
    parser.add_argument("--burst", type=int, default=None, help="Explicit count of Burst Contestant clients")
    parser.add_argument("--heavy", type=int, default=None, help="Explicit count of Heavy Compute clients")
    parser.add_argument(
        "--spammers",
        type=int,
        default=0,
        help="Explicit count of Adversarial Spammer clients (0ms think time, tests rate limiting)",
    )

    args = parser.parse_args()

    # Determine tenant populations dynamically based on broker worker count
    if args.clients is not None:
        total = max(1, args.clients)
        polite = max(1, int(round(total * 0.40)))
        rapid = max(1, int(round(total * 0.35)))
        burst = max(1, int(round(total * 0.15)))
        heavy = max(1, total - (polite + rapid + burst))
        spammers = args.spammers
    else:
        # Dynamically scale client population to match the broker's worker capacity:
        # In saturation/flood mode: client count = (workers + ~25% buffer)
        # to pin 100% worker saturation without causing massive multi-second queue backlogs.
        if args.mode in ("saturation", "flood"):
            total = max(2, args.broker_workers + max(1, args.broker_workers // 4))
        else:
            # In realistic mode (think time ~1.5s): need ~3-4x workers to saturate
            total = max(2, args.broker_workers * 3)

        if args.polite is None and args.rapid is None:
            polite = max(1, int(round(total * 0.40)))
            rapid = max(1, int(round(total * 0.35)))
            burst = max(1, int(round(total * 0.15)))
            heavy = max(1, total - (polite + rapid + burst))
            spammers = args.spammers
        else:
            polite = args.polite if args.polite is not None else 1
            rapid = args.rapid if args.rapid is not None else 1
            burst = args.burst if args.burst is not None else 1
            heavy = args.heavy if args.heavy is not None else 1
            spammers = args.spammers

    run_benchmark(
        host=args.host,
        port=args.port,
        duration_secs=args.duration,
        poll_interval=args.poll_interval,
        heartbeat_interval=args.heartbeat,
        mode=args.mode,
        think_scale=args.think_scale,
        broker_workers=args.broker_workers,
        polite_count=polite,
        rapid_count=rapid,
        burst_count=burst,
        heavy_count=heavy,
        spammer_count=spammers,
        csv_file=args.output_csv,
    )


if __name__ == "__main__":
    main()
