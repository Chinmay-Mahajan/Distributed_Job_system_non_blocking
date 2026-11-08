# Sandboxed Job Execution Broker (v2)

A backend service built in Rust for safely running untrusted user code.

Clients connect over TCP to submit scripts (such as Python), which the broker executes inside ephemeral, resource-capped Docker containers. The broker decouples fast network ingestion from slow container execution, uses an $O(1)$ fair queue so high-volume users cannot starve others, and automatically sizes its worker and container pools based on detected hardware.

### Key Capabilities at a Glance
* **Sandboxed Isolation:** Each job runs in an isolated, single-use container with no network access (`--network none`), capped memory/CPU, read-only root filesystems, and broker-enforced wall-clock timeouts.
* **Asynchronous Execution & Polling:** Clients submit code, receive a `job_id` immediately, and poll for `stdout`/`stderr` results on their own schedule—preventing slow scripts from blocking network sockets.
* **Fair Multi-Tenancy:** A two-queue $O(1)$ scheduler parks saturated tenants without scanning overhead, guaranteeing that legitimate users maintain low latency and 100% completion even under bursty neighbor traffic.
* **Sub-20ms Container Dispatch:** Pre-warmed standby containers and adaptive polling sleep reduce container cold-start and exit-detection overhead from ~500ms down to sub-20ms.
* **Hardware Auto-Sizing:** Detects host cores, RAM, and Docker VM limits at startup (`--auto`) to automatically configure worker threads, pool sizes, and lock shards.

---

## 1. High-Level Architecture

The system decouples network ingestion, two-tier authentication & poll rate-limiting, multi-tenant scheduling, container sandboxing, and result retention into independent, lock-minimized subsystems:

```mermaid
flowchart TD
    subgraph Clients["Multi-Tenant Traffic (JSON / Text over TCP)"]
        C1["Spammer Client (High-Volume)"]
        C2["Legitimate Client (Low-Volume)"]
        C3["Idle Client (nc / connection holder)"]
    end

    subgraph Ingress["Ingestion, Auth & Admission Layer"]
        TCP["TCP Listener (Port 7000)<br/>Sync Thread Pool OR Async Tokio Reactor"]
        Auth{"Two-Tier Auth & Poll Limiter<br/>SHA-256(secret_key)"}
        L1["L1 Cache (In-Process Heap)<br/>32-Shard RwLock Map<br/>Cap: 10k | TTL: 120s | Poll Quota: 25/s"]
        L2["L2 Cache (Redis DB 1)<br/>auth:&lt;hash&gt; (TTL 3600s)<br/>penalty:&lt;hash&gt; (TTL 60s)"]
        Gate{"Admission Gate<br/>Queue Limit Check<br/>(in_flight * multiplier)"}
        Reject["-ERR Unauthorized / Rate limit exceeded<br/>(Fast-fail rejection)"]
    end

    subgraph Scheduler["Two-Queue O(1) Fair Scheduler"]
        DQ["dispatchable_queue (VecDeque)<br/>in_flight &lt; cap<br/>O(1) Worker Pop"]
        WQ["waiting_queue (HashSet)<br/>in_flight &gt;= cap (Saturated)<br/>Parked with 0 scan overhead"]
        Queues["Per-Client Isolated FIFO Queues<br/>VecDeque[Job] keyed by key_hash"]
    end

    subgraph Workers["Execution Worker Pool"]
        W1["Worker Thread 1"]
        W2["Worker Thread 2"]
        WN["Worker Thread N"]
    end

    subgraph Sandboxes["Isolation Sandbox Engine"]
        SB["Docker + gVisor (runsc)<br/>• --network none<br/>• --read-only + tmpfs size cap<br/>• CPU / Mem / PID limits<br/>• Wall-clock hard timeouts"]
    end

    subgraph Storage["Result Storage & Retention"]
        RS["32-Shard Lock-Striped ResultStore<br/>• JobState (Pending/Running/Done/Failed/TimedOut)<br/>• Strict Owner Check (result.client_id == key_hash)<br/>• Background Reaper Thread"]
    end

    C1 & C2 & C3 --> TCP
    TCP --> Auth
    Auth <-->|"Sub-µs lookup"| L1
    L1 <-->|"Miss: MGET / Strike Demotion"| L2
    Auth -->|"Invalid key / Poll quota exceeded"| Reject
    Auth -->|"Verified SUBMIT"| Gate
    Auth -->|"Verified POLL / STATUS"| RS
    Gate -->|"Over quota"| Reject
    Gate -->|"Admitted"| Queues

    Queues -->|"Client has capacity"| DQ
    Queues -->|"Client saturated"| WQ

    DQ -->|"O(1) Pop"| W1 & W2 & WN
    W1 & W2 & WN --> SB
    SB --> RS

    W1 & W2 & WN -.->|"on_job_completed: Promoted in O(1)"| WQ
    WQ -.->|"Promoted to"| DQ
```

---

## 2. Core Architectural Pillars & Strengths

### 2.1 Two-Tier Authentication (L1 Heap + L2 Redis), Ownership & Poll Rate-Limiting
* **Server-Issued `secret_key` (Anti-Spoofing):** Clients do not choose their own `client_id`. New clients issue a `REGISTER` command to receive a cryptographically random `secret_key` (`sk_<uuid>`). The broker immediately hashes the key via `SHA-256` (`key_hash = SHA-256(secret_key)`), never storing raw secret keys in memory or Redis, and uses `key_hash` as the internal tenant identifier across the scheduler and result store.
* **L1 (In-Process Heap) + L2 (Redis) Cache Hierarchy ([`src/auth.rs`](src/auth.rs)):**
  * **L1 Heap Cache:** A 32-shard `RwLock<HashMap<String, L1Session>>` living in server process memory with a configurable capacity cap (`l1_max_capacity = 10,000`) and inactivity expiration (`l1_ttl = 120s`). Active users are authenticated in sub-microsecond time with zero network hops.
  * **L2 Redis Cache (`redis://127.0.0.1:6379/1`):** Stores `auth:<key_hash>` in an isolated Redis logical database (`DB 1`) with a longer retention window (`l2_redis_ttl_secs = 3600s`). When an idle user's L1 entry expires, their next request triggers a single-round-trip `MGET auth:<key_hash> penalty:<key_hash>` against Redis and automatically promotes genuine users back into L1.
* **Per-User Polling Quota & Penalty Demotion:**
  * Each L1 session tracks polling frequency within a 1-second window (`max_polls_per_sec = 25`).
  * Exceeding the polling limit rejects the excess poll and increments `violation_strikes`.
  * When a client reaches `max_poll_strikes` (default `3`), the broker **evicts the client from the L1 heap cache** and writes `penalty:<key_hash> = 1` (`EX 60s`) into L2 Redis. While on probation, L2 hits are **not** re-promoted to L1, protecting L1 heap capacity for well-behaved tenants.
* **Strict Cross-Tenant Job Ownership (AuthZ):** Every `POLL` and `STATUS` query verifies `result.client_id == key_hash`, preventing any tenant from inspecting or polling another user's job outputs.
* **Per-Connection `REGISTER` Guard (`peer_auth_map`):** Each accepted TCP connection (`default_client_id = client-<ip>:<port>`) is tracked in `peer_auth_map: RwLock<HashMap<String, bool>>` (initialized to `false` on connect, flipped to `true` on the first `REGISTER`). Calling `REGISTER` / `AUTH` more than once on the same connection is immediately rejected (`Already registered`), preventing a client from minting multiple `secret_key` tokens on one socket to bypass per-client in-flight or polling quotas while still allowing separate connections from the same host IP (distinct ephemeral ports) to register independently.
* **Proactive Idle & Key-Expiry Connection Eviction:** Both the synchronous (`TcpStream::set_read_timeout`) and asynchronous (`tokio::time::timeout`) socket loops enforce a dual deadline of `min(client_idle_timeout, remaining_key_ttl)`. If a client sits idle past `client_idle_timeout` (default `300s`) or its issued `secret_key` reaches `l2_redis_ttl_secs` (`3600s`), the server proactively sends an `ERR` disconnect notice, purges any unpopped job results belonging to that client's `key_hash` from `ResultStore`, closes the TCP socket, and guarantees cleanup of `peer_auth_map` via an RAII drop guard (`ConnectionCleanupGuard` / `AsyncConnectionCleanupGuard`).
* **Bounded TCP Ingress Framing (`MAX_LINE_BYTES = 64 KB`) & Buffer Reuse:** Socket line reads are strictly bounded via `.take((MAX_LINE_BYTES + 1) as u64)` (`std::io::Read` in Sync, `tokio::io::AsyncReadExt` in Async). Any request frame exceeding `64 KB` without a newline is immediately rejected and disconnected before JSON parsing or heap amplification can occur. Each connection pre-allocates a `1 KB` buffer (`String::with_capacity(1024)`) for zero-reallocation handling of normal commands and automatically resets capacity back to `1 KB` after any large payload spike.

### 2.2 Two-Queue $O(1)$ Fair Scheduler
* **Instantaneous $O(1)$ Dispatch:** Workers never scan through an $O(N)$ list of clients skipping saturated tenants under a shared mutex. Workers pop eligible tenants directly from [`dispatchable_queue`](src/scheduler.rs).
* **Zero-Overhead Tenant Saturation:** When a client reaches their concurrent in-flight limit (`max_in_flight_per_client`), they are parked in [`waiting_queue`](src/scheduler.rs). Saturated clients consume **zero worker CPU cycles**.
* **Automatic $O(1)$ Capacity Promotion:** The moment a worker finishes a job, [`on_job_completed()`](src/scheduler.rs) removes the client from [`waiting_queue`](src/scheduler.rs) and promotes them back into [`dispatchable_queue`](src/scheduler.rs) in $O(1)$ time.

### 2.3 Dynamic Admission Control & Bufferbloat Prevention
* **Configurable Backpressure:** Saturated clients can queue pending requests up to a configurable multiplier of their concurrency cap:
  `max_allowed_queue_depth = max_in_flight_per_client * queue_limit_multiplier`
* **Fast-Fail Memory Protection:** Excess requests beyond this quota are rejected immediately with a descriptive rate limit error before touching the [`ResultStore`](src/store.rs), preventing memory exhaustion and bufferbloat.
* **Empirically Proven Fairness:** Under heavy asymmetric traffic (128 concurrent requests), legitimate tenants experienced **100% completion rates** with an average queue latency of just **1.3s – 1.7s**, while abusive high-frequency spammers were safely bounded.

### 2.4 Dual Connection Models: Synchronous vs. Asynchronous Tokio Reactor
The broker supports two interchangeable connection models toggleable at startup via CLI flags or environment variables:
* **Asynchronous Tokio Reactor (`--async` / `DISTJOB_ASYNC=1`):** Uses non-blocking TCP socket multiplexing powered by Tokio (`epoll` on Linux / `kqueue` on macOS) in [`src/server_async.rs`](src/server_async.rs) paired with a multiplexed async Redis connection (`redis::aio::MultiplexedConnection`). Incoming client connections are handled as lightweight cooperative green tasks (~300 bytes each) on a fixed thread pool matching physical CPU cores. Idle clients and polling waits yield execution back to the kernel reactor without consuming dedicated OS threads, fully resolving the C10K connection exhaustion bottleneck.
* **Synchronous Thread-per-Connection (`--sync`, default):** The classic, standalone implementation in [`src/server.rs`](src/server.rs) where each TCP connection is handled by a dedicated OS thread.
* **The Hybrid Reactor Pattern:** In both models, client network ingestion is decoupled from the execution engine. Submissions immediately return an opaque `job_id`, while the heavy container sandboxes execute on dedicated background worker threads ([`WorkerPool`](src/worker.rs)), ensuring high network polling rates never starve container executions and heavy container jobs never freeze the network reactor.
* Clients poll on their own schedule via `POLL` or check status via `STATUS`.

### 2.5 Multi-Layered Sandbox Isolation & Pre-Warmed Acceleration
* **Pre-Warmed "Bake and Burn" Standby Pool:** A background thread maintains idle standby containers ready for instantaneous execution. Containers are claimed in $O(1)$ and burned asynchronously in detached threads after a single use, preserving strict cross-tenant isolation with sub-20ms container dispatch.
* **Pre-Baked Scientific Runtime (`distjob-python:latest`):** Bundles `numpy`, `pandas`, `scipy`, `sympy`, and `requests` with Ahead-of-Time (AOT) `.pyc` bytecode compilation, cutting import times by 3x–5x.
* **Fault-Tolerant Dynamic Image Fallback:** If `distjob-python:latest` is not present on the host, the broker automatically discovers and falls back to `python:3.11-slim` without dropping jobs.
* **Zero Network Access:** Hard air-gapped isolation with `--network none`.
* **Resource Caps:** Enforced cgroups via `--memory 128m`, `--cpus 0.5`, `--pids-limit 64`.
* **Filesystem Immutability:** `--read-only` root filesystem paired with an in-memory `--tmpfs /tmp:rw,noexec,nosuid,size=64m`.
* **Syscall Interception:** Optional integration with Google's gVisor runtime (`--runtime runsc`).
* **Broker-Enforced Hard Timeout:** A broker supervisor thread monitors wall-clock execution against `timeout_secs`, killing runaway containers or processes and recording `TimedOut`.
* **Safe Stdin EOF Transmission:** Decoupled pipe lifecycle ensuring standard input EOF is reliably delivered to container processes.
* **Adaptive Polling Sleep Backoff:** Execution loops in [`src/sandbox.rs`](src/sandbox.rs) employ exponential sleep backoff starting at 1ms (`sleep_ms = (sleep_ms * 2).min(20)`). Short jobs (~5ms–20ms) finish and return in ~1ms–3ms rather than waiting out a coarse fixed sleep timer, reducing queue wait latency by ~70% and increasing worker active execution time from ~66% to ~90%.

### 2.6 Automated Lifecycle Management, Pop-on-Poll & Lock Sharding
* **32-Shard Lock Striping:** Completed job outputs reside in an in-memory, lock-striped [`ResultStore`](src/store.rs) partitioned into 32 independent shards, eliminating lock contention between concurrent worker writes and poller reads.
* **Zero Input Retention (DTO Projection):** [`JobResult`](src/model.rs) stores only execution state and output (`job_id`, `client_id`, `status`, `stdout`, `stderr`, `exit_code`, `error_message`, timestamps)—never retaining duplicate copies of submitted `code` or `stdin` payloads in `ResultStore`.
* **Owner-Verified Pop-on-Poll (`poll_for_client`):** Once a job reaches a terminal state (`Done`, `Failed`, or `TimedOut`) and the rightful owner (`result.client_id == key_hash`) calls `POLL`, the result is immediately popped (removed) from `ResultStore` upon delivery.
* **Bounded Shard Capacity & Disconnect Purge:** Each shard is capped at `DEFAULT_MAX_SHARD_RESULTS` (`256` entries/shard), evicting expired entries first and falling back to FIFO eviction of the oldest completed result if a client submits jobs without polling. Additionally, when a connection is kicked on idle timeout or key expiry, `remove_client_results` purges all unpopped results for that client across all shards.
* **Background TTL Reaper:** An asynchronous background reaper periodically sweeps and evicts expired records shard-by-shard, preventing stop-the-world latency spikes and memory leaks.

### 2.7 Dynamic Hardware Profiling & Empirical Auto-Scaling ([`src/machine_config.rs`](src/machine_config.rs))
* **Host & Docker Resource Auto-Detection:** Automatically discovers physical host CPU cores and memory limits across macOS Darwin and Linux. In containerized or VM-backed environments (e.g., Docker Desktop on macOS), queries the Docker daemon (`docker info`) to determine effective CPUs and memory ceiling allocated to containers.
* **Empirically Proven Sizing Model (`--auto` / `DISTJOB_AUTO=1`):**
  * **Worker Threads (`W`):** Balances CPU cores against a 60% container memory budget (2 × 128 MB per execution slot to account for concurrent active + standby warm containers):
    `worker_count = clamp(min(cpu_workers, mem_workers), 2, 16)`
  * **Pre-Warmed Pool (`P`):** Scaled 1:1 with workers (`P = W`) so all workers can burst immediately without cold start overhead.
  * **Per-Client In-Flight Cap (`C`):** Sized to `max(2, floor(W / 3))` to prevent single-tenant starvation while allowing fair concurrency.
  * **ResultStore Shards (`S`):** Scaled to `max(16, next_power_of_two(W * 4))` to eliminate lock striping contention.
* **Preserved CLI Overrides:** Explicit CLI flags (e.g. `--auto -w 12`) take precedence over the auto-scaled recommendations while auto-scaling all other unconfigured parameters.
* **Zero-Dependency Profiler Dashboard (`visualize_profile.py`):** Converts OS-level profile traces (e.g. macOS `/usr/bin/sample`) into an interactive, visual HTML dashboard (`profile_visualizer.html`) displaying thread distributions, call tree weights, and lock bottlenecks.

---

## 3. Empirical Performance Benchmarks

> **Note on methodology.** Sections 3.1–3.2 were produced with the original `stress_test.sh` generator, whose summary averages counted rejected submits as 0 ms waits. In 3.2, the on-demand column reuses the 3.1 heavy-preset figures (13.14 s, 4.79 req/s) while the pre-warmed column comes from the light preset (29 requests), so the two columns are not the same workload. Treat 3.1–3.2 as indicative. Sections 3.3–3.5 use the v2 load generator, which counts rejected and failed jobs separately and takes latency from server-side timestamps.

### 3.1 Multi-Tenant Fairness Under Contention (128 Burst Requests)
Stress testing against asymmetric multi-tenant workloads (6 clients, 128 burst requests: 85 High-Freq spammers, 33 Med-Freq, 10 Low-Freq legitimate users):

| Metric | Baseline (Single-Queue) | Current (Two-Queue + Admission Control) | Improvement |
| :--- | :---: | :---: | :---: |
| **Total Test Duration** | `31.20s` | **`13.14s`** | **58% faster** (2.37x speedup) |
| **System-Wide Average Wait** | `15,371.3 ms` | **`3,052.4 ms`** | **5.0x lower latency** |
| **Broker Throughput** | `4.10 req/s` | **`4.79 req/s`** | **+16.8% higher throughput** |
| **Spammer P95 Tail Latency** | `27,861 ms` | **`11,195 ms`** | **59.8% reduction** |
| **Legitimate Tenant Queue Wait** | `2,148.8 ms` | **`1,764.7 ms`** | **Zero starvation** |
| **Legitimate Tenant Success Rate**| `100% (10/10)` | **`100% (10/10)`** | **Protected from noisy neighbors** |

### 3.2 Pre-Warmed Pool Acceleration vs On-Demand Cold Starts (Light Workload)
Benchmarking the impact of the **Pre-Warmed "Bake and Burn" Container Pool** against standard on-demand `docker run`:

| Metric | On-Demand Cold Starts (`docker run`) | Pre-Warmed Pool ("Bake and Burn") | Empirical Improvement |
| :--- | :---: | :---: | :---: |
| **Overall System Throughput** | `4.79 req/s` | **`10.79 req/s`** | **+125.3% Higher Throughput (2.25x)** |
| **Low-Frequency Client Turnaround** | `2,800.0 ms` | **`321.0 ms`** | **88.5% Latency Drop (8.7x faster)** |
| **Total Execution Duration** | `13.14s` | **`2.69s`** | **4.88x Faster Completion** |
| **Completion Rate** | `100%` | **`100% (29/29)`** | **Zero Failures / Zero Drops** |
| **Fairness Multiplier** | `1.4x` | **`4.1x lower latency`** | **Legitimate users bypass spammer backlog** |

### 3.3 Reproducible Results with the v2 Load Generator

These runs use [`stress_test.py`](stress_test.py), a single-process asyncio generator (one persistent TCP connection per simulated client, per-client poll pacing below the 25/s poll quota). In every run below it used 0.02–0.04 CPU cores (at most 0.4% of the machine), so it did not compete with the broker or Docker for CPU.

**Metric definitions**
* **srv p50 / p95 / p99:** job latency from server timestamps (`created_at_ms` to `completed_at_ms`), unaffected by client polling delay. Percentiles cover completed jobs only.
* **Goodput:** completed jobs divided by wall-clock time. Rejected jobs are not counted as work done.
* **Rejected:** submits refused by admission control. **Errors** (connection failures, poll errors, give-ups) were **0 in every run below**, and every admitted job completed.
* **Low-Freq p50:** server-side median latency of the 10 Low-Freq jobs (small sample, indicative only).

**Test environment (MacBook Air)**

| Item | Value |
| :--- | :--- |
| Machine | MacBook Air, 10 cores given to Docker Desktop, 8 GB RAM given to Docker Desktop |
| Sandbox | Docker with pre-warmed pool enabled |
| Workers / pool | 64 / 64 for 3.3.1 and 3.3.2, 10 / 10 for 3.3.3 |
| Per-client in-flight cap | Default (`2`), so 6 clients allow at most 12 jobs in flight |

#### 3.3.1 Baseline Presets (default schedule, `--speed 1`, 64 workers / 64 pool)

| Preset | Sent | Rejected | Done | Wall | Goodput | srv p50 | srv p95 | srv p99 | Low-Freq p50 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| light | 29 | 0 | 29 | 2.00 s | 14.48 jobs/s | 96 ms | 670 ms | 730 ms | 81 ms |
| standard (run 1) | 69 | 0 | 69 | 4.80 s | 14.37 jobs/s | 298 ms | 1,600 ms | 2,070 ms | 119 ms |
| standard (run 2) | 69 | 0 | 69 | 4.80 s | 14.37 jobs/s | 228 ms | 1,714 ms | 1,993 ms | 127 ms |
| heavy (run 1) | 128 | 4 | 124 | 7.20 s | 17.21 jobs/s | 757 ms | 4,681 ms | 4,958 ms | 469 ms |
| heavy (run 2) | 128 | 3 | 125 | 7.20 s | 17.35 jobs/s | 976 ms | 5,140 ms | 5,586 ms | 600 ms |
| heavy (run 3) | 128 | 6 | 122 | 7.20 s | 16.94 jobs/s | 707 ms | 4,329 ms | 4,654 ms | 470 ms |

At the default schedule, wall time equals the slowest client's submission schedule (2.0 s light, 4.8 s standard, 7.2 s heavy). Goodput in this table is therefore capped by the preset itself, not by broker capacity. Use `--speed` (below) to find the real ceiling.

#### 3.3.2 Saturation Sweep (heavy preset, `--speed` compresses submit intervals, 64 workers / 64 pool)

| `--speed` | `--warmup` | Rejected | Done | Wall | Goodput | srv p50 | srv p95 | srv p99 | Low-Freq p50 |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 10 | 0 | 34 | 94 | 5.13 s | 18.31 jobs/s | 1,981 ms | 4,234 ms | 5,039 ms | 541 ms |
| 30 | 0 | 37 | 91 | 5.07 s | 17.94 jobs/s | 2,063 ms | 4,455 ms | 4,845 ms | 285 ms |
| 50 | 0 | 37 | 91 | 5.01 s | 18.16 jobs/s | 1,996 ms | 4,431 ms | 4,885 ms | 214 ms |
| 100 | 0 | 37 | 91 | 5.32 s | 17.12 jobs/s | 2,360 ms | 5,020 ms | 5,164 ms | 370 ms |
| 30 | 10 | 37 | 91 | 4.86 s | 18.74 jobs/s | 1,933 ms | 4,532 ms | 4,635 ms | 286 ms |
| 50 | 10 | 37 | 91 | 4.91 s | 18.53 jobs/s | 2,021 ms | 4,622 ms | 4,746 ms | 264 ms |

Observations:
* **Saturation:** goodput stays at roughly 17–19 jobs/s from `--speed 10` through `--speed 100`, and p95 latency stays within about 4.2–5.0 s. Beyond saturation, extra offered load is shed by admission control (rejections plateau at 37 of the 85 High-Freq submits) instead of growing latency.
* **Where rejections land:** all rejections were High-Freq submits. Med-Freq (33/33) and Low-Freq (10/10) jobs were all admitted and completed in every saturated run.
* **Fairness signal:** Low-Freq p50 was 214–541 ms against 1,933–2,360 ms overall.
* **Warm-up:** `--warmup 10` changed goodput by +2% to +4%, which is within the spread seen between repeated runs, so no conclusion is drawn from it.

#### 3.3.3 Worker Count: 64 / 64 vs 10 / 10 (heavy preset, `--warmup 10`)

| Config | Flags | Rejected | Done | Wall | Goodput | srv p50 | srv p95 | srv p99 | Low-Freq p50 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 64 / 64 | `--speed 50` | 37 | 91 | 4.91 s | 18.53 jobs/s | 2,021 ms | 4,622 ms | 4,746 ms | 264 ms |
| 10 / 10 | `--speed 50` | 39 | 89 | 4.23 s | 21.04 jobs/s | 2,034 ms | 3,774 ms | 4,120 ms | 724 ms |
| 10 / 10 | `--speed 100` | 40 | 88 | 4.20 s | 20.96 jobs/s | 1,994 ms | 3,892 ms | 4,004 ms | 966 ms |
| 10 / 10 | light, `--speed 100` | 0 | 29 | 1.42 s | 20.41 jobs/s | 699 ms | 1,309 ms | 1,340 ms | 118 ms |

Observations:
* Dropping from 64 to 10 workers raised goodput by about 13% and lowered p95 by about 18%. With the default per-client cap, at most 12 jobs can be in flight in this preset, so workers beyond that sit idle, while a 64-container pool consumes RAM and refill CPU.
* The tradeoff is Low-Freq p50 latency: 724–966 ms at 10 workers against 214–370 ms at 64. With every worker busy, a small tenant's job can wait for a free worker, whereas 64 workers always leave one free. This is an untested explanation, and each figure is a single run.

### 3.4 Cross-Environment Comparison (heavy preset)

| Environment | Cores | Workers / pool | Load generator | Peak heavy goodput |
| :--- | :---: | :---: | :--- | :---: |
| MacBook Air + Docker Desktop | 10 | 64 / 64 | bash (`stress_test.sh`) | 17.9 jobs/s |
| MacBook Air + Docker Desktop | 10 | 64 / 64 | v2 async (`--speed` sweep) | 18.7 jobs/s |
| MacBook Air + Docker Desktop | 10 | 10 / 10 | v2 async | 21.0 jobs/s |
| GitHub Codespaces | 4 | 4 / 4 | bash (`stress_test.sh`) | 6.79 jobs/s |
| GitHub Codespaces | 4 | 4 / 4 | v2 async (`--speed` sweep) | 6.86 jobs/s |

Observations:
* **The generator was not the bottleneck.** Replacing the process-spawning bash generator with the v2 generator changed Codespaces from 6.79 to 6.86 jobs/s, so the gap to the MacBook is not explained by generator CPU.
* **This is not a like-for-like hardware comparison.** The machines ran different worker counts (4 versus 10 to 64) as well as different core counts, so the roughly 3x throughput difference mixes concurrency and hardware.
* **Time per worker slot (assuming workers are the binding limit):** 10 workers / 21 jobs/s is about 0.48 s of worker time per job on the MacBook, and 4 workers / 6.86 jobs/s is about 0.58 s on Codespaces. Part of each figure is the sleep-based jobs in the test mix (roughly 0.08 s on average); the rest is container and interpreter start-up cost.

### 3.5 How to Read These Numbers

* **Single runs.** Most rows are one run. Observed spread was about ±1% between repeated heavy baseline runs (16.94–17.35 jobs/s) and about ±4% between saturated 64 / 64 runs (17.12–18.74 jobs/s). Differences smaller than about 5% should be treated as noise.
* **Goodput at the default schedule is preset-bound** (see 3.3.1). Only the `--speed` runs approach the system's capacity.
* **Local hardware.** Results were measured on a laptop (shared CPU, thermal limits, Docker Desktop VM) and a shared cloud VM, not dedicated servers. Treat them as directional.
* **Not yet measured:** a matched worker-count matrix (for example 4, 8 and 12 workers on both machines), CPU utilisation during runs, the warm-pool hit ratio, and an nsjail or microVM backend on native Linux.

---

## 4. Current Architectural Gaps & Known Limitations

While the broker core, authentication layer, and execution engine are robust and fair, the current implementation has several recognized design constraints:

1. **Thread-Per-Connection Ingress in Sync Server [Resolved in Async Server]:**
   * *In Synchronous Mode ([`src/server.rs`](src/server.rs)):* Each active TCP connection spawns a dedicated OS thread (`std::thread::spawn`). While physical RSS memory per idle thread is small (~16 KB), holding thousands of idle connections reserves gigabytes of virtual memory and risks reaching OS thread ceilings (`EAGAIN` / `os error 11`).
   * *In Asynchronous Mode ([`src/server_async.rs`](src/server_async.rs)):* Fully resolved via the Tokio event loop (`--async`), which multiplexes thousands of connections over `epoll`/`kqueue` using a fixed set of threads.
2. **Synchronous Socket I/O (in Sync Model):**
   * Connection threads in the sync server block on `BufReader::read_line()`. A slow-loris client transmitting single bytes with long delays ties up an entire OS thread in sync mode (mitigated when running with `--async`).
3. **Single-Node In-Memory Execution Queues & Result Store:**
   * While authentication keys and penalty states are backed by Redis (L2), the active scheduler queues and [`ResultStore`](src/store.rs) entries reside in the memory of a single broker process. If the broker restarts, pending jobs and uncollected results are lost.
4. **Container Startup Latency (Mitigated by Pre-Warmed Pool):**
   * *Status:* Container initialization overhead is now mitigated by the asynchronous [`PrewarmedContainerPool`](src/sandbox.rs), dropping warm execution startup to sub-20ms. Under sustained bursts that temporarily exhaust the pool, the broker gracefully falls back to on-demand `docker run` (~300ms–800ms). MicroVMs (e.g., AWS Firecracker) represent the subsequent optimization frontier.
   * *Measured ceiling:* Under saturation (section 3.3), throughput topped out at about 21 jobs/s on a 10-core MacBook with Docker Desktop and about 7 jobs/s on a 4-vCPU Codespace, which works out to roughly 0.5 s of worker time per job. Per-job container and interpreter cost, not the scheduler, is the limiting factor.

---

## 5. Future Work & Engineering Roadmap

```mermaid
flowchart LR
    subgraph Milestone1["Phase 1: Event-Driven Ingress (Implemented)"]
        M1["Tokio Async Runtime<br/>• Non-blocking epoll/kqueue<br/>• Fixed OS thread pool<br/>• Green task per socket (--async)"]
    end

    subgraph Milestone2["Phase 2: Execution Acceleration (Implemented)"]
        M2["Pre-Warmed Sandbox Pool<br/>• Standby container pool<br/>• Sub-20ms container dispatch<br/>• Dynamic image fallback"]
    end

    subgraph Milestone3["Phase 3: Two-Tier Auth & Rate Limiting (Implemented)"]
        M3["L1 Heap + L2 Redis Auth<br/>• SHA-256 secret_key identity<br/>• Cross-tenant job AuthZ<br/>• Poll spammer L2 demotion"]
    end

    subgraph Milestone4["Phase 4: Distributed State & Clustering"]
        M4["Persistent Store & Clustering<br/>• Redis / Raft job queue backend<br/>• Multi-node worker clustering<br/>• Resilient crash recovery"]
    end

    Milestone1 --> Milestone2 --> Milestone3 --> Milestone4
```

* **Phase 1: Async Event-Driven Network Ingress (`tokio`):** *(Implemented in [`src/server_async.rs`](src/server_async.rs))*
  Implemented a non-blocking event-driven reactor powered by Tokio. Connections are handled as green tasks yielding on I/O, allowing thousands of concurrent idle/polling clients on a fixed thread pool.
* **Phase 2: Pre-Warmed Container Pools:** *(Implemented in [`src/sandbox.rs`](src/sandbox.rs))*
  Maintains pre-warmed standby containers ready for immediate execution, dropping cold-start latency from ~500ms to sub-20ms.
* **Phase 3: Two-Tier L1/L2 Authentication & Poll Rate-Limiting:** *(Implemented in [`src/auth.rs`](src/auth.rs))*
  Replaces client-supplied IDs with server-issued `secret_key` tokens hashed via SHA-256 across an in-process L1 heap cache and L2 Redis instance (`DB 1`), enforcing job ownership and demoting polling spammers to L2 probation.
* **Phase 4: Distributed Job Queuing & Persistence:**
  Decouple the broker ingress nodes from worker nodes using a distributed queue (e.g., Redis Streams, NATS, or a Raft log) and persist result records in durable distributed storage for high availability.
* **Phase 5: Explicit Cancellation Protocol:**
  Add a `CANCEL <secret_key> <job_id>` command allowing clients to abort runaway jobs voluntarily before their wall-clock timeout expires.

---

## 6. Wire Protocol Specification

The broker natively supports two wire protocols over the same TCP listener:

### 6.1 JSON Lines Protocol (Recommended)

#### 1. Register to Obtain a Server-Issued `secret_key`
**Request:**
```json
{"action": "REGISTER"}
```
**Response:**
```json
{"status": "ok", "secret_key": "sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10"}
```

#### 2. Submit a Job
**Request:**
```json
{"action": "SUBMIT", "secret_key": "sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10", "language": "python", "code": "print(2 ** 10)", "stdin": "", "timeout_secs": 5}
```
**Response (Success):**
```json
{"status": "ok", "job_id": "a9844f25-82b5-4b36-a1a2-cb31a61c33f2"}
```
**Response (Queue Rate Limited):**
```json
{"status": "error", "error": "Rate limit exceeded: client '<sha256_hash>' reached maximum queue capacity of 10 pending jobs (in-flight cap: 2, multiplier: 5)"}
```

#### 3. Poll for Result
**Request:**
```json
{"action": "POLL", "secret_key": "sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10", "job_id": "a9844f25-82b5-4b36-a1a2-cb31a61c33f2"}
```
**Response (Popped from `ResultStore` once terminal):**
```json
{
  "job_id": "a9844f25-82b5-4b36-a1a2-cb31a61c33f2",
  "client_id": "e44e628a3c22186e4e6f7ccab624df3bc121099e96e1492fc3e0d21ebd898392",
  "status": "Done",
  "stdout": "1024\n",
  "stderr": "",
  "exit_code": 0,
  "error_message": null,
  "created_at_ms": 1711468000000,
  "completed_at_ms": 1711468000050,
  "execution_duration_ms": 50
}
```

---

### 6.2 Redis-Style Text Protocol (for `nc` / `telnet`)

| Command | Syntax | Success Response | Error Response |
|---|---|---|---|
| **Register Key** | `REGISTER` (or `AUTH`) | `+sk_<uuid>\r\n` | `-ERR <reason>\r\n` |
| **Submit Job** | `SUBMIT <secret_key> <language> <code...>` | `:<job_id>\r\n` | `-ERR <reason>\r\n` |
| **Poll Job** | `POLL <secret_key> <job_id>` | `+<Status> <json_result>\r\n` | `-ERR Unauthorized / Job not found\r\n` |
| **Check Status** | `STATUS <secret_key> <job_id>` | `+<Status>\r\n` | `-ERR Unauthorized / Job not found\r\n` |
| **Health Check** | `PING` | `+PONG\r\n` | — |
| **Server Telemetry** | `STATS` | `+OK active_clients=... queued=... in_flight=... stored_results=... dispatchable=... waiting=...\r\n` | — |

---

## 7. Configuration Reference

All settings can be specified via [`Config`](src/config.rs) or environment variables:

| Option | Env Variable | Default | Description |
|---|---|---|---|
| **Auto-Scaling** | `DISTJOB_AUTO` | `false` | Automatically scales workers, pool size, client cap, and shards (`--auto`) |
| **Bind Address** | `DISTJOB_BIND_ADDR` | `127.0.0.1:7000` | TCP listener bind address |
| **Connection Model** | `DISTJOB_ASYNC` | `false` | Toggles Tokio async model (`true`/`1` via `--async`, or `--sync`) |
| **Workers** | `DISTJOB_WORKERS` | `4` | Number of execution worker threads (`-w`) |
| **Per-Client Cap** | `DISTJOB_CLIENT_CAP` | `2` | Max concurrent in-flight jobs per client (`-c`) |
| **Enable Queue Limit** | `DISTJOB_ENABLE_QUEUE_LIMIT` | `true` | Toggles per-client pending queue rate limiting |
| **Queue Multiplier** | `DISTJOB_QUEUE_LIMIT_MULTIPLIER` | `5` | Multiplier defining queue limit: `cap * multiplier` |
| **Two-Queue Scheduler**| `DISTJOB_ENABLE_TWO_QUEUE_SCHEDULER` | `true` | Toggles $O(1)$ two-queue engine vs legacy single-queue loop |
| **Pre-Warmed Pool** | `DISTJOB_ENABLE_PREWARMED_POOL` | `true` | Toggles standby pre-warmed container pool |
| **Pool Size** | `DISTJOB_PREWARMED_POOL_SIZE` | `4` | Number of standby containers kept in warm pool (`-p`) |
| **Store Shards** | `DISTJOB_STORE_SHARDS` | `32` | Number of lock-striped shards in ResultStore (`-s`) |
| **Python Image** | `DISTJOB_PYTHON_IMAGE` | `distjob-python:latest` | Docker image with pre-baked libraries |
| **Sandbox Type** | `DISTJOB_SANDBOX` | `docker` | Sandbox engine: `docker`, `local`, or `mock` |
| **Docker Runtime** | `DISTJOB_DOCKER_RUNTIME` | (none) | e.g. `runsc` for gVisor syscall isolation |
| **Result TTL** | `DISTJOB_TTL_SECS` | `600` | Retention duration for completed job results (seconds) |
| **Redis L2 URL** | `DISTJOB_REDIS_URL` | `redis://127.0.0.1:6379/1` | Redis L2 cache connection URI (logical database `1`) |
| **L1 Max Capacity** | `DISTJOB_L1_CAP` | `10000` | Maximum hashed sessions held in the in-process L1 heap cache |
| **L1 Session TTL** | `DISTJOB_L1_TTL_SECS` | `120` | Inactivity TTL (seconds) before an L1 session expires to L2 |
| **L2 Redis TTL** | `DISTJOB_L2_TTL_SECS` | `3600` | Retention TTL (seconds) for `auth:<key_hash>` in Redis L2 |
| **Max Polls / Sec** | `DISTJOB_MAX_POLLS_PER_SEC` | `25` | Maximum allowed `POLL` / `STATUS` requests per second per user |
| **Max Poll Strikes** | `DISTJOB_MAX_POLL_STRIKES` | `3` | Poll quota violations before eviction from L1 to L2 probation |
| **L2 Penalty TTL** | `DISTJOB_L2_PENALTY_SECS` | `60` | Duration (seconds) a demoted spammer is blocked from L1 promotion |
| **Client Idle Timeout** | `DISTJOB_IDLE_TIMEOUT_SECS` | `300` | Max socket inactivity (seconds) before kicking an idle connection |

---

## 8. Build, Test & Run

### 8.1 Build Pre-Baked Sandbox Image
```bash
# Build custom image with NumPy, Pandas, SciPy, SymPy, and precompiled .pyc bytecode
docker build -t distjob-python:latest .
```
> For a line-by-line breakdown of the Dockerfile, layer optimizations, and security profile, see [`docker_image.md`](docker_image.md).

### 8.2 Build & Test Suite
```bash
# Compile project
cargo build --release

# Run all unit and integration tests
cargo test
```

### 8.3 Launch Broker

The broker can run using either the **Asynchronous (Tokio)** or **Synchronous** connection model, with manual configuration or automatic hardware sizing:

```bash
# Auto-scaled server sizing derived from detected host/Docker hardware
cargo run --release -- --auto

# Auto-scaled server with Async Tokio connection model
cargo run --release -- --auto --async

# Auto-scale with explicit override (e.g., auto-scale pool & shards, but force 12 workers)
cargo run --release -- --auto -w 12

# Asynchronous Tokio connection model (manual sizing)
cargo run --release -- --async 127.0.0.1:7000

# Asynchronous model with 16 workers and 16 pre-warmed standby containers
cargo run --release -- --async --workers 16 --pool-size 16 127.0.0.1:7000

# Synchronous thread-per-connection model (default)
cargo run --release -- --sync 127.0.0.1:7000

# Run with Local process sandbox (fast dev mode without Docker dependency)
cargo run --release -- --async --local 127.0.0.1:7000
```

### 8.4 Quick Smoke Test via Netcat
```bash
nc 127.0.0.1 7000
REGISTER
# Response: +sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10

SUBMIT sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10 python print("Hello from Sandboxed Broker!")
# Response: :<job_id>

STATUS sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10 <job_id>
# Response: +Done

POLL sk_9c8b1a2e4f3d4b5c8a7e6f5d4c3b2a10 <job_id>
# Response: +Done {"job_id": ...}
```

### 8.5 Run Stress Test Suite
```bash
# Fast validation test (~3-5s, 4 clients, 29 requests)
./stress_test.sh --preset light
# Alternatively via Python test harness:
python3 stress_test.py --preset light

# Standard multi-tenant test (~25-35s)
./stress_test.sh --preset standard

# High-concurrency heavy stress test (6 clients, 128 burst requests)
./stress_test.sh --preset heavy
```

The Python generator (`stress_test.py`) is the recommended way to benchmark. It runs as a single process with one persistent connection per client, reports rejected and failed jobs separately, prints its own CPU usage, and takes latency from server timestamps. The bash generator (`stress_test.sh`) starts several processes per poll, so on small machines it can use a noticeable share of the CPU it is measuring. Use it for quick smoke tests only.

```bash
# Default schedule (goodput is capped by the preset's submission schedule)
python3 stress_test.py --preset heavy

# Saturation test: compress submit intervals 50x, warm the pool first, save raw results
python3 stress_test.py --preset heavy --speed 50 --warmup 10 --json run1.json

# Run the generator from a different machine than the broker
python3 stress_test.py --host <broker-ip> --port 7000 --preset heavy --speed 50
```

| Flag | Default | Description |
|---|---|---|
| `--host` / `--port` | `127.0.0.1` / `7000` | Broker address |
| `--preset` | `standard` | `light` (alias `quick`), `standard`, or `heavy` |
| `--speed` | `1.0` | Divides submit intervals, so `50` offers 50x the load rate |
| `--warmup` | `8` | Throwaway jobs run before measuring to fill the warm pool (`0` disables) |
| `--poll-interval` | `0.1` | Seconds between polls for each job |
| `--max-poll-rate` | `20` | Per-client polls/sec cap, keep below `DISTJOB_MAX_POLLS_PER_SEC` |
| `--job-deadline` | `120` | Give up on a job after this many seconds |
| `--json` | (none) | Write raw per-job records and the summary to a file |

For comparisons between configurations or machines, use the same preset, `--speed`, and commit, record `--workers` and `--pool-size`, and repeat each run at least three times.

### 8.6 Local Testing Workbench GUI (`testing_gui/`)
```bash
# Launch the zero-dependency HTTP-to-TCP testing workbench on http://127.0.0.1:8080
python3 testing_gui/app.py --port 8080 --broker-host 127.0.0.1 --broker-port 7000
```
Open **[http://127.0.0.1:8080](http://127.0.0.1:8080)** to interactively manage persistent TCP sockets (`peer_auth_map`), test single-socket duplicate `REGISTER` rejection, watch idle/key-expired connections get kicked, submit jobs with custom payload padding, and inspect live `STATS` and wire transcripts.