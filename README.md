# Sandboxed Job Execution Broker (v2)

Remote Code Execution (RCE) as a service: users submit arbitrary code, it executes within an isolated sandbox, and clients poll asynchronously for results.

This project implements the architecture proposal specified in [`plan.md`](file:///Users/chinmaymahajan/Desktop/IMP%20NOTES%20PDF/RUST/distributed_jobv2/plan.md).

---

## 1. Architecture Overview

```
Client ──SUBMIT(code, stdin, lang)──▶  Server (1 thread/connection)
                                             │ (returns opaque job_id immediately)
                                             ▼
                                  Per-client queue (FIFO)
                                             │
                               ┌─────────────┴──────────────┐
                               │  Two-level scheduler:       │
                               │  round-robin across clients,│
                               │  FIFO within a client,      │
                               │  capped at N in-flight/client│
                               └─────────────┬──────────────┘
                                             ▼
                                Worker pool (Docker + gVisor,
                                network off, mem/cpu/proc limits)
                                             │
                                             ▼
                                Result store (job_id → output)
                                with TTL-based expiry & cleanup

Client ──POLL(job_id)──▶  Server ──▶ {status, stdout, stderr, submitted job}
```

---

## 2. Implemented Features & Verification

All components from the [Architecture Proposal Checklist](file:///Users/chinmaymahajan/Desktop/IMP%20NOTES%20PDF/RUST/distributed_jobv2/plan.md#L166-L180) have been implemented and covered by unit and integration tests:

- [x] **TCP Server (thread-per-connection, non-blocking on execution)**:
  - Each incoming connection is handed to its own OS thread.
  - Returns opaque `job_id` immediately upon `SUBMIT`. Connection threads never block on job execution.
- [x] **Per-Client FIFO Queues**:
  - Each client maintains an isolated FIFO `VecDeque<Job>`.
- [x] **Round-Robin Across Clients + FIFO Within Client**:
  - The dispatch loop visits active clients in round-robin order, popping the oldest pending job for each eligible client.
- [x] **Per-Client In-Flight Cap (`max_in_flight_per_client`)**:
  - Enforces a hard cap $N$ (default: 2) on the number of concurrently running jobs per client to prevent resource monopolization.
- [x] **Fixed-Size Worker Pool**:
  - Sized worker pool (default: 4 threads) pulling jobs from the scheduler via condition variables.
- [x] **Isolated Docker + gVisor Sandbox**:
  - Ephemeral single-use containers (`--rm`) with unique names.
  - Network disabled (`--network none`).
  - Strict resource limits (`--memory 128m`, `--cpus 0.5`, `--pids-limit 64`).
  - Read-only root filesystem (`--read-only`) with writable tmpfs quota (`--tmpfs /tmp:rw,noexec,nosuid,size=64m`).
  - Read-only host mount for script code (`-v <tmp_dir>:/code:ro`).
  - No Docker socket mounted.
  - Optional gVisor runtime (`--runtime runsc`).
  - Local process and mock sandboxes for development/CI environments.
- [x] **Broker-Enforced Hard Wall-Clock Timeout**:
  - Broker monitors execution against `timeout_secs`. If exceeded, forcibly terminates container/process and marks the job as `TimedOut`.
- [x] **Job State Machine**:
  - `Pending` $\rightarrow$ `Running` $\rightarrow$ `Done` | `Failed` | `TimedOut`.
- [x] **Result Store with TTL Expiry**:
  - Results stored in memory keyed by `job_id` with background reaper purging entries older than TTL (default: 10 minutes).
- [x] **Dual Wire Protocol (JSON Lines + Text Command)**:
  - Supports modern JSON Lines as well as Redis-like text commands (`SUBMIT`, `POLL`, `STATUS`, `PING`, `STATS`).

---

## 3. Wire Protocol Specification

### JSON Lines Protocol (Recommended)

#### Submit a Job
Request:
```json
{"action": "SUBMIT", "client_id": "alice", "language": "python", "code": "print(2 ** 10)", "stdin": "", "timeout_secs": 5}
```
Response:
```json
{"status": "ok", "job_id": "a9844f25-82b5-4b36-a1a2-cb31a61c33f2"}
```

#### Poll for Result
Request:
```json
{"action": "POLL", "job_id": "a9844f25-82b5-4b36-a1a2-cb31a61c33f2"}
```
Response:
```json
{
  "job_id": "a9844f25-82b5-4b36-a1a2-cb31a61c33f2",
  "client_id": "alice",
  "status": "Done",
  "stdout": "1024\n",
  "stderr": "",
  "exit_code": 0,
  "error_message": null,
  "job": {
    "client_id": "alice",
    "language": "python",
    "code": "print(2 ** 10)",
    "stdin": "",
    "timeout_secs": 5
  },
  "created_at_ms": 1711468000000,
  "completed_at_ms": 1711468000050,
  "execution_duration_ms": 50
}
```

---

### Text Protocol (for `nc` / `telnet`)

| Command | Syntax | Success Response | Error Response |
|---|---|---|---|
| Submit Job | `SUBMIT <client_id> <language> <code...>` | `:<job_id>\r\n` | `-ERR <reason>\r\n` |
| Poll Job | `POLL <job_id>` | `+<Status> <json_result>\r\n` | `-ERR Job not found or expired\r\n` |
| Check Status | `STATUS <job_id>` | `+<Status>\r\n` | `-ERR Job not found or expired\r\n` |
| Health Check | `PING` | `+PONG\r\n` | — |
| Server Stats | `STATS` | `+OK active_clients=... queued=... in_flight=... stored_results=...\r\n` | — |

---

## 4. How to Run

### Run Broker (Docker Sandbox)
```bash
cargo run -- --docker 127.0.0.1:7000
```

### Run Broker (Local Process Sandbox for Dev / Testing without Docker)
```bash
cargo run -- --local 127.0.0.1:7000
```

### Run Tests
```bash
cargo test
```

### Run Example Client
```bash
python3 client.py
```

### Netcat Quick Test
```bash
nc 127.0.0.1 7000
SUBMIT user1 python print("Hello from netcat!")
# Output: :<job_id>
STATUS <job_id>
# Output: +Done
POLL <job_id>
# Output: +Done {"job_id": ...}
```

---

## 5. Configuration Options

| Option | Env Var | Default | Description |
|---|---|---|---|
| Bind Address | `DISTJOB_BIND_ADDR` | `127.0.0.1:7000` | Server TCP listener address |
| Workers | `DISTJOB_WORKERS` | `4` | Fixed number of worker threads |
| Per-Client Cap | `DISTJOB_CLIENT_CAP` | `2` | Max concurrent running jobs per client |
| Sandbox Type | `DISTJOB_SANDBOX` | `docker` | `docker`, `local`, or `mock` |
| Docker Runtime | `DISTJOB_DOCKER_RUNTIME`| (none) | e.g. `runsc` for gVisor syscall isolation |
| Result TTL | `DISTJOB_TTL_SECS` | `600` | Retention duration for completed jobs |
