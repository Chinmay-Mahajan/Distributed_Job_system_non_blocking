# Sandboxed Job Execution Broker — Architecture Proposal (v2)

> Prototype design doc. Successor to the earlier single-threaded-lock
> `DispatchLite` broker. Goal here is **remote code execution as a
> service**: users submit code, it runs in an isolated sandbox, they poll
> for the result. This doc exists so future-me can re-derive the reasoning
> without re-running the whole conversation.

---

## 1. High-level flow

```
Client ──SUBMIT(code, stdin, lang)──▶  Server (1 thread/connection)
                                             │
                                             ▼
                               Per-client queue (FIFO)
                                             │
                              ┌──────────────┴──────────────┐
                              │  Two-level scheduler:        │
                              │  round-robin across clients, │
                              │  FIFO within a client,       │
                              │  capped at N in-flight/client│
                              └──────────────┬──────────────┘
                                             ▼
                                Worker pool (Docker + gVisor,
                                network off, mem/cpu/proc limits)
                                             │
                                             ▼
                              Result store (job_id → output),
                              keyed, with TTL

Client ──POLL(job_id)──▶  Server ──▶ {status, stdout, stderr, submitted job}
```

Return an opaque `job_id` immediately on SUBMIT. Never block a connection
thread on job execution — execution happens fully decoupled, client polls
for the result later.

---

## 2. Decisions made, and why

### 2.1 No caching
Considered "cache deterministic results per user," rejected. Code
execution isn't reliably deterministic (wall-clock, RNG seeds, floating
point across CPUs, thread scheduling in concurrent code all break it even
with network off), and a *confidently wrong stale result* is worse than a
slower correct one. Caching is a better fit for something like LLM API
calls, where a memoized answer is an acceptable trade for cost savings —
not for this.

### 2.2 One thread per connection
Chosen for prototype simplicity. Each thread only handles networking
(accept SUBMIT → enqueue → return ID; accept POLL → look up result store)
— it must never block on job execution itself. This is the fix for v1's
core flaw, where a lock was held for the full duration of a command
including execution.

**Known ceiling (not fixed now, noted for later):** thread-per-connection
doesn't scale past a few thousand concurrent connections (the "C10K
problem") — each idle connection holds a full OS thread/stack. The proper
fix later is an async/event-driven model (e.g. `tokio` in Rust), where one
thread multiplexes many connections via an event loop. Not urgent for a
prototype; worth knowing it's there.

### 2.3 Per-client queue: FIFO, not stack
Queue chosen over stack. A stack (LIFO) lets a user's newest submission
jump ahead of their own older ones — fine for a "debounced autosave /
live-REPL, only the latest matters" use case, but not the default here,
since it can starve a user's own earlier jobs indefinitely under heavy
resubmission. If a LIFO use case shows up later, it should be an explicit,
documented choice — not the default.

### 2.4 Fairness: two-level scheduler + per-client concurrency cap
Two distinct mechanisms, both needed, not interchangeable:

- **Per-client cap (top-N in-flight):** a client can never have more than
  `N` jobs running at once. Implemented as a counter per client,
  incremented on dispatch, decremented on completion; refuse to dispatch
  more of that client's jobs once at `N`. Guarantees no client can occupy
  more than `N / total_workers` of total capacity.
- **Round-robin dispatch across clients:** the cap alone only limits *how
  many* of a client's jobs run concurrently — it says nothing about
  *which* client gets served next when a worker frees up. Without
  round-robin, naive iteration order (e.g. queue/map insertion order)
  quietly re-introduces unfairness. Dispatch loop: worker frees → walk
  clients in round-robin order → pick first one with `in_flight < N` and a
  non-empty queue → pop their oldest job → dispatch, increment counter.

**Known trade-off, accepted for the prototype:** a hard per-client cap
sacrifices utilization for fairness — a lone active client with N=2 and 10
idle workers still only gets 2 concurrent jobs, leaving 8 workers idle
even with zero contention. A "soft cap that only activates under
contention" (let a lone client use free workers, throttle only once a
second client shows up) is the more efficient design but meaningfully more
complex (requires contention detection); explicitly deferred past v1 of
this architecture.

### 2.5 Sandbox: Docker + gVisor, network off, resource limits
gVisor chosen deliberately over relying on the container boundary alone —
it intercepts syscalls, which matters for genuinely untrusted RCE (this is
the same reasoning behind Google Cloud Run/Cloud Functions' sandboxing
choice). Required limits, not optional:

- **Network off** — kills most nondeterminism and most exfiltration risk.
- **Memory limit** (cgroups)
- **CPU limit** (cgroups) — memory limit alone doesn't stop an infinite
  loop with flat memory usage from starving the host.
- **Process/thread limit** — stops fork bombs.
- **Hard wall-clock timeout, enforced by the broker** — kill the container
  if it overruns; never trust sandboxed code to self-terminate.
- **Read-only root filesystem** + small writable tmpfs with a disk quota —
  stops disk-fill attacks even with process limits in place.
- **Ephemeral, single-use containers** — never reuse a container across
  two different users' jobs. Reuse is how sandbox state leaks cross-user.
- **No Docker socket mounted inside the container**, ever.

**Known cost:** cold-starting a fresh gVisor+Docker sandbox per job is
hundreds of ms to low seconds — not free. See §2.7 (warm pool) for the
standard mitigation.

### 2.6 Result delivery: async poll, not sync block
Client gets a `job_id` back immediately on SUBMIT and polls
`STATUS/RESULT job_id` later, receiving `{status, stdout, stderr,
original submitted job}` once done. This is what makes §2.2 possible —
a synchronous "hold the connection open until the job finishes" model
would re-couple networking capacity to execution latency, recreating v1's
central flaw one layer up.

Explicit job state machine required:
```
Pending → Running → Done | Failed | TimedOut
                  ↘ Reclaimed   (if/when a worker-crash lease mechanism
                                 is added — see §4)
```

**Result store needs a TTL.** Without one, a client that submits and never
polls leaves its result in memory forever — a slow, silent leak. Expire
results after some fixed window (e.g. 10 minutes) even in the prototype.

### 2.7 Dynamic worker scaling — deferred, not solved yet
Desired eventually (scale worker count with connection/submission
density), but flagged as the hardest, least-baked part of this design.
Known pitfalls to design around when this is tackled for real:

- **Cold-start lag** means density-triggered scale-up reacts a beat late.
  The standard fix is a **warm pool**: idle pre-spun sandboxes ready
  instantly, replenished in the background, rather than spin-on-demand.
- **Oscillation/thrashing** under bursty load — needs hysteresis
  (different scale-up vs. scale-down thresholds) and a cooldown period
  between scaling actions.
- **Never kill a worker mid-job** — drain (stop assigning new jobs, wait
  for current job with a timeout) before tearing down.
- **Hard ceiling required** — "dynamic" must still have a max, or one
  client flooding submissions can force unbounded container creation and
  exhaust the host (cost + resource-exhaustion risk, not just perf).

**Decision:** ship v1 with a **fixed-size worker pool**, sized generously.
Get the two-level scheduler and result/poll lifecycle correct and tested
first. Add dynamic scaling as a deliberate, separately-tested phase two —
bolting it onto an unproven scheduler makes both harder to debug at once.

---

## 3. Component checklist for a first working prototype

- [x] TCP server, thread-per-connection, non-blocking on execution
- [x] Per-client FIFO queue (isolated from other clients')
- [x] Round-robin-across-clients + FIFO-within-client dispatch loop
- [x] Per-client `in_flight` counter, hard cap at N
- [x] Fixed-size worker pool (start here — no dynamic scaling yet)
- [x] Docker + gVisor sandbox: network off, memory/CPU/process limits,
      read-only rootfs + quota'd tmpfs, ephemeral per job
- [x] Broker-enforced hard wall-clock timeout per job
- [x] Job state machine (Pending/Running/Done/Failed/TimedOut)
- [x] Result store keyed by job_id, with TTL-based expiry
- [x] SUBMIT → job_id; POLL job_id → {status, stdout, stderr, job}

## 4. Explicitly out of scope for v1 (parking lot)

- Dynamic worker autoscaling (§2.7)
- Soft per-client caps that only throttle under contention (§2.4)
- Worker-crash detection / lease-based job reclaiming (carried over open
  question from v1 broker — still unsolved here: if a worker dies mid-job,
  the job currently just sits in `Running` forever)
- Async/event-loop networking model to replace thread-per-connection
  (§2.2) — only needed once connection counts get large
- Cross-user cache-sharing policy — moot since caching was dropped
  entirely (§2.1), but would need revisiting if a *different*, genuinely
  deterministic workload (not arbitrary RCE) were ever layered on top