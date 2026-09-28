# Idle User Simulation & OS Thread Resource Exhaustion Report

**Target Project:** Sandboxed Job Execution Broker (v2)  
**Date:** September 28, 2026  
**Test Suite:** `simulate_idle_clients.sh`  
**Test Protocol:** Raw TCP via `nc` (Netcat)  
**Host Environment:** macOS (Darwin / Apple Silicon)  

---

## 1. Executive Summary

This report documents the empirical investigation into the architectural hypothesis:
> *"Users who have not submitted any jobs and simply connect to the server via `nc` still hog an entire OS thread to themselves, and consume system memory."*

### Empirical Verdict: **CLAIM CONFIRMED (100% Architecturally & Empirically Verified)**

Key findings from our 2,000-connection live benchmark:
1. **1:1 Thread Allocation:** 2,000 idle TCP connections spawned exactly **2,000 dedicated OS threads** ($7 \rightarrow 2,007$ threads).
2. **The "16 KB" Idle Memory Illusion:** While physical resident memory (RSS) only increased by **33.6 MB** ($\approx 16.8\text{ KB}$ per idle thread), the virtual address space (VSZ) surged by **+4.38 GB** ($400.8\text{ GB} \rightarrow 405.2\text{ GB}$) due to OS stack reservations.
3. **Worker Pool Isolation Confirmed:** Even with 2,000 idle client threads blocked on socket I/O, an active execution probe job submitted during the test completed in **302 ms**, demonstrating that the execution `WorkerPool` remained decoupled from connection ingress.
4. **Clean Reclamation:** Upon closing the `nc` instances, all 2,000 OS threads were immediately destroyed, returning the broker to its 7-thread baseline.

---

## 2. Glossary of Systems Engineering Terms Explained

To understand why this behavior occurs and its architectural implications, the following operating system and concurrency concepts are defined below:

### 2.1 OS Thread (Kernel / Native Thread)
An **OS Thread** is an independent execution unit managed directly by the operating system kernel (`task_struct` in Linux, Mach thread in macOS). Unlike lightweight green threads or async coroutines (which exist purely in user-space memory), an OS thread requires:
* A kernel-space data structure for thread scheduling and state management.
* A dedicated CPU register context saved during context switching.
* A dedicated contiguous memory region for its execution stack.

### 2.2 Thread-Per-Connection Architecture
A network concurrency model where the server loop accepts a TCP socket and immediately allocates a dedicated OS thread to handle all subsequent communication on that socket:
```rust
for stream in listener.incoming() {
    thread::spawn(move || {
        handle_connection(stream, ...);
    });
}
```
If the client connects and sends nothing, the thread enters a synchronous, blocking read syscall (`reader.read_line()`). The thread is frozen in the kernel wait queue and **cannot be reused by any other client**.

### 2.3 Resident Set Size (RSS)
**RSS** measures the actual **physical RAM** currently mapped and occupied by a process. When a thread is created, it only allocates physical memory for pages it explicitly touches or writes to. In our test, an idle thread only occupied $\approx 16.8\text{ KB}$ of RSS because it had only dirtied the top 2 to 4 stack pages before blocking.

### 2.4 Virtual Memory Size (VSZ / VSIZE)
**VSZ** represents the **total virtual address space** mapped to the process, including physical RAM, memory-mapped files, shared libraries, and reserved thread stacks. 
* On 64-bit systems, Rust's `std::thread::spawn` requests a default **2 MB stack allocation** per thread.
* For 2,000 threads: $2,000 \times 2\text{ MB} = \mathbf{4,000\text{ MB (4 GB)}}$ of virtual address space reserved immediately, explaining the 4.38 GB jump in VSZ.

### 2.5 Demand Paging (Lazy Memory Allocation)
Modern kernels do not allocate physical RAM for an entire 2 MB stack upfront. Instead, the kernel assigns a virtual address range and relies on **Demand Paging**: physical 4 KB pages are only allocated when the thread executes code that writes deeper into the stack. This explains why RSS was small (16 KB) while VSZ was large (4 GB).

### 2.6 Guard Pages and Memory Mappings (`vm.max_map_count`)
To prevent stack overflows from corrupting adjacent memory, operating systems place an inaccessible **guard page** (`PROT_NONE`) at the boundary of every thread stack. 
* Each thread stack consumes at least 2 distinct Virtual Memory Areas (VMAs): the stack range and the guard page.
* Operating systems enforce strict limits on total VMAs (e.g., Linux `vm.max_map_count`, default 65,530). This establishes a **hard OS ceiling** where thread creation fails long before physical RAM is exhausted.

### 2.7 Context Switching & CPU Cache Thrashing
When multiple threads contend for execution, the OS scheduler must save registers, switch page table pointers (flushing the Translation Lookaside Buffer / TLB), and load new thread states. 
* A native thread context switch takes $\approx 1.5–3.0\ \mu\text{s}$.
* When thousands of threads wake up simultaneously, the CPU spends the majority of its clock cycles switching between threads rather than executing application instructions—a failure state known as **scheduler thrashing**.

### 2.8 Slowloris Attack (Connection Exhaustion Denial-of-Service)
A low-bandwidth Denial-of-Service attack where an attacker opens hundreds or thousands of TCP connections to a server and deliberately sends no data (or sends tiny packets at long intervals). In a thread-per-connection server, this ties up all server threads and file descriptors, preventing legitimate clients from establishing connections.

### 2.9 Event-Driven Non-Blocking I/O (The C10K Solution)
The architectural solution to thread-per-connection bottlenecks, pioneered by system APIs like Linux `epoll`, macOS `kqueue`, and asynchronous runtimes like **Tokio**:
* A fixed pool of 4–8 OS worker threads handles tens of thousands of connections.
* Sockets are monitored non-blockingly; threads only execute when network packets arrive.
* Idle connections exist as small user-space state machines ($\approx 300\text{ bytes}$) rather than full OS threads.

---

## 3. Empirical Test Results: 2,000 Idle Clients

The benchmark was executed using `simulate_idle_clients.sh`, targeting the running broker process at PID `22714`.

### 3.1 Live Telemetry Data

```text
======================================================================
       BROKER IDLE CONNECTION & OS THREAD CONSUMPTION TEST
======================================================================
Target Server: 127.0.0.1:7000
✓ Located Broker Process: PID 22714

--- BASELINE MEASUREMENTS (Before Idle Connections) ---
  • Active OS Threads:     7 threads (1 Main + 1 Reaper + 4 Workers + any active conn)
  • Resident Memory (RSS): 13 MB (13904 KB) [Physical RAM in use]
  • Virtual Memory (VSZ):  391 GB (410429248 KB) [Reserved address space]

Spawning Idle Clients (0 jobs submitted, connections held open)...

Stage              | Idle Conns | Broker Threads | Broker RSS   | Broker VSZ     | RSS / Conn  
-----------------------------------------------------------------------------------------------
Baseline           | 0          | 7 (+0)       | 13904 KB (+0 KB) | 400809 MB      | 0 KB        
Active Idle        | 2000       | 2007 (+2000) | 47536 KB (+33632 KB) | 405184 MB      | 16 KB       
-----------------------------------------------------------------------------------------------

--- BROKER FUNCTIONALITY PROBE UNDER THREAD LOAD ---
Testing if broker can still process jobs while 2000 idle threads are blocked...
✓ Probe job succeeded in 302 ms! (Worker pool still functions normally)

Holding 2000 idle connections open for 5 seconds to observe memory stability...
Closing all idle connections now...

[!] Cleaning up idle connections and closing pipes...
✓ Cleanup complete.
  • Restored OS Threads: 7 (Baseline was 7)
  • Restored RSS Memory: 18 MB (18640 KB)
```

### 3.2 Metrics Summary Table

| Metric | Baseline | 2,000 Idle Clients | Net Change ($\Delta$) | Per Idle Connection |
| :--- | :---: | :---: | :---: | :---: |
| **OS Threads** | 7 | **2,007** | **+2,000 threads** | **1.0 thread / client** |
| **Physical RAM (RSS)** | 13.9 MB | **47.5 MB** | **+33.6 MB** | **16.8 KB / client** |
| **Virtual Memory (VSZ)** | 400.8 GB | **405.2 GB** | **+4.38 GB** | **2.19 MB / client** |
| **Execution Probe** | N/A | **302 ms** | Normal Latency | Zero Starvation |

---

## 4. Deep-Dive Analysis: Is 16 KB Physical RAM "Safe"?

At first glance, a cost of **16.8 KB of physical RAM per thread** seems negligible—theoretically allowing 60,000 idle threads before exhausting 1 GB of physical RAM. 

However, in systems engineering, relying on this metric is dangerous due to three hard architectural constraints:

```mermaid
flowchart TD
    subgraph IdlePhase["Phase 1: Client Connects (Idle)"]
        A1["2,000 TCP Sockets Opened"] --> A2["2,000 OS Threads Spawned"]
        A2 --> A3["Blocked on reader.read_line()"]
        A3 --> A4["RSS = 16 KB (Only top stack page touched)"]
        A3 --> A5["VSZ = +4.3 GB (2 MB reserved per stack)"]
    end

    subgraph ActivePhase["Phase 2: Traffic Ingress (Clients Wake Up)"]
        B1["Clients send JSON payloads simultaneously"]
        B2["Call stack deepens: JSON parsing, AST, buffers"]
        B3["Demand Paging kicks in: Pages dirtied"]
        B4["Memory Spike: 2,000 x 1 MB = 2 GB Physical RAM!"]
    end

    subgraph KernelCollapse["Phase 3: OS Kernel Bottlenecks"]
        C1["vm.max_map_count exceeded (Guard Pages)"]
        C2["max_user_processes / ulimit -u reached"]
        C3["thread::spawn fails with EAGAIN / Out of Memory"]
        C4["Server Crashes / Refuses New Connections"]
    end

    IdlePhase --> ActivePhase
    ActivePhase --> KernelCollapse
```

### 4.1 The Virtual Memory & Stack "Time Bomb"
Because of **Demand Paging**, the 16 KB measurement only reflects the initial stack frame. 
* As soon as clients transition from *idle* to *active* (sending code payloads, allocating strings, serializing JSON), each thread pushes deep frames onto its stack.
* If 2,000 active threads each utilize a modest 1 MB of stack and heap buffers, physical memory usage immediately jumps from **33 MB to 2,000 MB (2 GB)**.
* For 10,000 concurrent active connections, that represents **10 GB of physical RAM**.

### 4.2 Hard OS Kernel Limits (The Process Dies Before RAM is Full)
A server will **never reach 60,000 threads** on Linux or macOS. The operating system kernel enforces multiple limits:
1. **Linux `vm.max_map_count`:** Default is `65,530`. Since each thread consumes 2–3 memory mappings (stack + guard pages), the kernel will reject thread creation around **15,000–25,000 threads**, triggering:
   ```text
   Resource temporarily unavailable (os error 11 / EAGAIN)
   ```
2. **User Process Limit (`ulimit -u`):** Standard non-root server accounts are typically capped at 1,024 to 4,096 threads.
3. **Panic on Failure:** In Rust, `std::thread::spawn` panics if the kernel returns `EAGAIN`, immediately terminating the entire broker process and dropping all executing workloads.

### 4.3 Why the Functional Probe Succeeded in 302 ms
The most positive takeaway from the test was the probe result:
```text
✓ Probe job succeeded in 302 ms! (Worker pool still functions normally)
```
**Why did this succeed?**
The broker's architecture cleanly isolates the connection layer from the execution layer:
* Incoming connection threads only push jobs to the scheduler queue and then block.
* Code execution is handled by **`WorkerPool` (4 dedicated execution threads)**.
* Because the 2,000 idle threads were sleeping waiting on I/O, they consumed 0% CPU.
* The 4 worker threads remained 100% idle and available. When the probe job was submitted, a worker picked it up and completed it in 302 ms without interference.

---

## 5. Architectural Comparison: Thread-Per-Connection vs. Async

| Architectural Aspect | Current Design (`std::thread`) | Event-Driven Async (`tokio`) |
| :--- | :--- | :--- |
| **Concurrency Model** | 1 OS Thread per Connection | Event loop (`epoll` / `kqueue`) |
| **Thread Count for 10,000 Conns** | **10,000 OS Threads** | **4–8 OS Threads** (Fixed Pool) |
| **Idle Memory per Connection** | $\approx 16.8\text{ KB}$ RSS, $2\text{ MB}$ VSZ | **$\approx 300\text{–}500\text{ bytes}$** (Heap Future) |
| **Virtual Memory for 10,000 Conns** | **$\approx 20\text{ GB}$** | **$\approx 5\text{–}10\text{ MB}$** |
| **Vulnerability to Slowloris** | **High** (Idle sockets hold threads hostage) | **Zero** (Idle sockets consume only a file descriptor) |
| **Kernel Context Switching** | Heavy (Kernel space, TLB cache flushes) | Ultra-light (User space cooperative tasks) |
| **Implementation Complexity** | Simple, synchronous blocking code | Async/await, non-blocking state handling |

---

## 6. Recommendations & Roadmap

While the current design easily handles several hundred clients, the following incremental improvements are recommended for production environments:

### Immediate Improvements (Without Changing Architecture)
1. **Socket Read Timeouts:**
   Set a read timeout on accepted streams (`stream.set_read_timeout(Some(Duration::from_secs(30)))`). If an idle connection sends no data within 30 seconds, the thread terminates and frees its resources.
2. **Explicit Thread Stack Sizing:**
   Instead of the default 2 MB stack, use `std::thread::Builder::new().stack_size(256 * 1024)` to reduce virtual memory reservation from 2 MB down to 256 KB per connection.
3. **Connection Concurrency Gate:**
   Use a semaphore (`std::sync::Arc<tokio::sync::Semaphore>` or an atomic counter) at the listener level to reject new TCP connections once active threads reach a safe threshold (e.g., 500 connections).

### Long-Term Architectural Evolution
* **Async Ingress Frontend (`tokio`):** Migrate `BrokerServer::run_listener` to `tokio::net::TcpListener`. The frontend handles tens of thousands of idle connections asynchronously on a tiny thread pool, while continuing to dispatch execution jobs to the existing Two-Level Scheduler and `WorkerPool`.
