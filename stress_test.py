#!/usr/bin/env python3
"""
Single-process asyncio load generator for the job broker (text protocol).

Why this exists: the bash generator forks nc/head/tr/perl/python3 for every
poll, which burns CPU on the same cores as the broker and Docker. This version
uses ONE process and ONE persistent TCP connection per simulated client, and
reports how much CPU the generator itself used so you can verify it is not
distorting the result.

Usage:
  python3 stress_async.py --host 127.0.0.1 --port 7000 --preset heavy
  python3 stress_async.py --preset heavy --json run1.json   # for repeat runs
"""
import argparse
import asyncio
import json
import math
import os
import random
import statistics
import time

# (client_id, tier, num_requests, submit_interval_s, complex_percent)
PRESETS = {
    "light": [
        ("client_spammer_1", "High-Freq", 12, 0.06, 40),
        ("client_spammer_2", "High-Freq", 10, 0.08, 50),
        ("client_moderate_1", "Med-Freq", 5, 0.35, 40),
        ("client_slow_1", "Low-Freq", 2, 1.00, 50),
    ],
    "standard": [
        ("client_spammer_1", "High-Freq", 24, 0.04, 50),
        ("client_spammer_2", "High-Freq", 20, 0.07, 50),
        ("client_moderate_1", "Med-Freq", 10, 0.30, 40),
        ("client_moderate_2", "Med-Freq", 8, 0.40, 40),
        ("client_slow_1", "Low-Freq", 4, 1.20, 50),
        ("client_slow_2", "Low-Freq", 3, 1.50, 50),
    ],
    "heavy": [
        ("client_spammer_1", "High-Freq", 45, 0.03, 50),
        ("client_spammer_2", "High-Freq", 40, 0.05, 50),
        ("client_moderate_1", "Med-Freq", 18, 0.25, 40),
        ("client_moderate_2", "Med-Freq", 15, 0.35, 40),
        ("client_slow_1", "Low-Freq", 6, 1.20, 50),
        ("client_slow_2", "Low-Freq", 4, 1.60, 50),
    ],
}
PRESETS["quick"] = PRESETS["light"]

SIMPLE = [
    ("Simple-Math", "python", "print(sum(range(500)))"),
    ("Simple-String", "python", "print('stress_test_traffic'[::-1]*2)"),
    ("Simple-Echo", "sh", "echo worker_job_done"),
    ("Simple-Sqrt", "python", "import math; print(math.isqrt(9876543210))"),
]
COMPLEX = [
    ("Complex-Fib", "python", "a,b=0,1; exec('for _ in range(3000): a,b=b,a+b'); print('fib_len:', len(str(b)))"),
    ("Complex-Primes", "python", "p=[x for x in range(2, 5000) if all(x%d!=0 for d in range(2, int(x**0.5)+1))]; print('primes:', len(p))"),
    ("Complex-Delay300", "python", "import time; time.sleep(0.3); sum(i*i for i in range(100000)); print('delay_300ms_ok')"),
    ("Complex-Delay600", "python", "import time; time.sleep(0.6); print('delay_600ms_ok')"),
    ("Complex-Matrix", "python", "a=[[1,2,3],[4,5,6],[7,8,9]]; print(sum(sum(r) for r in a)*1000)"),
]
TERMINAL = ("+Done", "+Failed", "+TimedOut")


def pick(i, complex_pct):
    use_complex = complex_pct > 0 and (i * 17) % 100 < complex_pct
    pool = COMPLEX if use_complex else SIMPLE
    return pool[i % len(pool)]


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[max(0, math.ceil(p * len(xs)) - 1)]


class Client:
    def __init__(self, spec, args, sink):
        self.name, self.tier, self.n, self.interval, self.cplx = spec
        self.interval = self.interval / args.speed
        self.args = args
        self.sink = sink
        self.io = asyncio.Lock()          # one request/response at a time per connection
        self.pace_lock = asyncio.Lock()   # keeps this client's polls under the broker quota
        self.next_poll = 0.0
        self.min_gap = 1.0 / args.max_poll_rate

    async def connect(self):
        self.r, self.w = await asyncio.open_connection(self.args.host, self.args.port)
        resp = await self.rpc("REGISTER")
        if not resp.startswith("+sk_"):
            raise RuntimeError(f"[{self.name}] REGISTER failed: {resp}")
        self.key = resp[1:]

    async def rpc(self, line):
        async with self.io:
            self.w.write((line + "\n").encode())
            await self.w.drain()
            data = await asyncio.wait_for(self.r.readline(), 30)
        if not data:
            raise ConnectionError("broker closed the connection")
        return data.decode().strip()

    async def pace(self):
        async with self.pace_lock:
            now = time.monotonic()
            if self.next_poll > now:
                await asyncio.sleep(self.next_poll - now)
                now = time.monotonic()
            self.next_poll = max(now, self.next_poll) + self.min_gap

    async def job(self, i):
        jtype, lang, code = pick(i, self.cplx)
        rec = dict(client=self.name, tier=self.tier, type=jtype, outcome="Unknown",
                   client_ms=None, server_ms=None, exec_ms=None, queue_ms=None, throttled=0)
        t0 = time.monotonic()
        try:
            resp = await self.rpc(f"SUBMIT {self.key} {lang} {code}")
            if not resp.startswith(":"):
                low = resp.lower()
                rec["outcome"] = "Rejected" if ("rate limit" in low or "queue" in low) else "SubmitError"
                rec["detail"] = resp[:120]
                return
            job_id = resp[1:]
            while True:
                if time.monotonic() - t0 > self.args.job_deadline:
                    rec["outcome"] = "GaveUp"
                    return
                await self.pace()
                resp = await self.rpc(f"POLL {self.key} {job_id}")
                head = resp.split(" ", 1)[0]
                if head in TERMINAL:
                    rec["client_ms"] = (time.monotonic() - t0) * 1000
                    rec["outcome"] = head[1:]
                    if " " in resp:
                        try:
                            d = json.loads(resp.split(" ", 1)[1])
                            c, f = d.get("created_at_ms"), d.get("completed_at_ms")
                            ex = d.get("execution_duration_ms") or 0
                            if c and f:
                                rec["server_ms"] = f - c
                                rec["exec_ms"] = ex
                                rec["queue_ms"] = max(0, (f - c) - ex)
                        except ValueError:
                            pass
                    return
                if "poll rate limit" in resp.lower():
                    rec["throttled"] += 1
                    await asyncio.sleep(0.25)
                    continue
                if resp.startswith("-ERR"):
                    rec["outcome"] = "PollError"
                    rec["detail"] = resp[:120]
                    return
                await asyncio.sleep(self.args.poll_interval * random.uniform(0.8, 1.2))
        except Exception as e:  # connection problems etc. are counted, not hidden
            rec["outcome"] = "Error"
            rec["detail"] = repr(e)[:120]
        finally:
            self.sink.append(rec)

    async def run(self):
        tasks = []
        for i in range(1, self.n + 1):
            tasks.append(asyncio.create_task(self.job(i)))
            await asyncio.sleep(self.interval)
        await asyncio.gather(*tasks)


def report(records, wall, cpu, args):
    tiers = ["High-Freq", "Med-Freq", "Low-Freq"]
    print("=" * 112)
    print(f"{'Tier':<10} {'Sent':>5} {'Rej':>5} {'Err':>5} {'Done':>5} | "
          f"{'srv p50':>8} {'srv p95':>8} {'srv p99':>8} | {'cli p50':>8} {'cli p95':>8} | {'queue avg':>9} {'throttled':>9}")
    print("-" * 112)
    out = {}
    for tier in tiers + ["ALL"]:
        rs = records if tier == "ALL" else [r for r in records if r["tier"] == tier]
        if not rs:
            continue
        done = [r for r in rs if r["outcome"] in ("Done", "Failed", "TimedOut")]
        rej = sum(r["outcome"] == "Rejected" for r in rs)
        err = len(rs) - len(done) - rej
        srv = [r["server_ms"] for r in done if r["server_ms"] is not None]
        cli = [r["client_ms"] for r in done if r["client_ms"] is not None]
        q = [r["queue_ms"] for r in done if r["queue_ms"] is not None]
        thr = sum(r["throttled"] for r in rs)
        print(f"{tier:<10} {len(rs):>5} {rej:>5} {err:>5} {len(done):>5} | "
              f"{pct(srv, .5):>8.0f} {pct(srv, .95):>8.0f} {pct(srv, .99):>8.0f} | "
              f"{pct(cli, .5):>8.0f} {pct(cli, .95):>8.0f} | "
              f"{(statistics.mean(q) if q else float('nan')):>9.0f} {thr:>9}")
        out[tier] = dict(sent=len(rs), rejected=rej, errors=err, done=len(done),
                         srv_p50=pct(srv, .5), srv_p95=pct(srv, .95), srv_p99=pct(srv, .99),
                         cli_p50=pct(cli, .5), cli_p95=pct(cli, .95), throttled=thr)
    print("-" * 112)
    done_all = out["ALL"]["done"]
    cores = os.cpu_count() or 1
    gen_cores = cpu / wall if wall else 0
    print(f"Wall time:            {wall:.2f}s")
    print(f"Goodput (completed):  {done_all / wall:.2f} jobs/s   (rejected jobs are NOT counted as work done)")
    print(f"Rejected / Errors:    {out['ALL']['rejected']} / {out['ALL']['errors']} of {out['ALL']['sent']} sent")
    print(f"Generator CPU:        {cpu:.2f}s = {gen_cores:.2f} cores avg ({100 * gen_cores / cores:.1f}% of this machine)")
    if gen_cores / cores > 0.10:
        print("  WARNING: generator used >10% of the machine; move it off the broker's host.")
    print("srv = server timestamps (created->completed), unaffected by client polling.")
    print("cli = client-observed (includes polling delay). Latency stats cover completed jobs only.")
    out["_meta"] = dict(preset=args.preset, wall_s=wall, goodput=done_all / wall,
                        generator_cores=gen_cores, machine_cores=cores)
    return out


async def main_async(args):
    sink = []
    if args.warmup > 0:  # fill the warm pool / caches, then discard
        warm_sink = []
        wc = Client(("warmup", "Warmup", args.warmup, 0.05, 0), args, warm_sink)
        await wc.connect()
        await wc.run()
        await asyncio.sleep(1.0)

    clients = [Client(s, args, sink) for s in PRESETS[args.preset]]
    await asyncio.gather(*(c.connect() for c in clients))
    cpu0, t0 = time.process_time(), time.monotonic()
    await asyncio.gather(*(c.run() for c in clients))
    wall, cpu = time.monotonic() - t0, time.process_time() - cpu0
    summary = report(sink, wall, cpu, args)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(dict(summary=summary, records=sink), f, indent=1)
        print(f"Saved {args.json}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=7000)
    p.add_argument("--preset", choices=sorted(PRESETS), default="standard")
    p.add_argument("--poll-interval", type=float, default=0.1, help="seconds between polls per job")
    p.add_argument("--max-poll-rate", type=float, default=20.0,
                   help="per-client polls/sec cap; keep below DISTJOB_MAX_POLLS_PER_SEC (25)")
    p.add_argument("--job-deadline", type=float, default=120.0, help="give up on a job after N seconds")
    p.add_argument("--warmup", type=int, default=8, help="throwaway jobs before measuring (0 to disable)")
    p.add_argument("--speed", type=float, default=1.0,
                   help="divide submit intervals by this (e.g. 4 = 4x offered load) to get past the preset's ceiling")
    p.add_argument("--json", help="write raw records + summary to this file")
    asyncio.run(main_async(p.parse_args()))


if __name__ == "__main__":
    main()