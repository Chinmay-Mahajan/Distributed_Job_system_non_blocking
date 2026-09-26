#!/usr/bin/env python3
"""
Example interactive client for Sandboxed Job Execution Broker (v2).
Supports submitting code, receiving an immediate opaque job_id,
and polling asynchronously for the execution result.
"""

import json
import socket
import sys
import time

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 7000

def run_sample():
    print(f"Connecting to broker at {SERVER_HOST}:{SERVER_PORT}...")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((SERVER_HOST, SERVER_PORT))
    except Exception as e:
        print(f"Could not connect to broker: {e}")
        print("Please start the broker first: cargo run -- --local")
        sys.exit(1)

    reader = s.makefile("r")

    # 1. Submit a Python job via JSON
    job_payload = {
        "action": "SUBMIT",
        "client_id": "alice",
        "language": "python",
        "code": "print('Hello from isolated sandbox!')\nprint('2 ** 32 =', 2**32)",
        "stdin": "",
        "timeout_secs": 5,
    }

    print("\n[Client] Submitting job:")
    print(json.dumps(job_payload, indent=2))

    s.sendall((json.dumps(job_payload) + "\n").encode())
    raw_response = reader.readline()
    submit_resp = json.loads(raw_response)

    job_id = submit_resp.get("job_id")
    print(f"\n[Client] Received immediate job_id: {job_id}")
    print("[Client] Connection thread was NOT blocked on execution.")

    # 2. Asynchronously poll for the result
    print("\n[Client] Polling for result...")
    start_poll = time.time()
    while True:
        poll_payload = {"action": "POLL", "job_id": job_id}
        s.sendall((json.dumps(poll_payload) + "\n").encode())
        res_line = reader.readline()
        res = json.loads(res_line)

        status = res.get("status")
        print(f"  Status: {status} (elapsed: {time.time() - start_poll:.2f}s)")

        if status in ("Done", "Failed", "TimedOut"):
            print("\n[Client] Execution finished:")
            print(f"  Final Status: {status}")
            print(f"  Exit Code:    {res.get('exit_code')}")
            print(f"  Stdout:\n{res.get('stdout', '').strip()}")
            if res.get("stderr"):
                print(f"  Stderr:\n{res.get('stderr', '').strip()}")
            if res.get("execution_duration_ms"):
                print(f"  Duration:     {res.get('execution_duration_ms')}ms")
            break

        time.sleep(0.1)

    s.close()

if __name__ == "__main__":
    run_sample()
