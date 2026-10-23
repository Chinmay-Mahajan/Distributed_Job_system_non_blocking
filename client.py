#!/usr/bin/env python3
"""
CLI Client for Sandboxed Job Execution Broker (v2).
Supports:
  - Automatic REGISTER handshake to obtain a server-issued secret_key (or --secret-key reuse)
  - Submitting any file from disk to execute remotely
  - Auto-detecting language from file extension (.py, .sh, .js)
  - Optional stdin input
  - Immediate non-blocking submission + asynchronous authenticated polling
"""

import argparse
import json
import os
import socket
import sys
import time

EXTENSION_TO_LANG = {
    ".py": "python",
    ".sh": "sh",
    ".bash": "bash",
    ".js": "node",
    ".mjs": "node",
}


def detect_language(filepath: str) -> str:
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    return EXTENSION_TO_LANG.get(ext, "python")


def run_client(
    filepath: str = None,
    language: str = None,
    secret_key: str = None,
    timeout_secs: int = 10,
    stdin_data: str = None,
    server_host: str = "127.0.0.1",
    server_port: int = 7000,
):
    # If a file is specified, read code from disk
    if filepath:
        if not os.path.exists(filepath):
            print(f"Error: File '{filepath}' not found.")
            sys.exit(1)
        with open(filepath, "r", encoding="utf-8") as f:
            code = f.read()
        lang = language or detect_language(filepath)
        print(f"[Client] Loaded '{filepath}' ({len(code)} bytes, language: {lang})")
    else:
        # Default interactive sample if no file is provided
        code = "print('Hello from isolated sandbox!')\nprint('2 ** 32 =', 2**32)"
        lang = language or "python"
        print("[Client] No file provided, running default sample code.")

    print(f"[Client] Connecting to broker at {server_host}:{server_port}...")
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((server_host, server_port))
    except Exception as e:
        print(f"Error: Could not connect to broker at {server_host}:{server_port}: {e}")
        print("Make sure the broker is running: cargo run -- --async --local 127.0.0.1:7000")
        sys.exit(1)

    reader = s.makefile("r", encoding="utf-8")

    # 0. Register with the broker if no secret_key was supplied
    if not secret_key:
        reg_payload = {"action": "REGISTER"}
        s.sendall((json.dumps(reg_payload) + "\n").encode("utf-8"))
        reg_line = reader.readline()
        try:
            reg_resp = json.loads(reg_line)
            secret_key = reg_resp.get("secret_key")
        except Exception as e:
            print(f"Error parsing REGISTER response: {reg_line} ({e})")
            s.close()
            sys.exit(1)

        if not secret_key:
            print(f"[Client] Registration failed: {reg_resp}")
            s.close()
            sys.exit(1)
        print(f"[Client] Registered with broker! Issued secret_key: {secret_key[:12]}...")

    # 1. Submit job via JSON using secret_key
    job_payload = {
        "action": "SUBMIT",
        "secret_key": secret_key,
        "language": lang,
        "code": code,
        "stdin": stdin_data or "",
        "timeout_secs": timeout_secs,
    }

    s.sendall((json.dumps(job_payload) + "\n").encode("utf-8"))
    raw_response = reader.readline()

    try:
        submit_resp = json.loads(raw_response)
    except Exception as e:
        print(f"Server response was not JSON: {raw_response} ({e})")
        s.close()
        sys.exit(1)

    job_id = submit_resp.get("job_id")
    if not job_id:
        print(f"[Client] Submission failed: {submit_resp}")
        s.close()
        sys.exit(1)

    print(f"[Client] Job accepted! Assigned Job ID: {job_id}")
    print("[Client] Connection was NOT blocked. Polling for results...")

    # 2. Asynchronously poll for the result with secret_key
    while True:
        poll_payload = {"action": "POLL", "secret_key": secret_key, "job_id": job_id}
        s.sendall((json.dumps(poll_payload) + "\n").encode("utf-8"))
        res_line = reader.readline()
        res = json.loads(res_line)

        if res.get("status") == "error":
            print(f"[Client] Poll error: {res.get('error')}")
            break

        status = res.get("status")

        if status in ("Done", "Failed", "TimedOut"):
            duration = res.get("execution_duration_ms")
            print(f"\n{'='*50}")
            print(f"Status:   {status}")
            print(f"Owner ID: {res.get('client_id', '')[:16]}... (SHA-256)")
            print(f"Exit:     {res.get('exit_code')}")
            if duration is not None:
                print(f"Duration: {duration} ms")
            print(f"{'='*50}")

            stdout = res.get("stdout", "")
            if stdout:
                print(f"--- STDOUT ---\n{stdout.rstrip()}")

            stderr = res.get("stderr", "")
            if stderr:
                print(f"--- STDERR ---\n{stderr.rstrip()}")

            if res.get("error_message"):
                print(f"--- ERROR ---\n{res.get('error_message')}")
            break

        time.sleep(0.1)

    s.close()


def main():
    parser = argparse.ArgumentParser(
        description="Submit a code file to the Sandboxed Job Execution Broker."
    )
    parser.add_argument(
        "file",
        nargs="?",
        help="Path to the code file to execute (e.g. script.py, test.sh)",
    )
    parser.add_argument(
        "--lang",
        help="Language (python, sh, node). Auto-detected from file extension if omitted.",
    )
    parser.add_argument(
        "--secret-key",
        "--key",
        dest="secret_key",
        default=None,
        help="Optional existing server-issued secret_key (registers a new key if omitted)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Execution timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "--stdin",
        help="Optional input string to pass to the program's standard input",
    )
    parser.add_argument(
        "--stdin-file",
        help="Optional path to a file containing standard input for the program",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Broker server host (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7000,
        help="Broker server port (default: 7000)",
    )

    args = parser.parse_args()

    stdin_content = None
    if args.stdin_file:
        with open(args.stdin_file, "r", encoding="utf-8") as f:
            stdin_content = f.read()
    elif args.stdin:
        stdin_content = args.stdin

    run_client(
        filepath=args.file,
        language=args.lang,
        secret_key=args.secret_key,
        timeout_secs=args.timeout,
        stdin_data=stdin_content,
        server_host=args.host,
        server_port=args.port,
    )


if __name__ == "__main__":
    main()
