#!/usr/bin/env python3
"""
Local Testing & Diagnostic GUI Server for Distributed Job Broker v2.
Zero external dependencies (uses Python standard library http.server + socket).

Usage:
    python3 testing_gui/app.py --port 8080 --broker-host 127.0.0.1 --broker-port 7000
"""

import argparse
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional

BASE_DIR = Path(__file__).resolve().parent


class ManagedConnection:
    """Represents a persistent TCP connection to the Rust broker for interactive GUI testing."""

    def __init__(self, conn_id: str, host: str, port: int):
        self.conn_id = conn_id
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
        self.file = None
        self.local_addr: str = ""
        self.default_client_id: str = ""
        self.secret_key: Optional[str] = None
        self.connected_at: float = 0.0
        self.last_active: float = 0.0
        self.alive: bool = False
        self.last_disconnect_reason: Optional[str] = None
        self.lock = threading.Lock()
        self.connect()

    def connect(self):
        with self.lock:
            self._close_locked()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10.0)
            s.connect((self.host, self.port))
            ip, port = s.getsockname()
            self.sock = s
            self.file = s.makefile("r", encoding="utf-8", errors="replace")
            self.local_addr = f"{ip}:{port}"
            self.default_client_id = f"client-{ip}:{port}"
            self.connected_at = time.time()
            self.last_active = self.connected_at
            self.alive = True
            self.last_disconnect_reason = None

    def send_line(self, line: str, timeout: float = 30.0) -> dict:
        with self.lock:
            if not self.alive or self.sock is None or self.file is None:
                return {
                    "ok": False,
                    "error": self.last_disconnect_reason or "Socket is disconnected",
                    "alive": False,
                }
            try:
                self.sock.settimeout(timeout)
                t0 = time.time()
                payload = line.rstrip("\r\n") + "\n"
                payload_bytes = payload.encode("utf-8")
                self.sock.sendall(payload_bytes)
                resp_line = self.file.readline()
                elapsed_ms = round((time.time() - t0) * 1000, 1)
                if not resp_line:
                    self.alive = False
                    self.last_disconnect_reason = "Server closed connection (EOF)"
                    self._close_locked()
                    return {
                        "ok": False,
                        "error": f"Server closed connection (EOF) after {elapsed_ms} ms",
                        "alive": False,
                        "elapsed_ms": elapsed_ms,
                        "bytes_sent": len(payload_bytes),
                    }
                self.last_active = time.time()
                stripped = resp_line.strip()
                parsed_json = None
                try:
                    parsed_json = json.loads(stripped)
                except Exception:
                    pass

                # If this was a successful REGISTER response, record the secret_key on this connection
                if isinstance(parsed_json, dict) and parsed_json.get("status") == "ok" and "secret_key" in parsed_json:
                    self.secret_key = parsed_json["secret_key"]
                elif stripped.startswith("+sk_"):
                    self.secret_key = stripped[1:].strip()

                # Check if server sent a disconnect notice
                if "disconnecting" in stripped.lower():
                    try:
                        self.sock.settimeout(0.2)
                        eof_check = self.file.readline()
                        if not eof_check:
                            self.alive = False
                            self.last_disconnect_reason = stripped
                            self._close_locked()
                    except Exception:
                        pass

                # Truncate huge responses so we never freeze the browser DOM
                display_raw = stripped
                if len(display_raw) > 2000:
                    display_raw = display_raw[:2000] + f"\n... [truncated, total response length: {len(stripped):,} chars]"

                if isinstance(parsed_json, dict):
                    # Truncate huge echoed fields inside JobResult (like job.stdin or stdout) for GUI display
                    for field in ("stdout", "stderr"):
                        if isinstance(parsed_json.get(field), str) and len(parsed_json[field]) > 1000:
                            orig_len = len(parsed_json[field])
                            parsed_json[field] = parsed_json[field][:1000] + f"... [+{orig_len - 1000:,} chars]"
                    if isinstance(parsed_json.get("job"), dict):
                        for jfield in ("stdin", "code"):
                            val = parsed_json["job"].get(jfield)
                            if isinstance(val, str) and len(val) > 500:
                                orig_len = len(val)
                                parsed_json["job"][jfield] = val[:500] + f"... [+{orig_len - 500:,} chars]"

                return {
                    "ok": True,
                    "raw": display_raw,
                    "json": parsed_json,
                    "alive": self.alive,
                    "elapsed_ms": elapsed_ms,
                    "bytes_sent": len(payload_bytes),
                }
            except Exception as e:
                self.alive = False
                self.last_disconnect_reason = str(e)
                self._close_locked()
                return {
                    "ok": False,
                    "error": str(e),
                    "alive": False,
                }

    def check_server_kick(self) -> dict:
        """Non-blocking peek to see if the server proactively kicked an idle/expired connection."""
        with self.lock:
            if not self.alive or self.sock is None or self.file is None:
                return {"alive": False, "reason": self.last_disconnect_reason}
            try:
                self.sock.setblocking(False)
                try:
                    data = self.sock.recv(4096, socket.MSG_PEEK)
                    if data == b"":
                        self.alive = False
                        self.last_disconnect_reason = "Server closed socket (EOF)"
                        self._close_locked()
                    elif b"disconnecting" in data.lower() or b"-err" in data.lower():
                        self.sock.setblocking(True)
                        self.sock.settimeout(0.5)
                        line = self.file.readline().strip()
                        self.alive = False
                        self.last_disconnect_reason = line
                        self._close_locked()
                except BlockingIOError:
                    # No bytes waiting -> connection is still open and idle
                    pass
                finally:
                    if self.sock is not None:
                        self.sock.setblocking(True)
                        self.sock.settimeout(10.0)
            except Exception as e:
                self.alive = False
                self.last_disconnect_reason = str(e)
                self._close_locked()

            return {"alive": self.alive, "reason": self.last_disconnect_reason}

    def close(self):
        with self.lock:
            self.alive = False
            self.last_disconnect_reason = "Closed by user"
            self._close_locked()

    def _close_locked(self):
        try:
            if self.file:
                self.file.close()
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.file = None
        self.sock = None

    def to_dict(self) -> dict:
        self.check_server_kick()
        now = time.time()
        return {
            "conn_id": self.conn_id,
            "local_addr": self.local_addr,
            "default_client_id": self.default_client_id,
            "secret_key": self.secret_key,
            "alive": self.alive,
            "idle_secs": round(now - self.last_active, 1) if self.alive else None,
            "uptime_secs": round(now - self.connected_at, 1) if self.alive else None,
            "last_disconnect_reason": self.last_disconnect_reason,
        }


class BrokerBridge:
    def __init__(self, broker_host: str, broker_port: int):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.connections: Dict[str, ManagedConnection] = {}
        self.conn_counter = 0
        self.lock = threading.Lock()

    def create_connection(self, label: Optional[str] = None) -> dict:
        with self.lock:
            self.conn_counter += 1
            conn_id = label or f"conn-{self.conn_counter}"
            conn = ManagedConnection(conn_id, self.broker_host, self.broker_port)
            self.connections[conn_id] = conn
            return conn.to_dict()

    def list_connections(self) -> list:
        with self.lock:
            conns = list(self.connections.values())
        return [c.to_dict() for c in conns]

    def close_connection(self, conn_id: str) -> bool:
        with self.lock:
            conn = self.connections.pop(conn_id, None)
        if conn:
            conn.close()
            return True
        return False

    def get_connection(self, conn_id: str) -> Optional[ManagedConnection]:
        with self.lock:
            return self.connections.get(conn_id)

    def fetch_stats(self) -> dict:
        """Opens a short-lived socket to query STATS and PING from the broker."""
        try:
            with socket.create_connection((self.broker_host, self.broker_port), timeout=2.0) as s:
                f = s.makefile("r", encoding="utf-8", errors="replace")
                s.sendall(b'{"action": "STATS"}\n')
                line = f.readline().strip()
                data = json.loads(line)
                return {"reachable": True, "stats": data}
        except Exception as e:
            return {"reachable": False, "error": str(e)}


BRIDGE: Optional[BrokerBridge] = None


class TestingGUIHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Keep terminal output clean
        pass

    def _send_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Browser aborted or timed out waiting for HTTP response
            pass

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        return json.loads(raw) if raw else {}

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            index_path = BASE_DIR / "index.html"
            content = index_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        if self.path == "/api/state":
            stats_info = BRIDGE.fetch_stats()
            conns = BRIDGE.list_connections()
            self._send_json({
                "broker": f"{BRIDGE.broker_host}:{BRIDGE.broker_port}",
                "health": stats_info,
                "connections": conns,
            })
            return

        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self):
        try:
            body = self._read_body()
        except Exception as e:
            self._send_json({"ok": False, "error": f"Invalid JSON body: {e}"}, status=400)
            return

        if self.path == "/api/connect":
            try:
                label = body.get("label")
                info = BRIDGE.create_connection(label=label)
                self._send_json({"ok": True, "connection": info})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, status=500)
            return

        if self.path == "/api/disconnect":
            conn_id = body.get("conn_id", "")
            removed = BRIDGE.close_connection(conn_id)
            self._send_json({"ok": removed})
            return

        if self.path == "/api/send":
            conn_id = body.get("conn_id", "")
            conn = BRIDGE.get_connection(conn_id)
            if not conn:
                self._send_json({"ok": False, "error": f"Connection '{conn_id}' not found"}, status=404)
                return

            raw_line = body.get("raw_line")
            inflate_kb = int(body.get("inflate_kb") or 0)

            if raw_line is None:
                payload_obj = body.get("payload", {})
                if inflate_kb > 0 and payload_obj.get("action") == "SUBMIT":
                    # Generate large stdin directly in Python memory without freezing browser
                    base_stdin = payload_obj.get("stdin") or ""
                    payload_obj["stdin"] = base_stdin + ("A" * (inflate_kb * 1024))
                raw_line = json.dumps(payload_obj)
            elif inflate_kb > 0:
                raw_line = raw_line + ("A" * (inflate_kb * 1024))

            res = conn.send_line(raw_line)
            sent_preview = raw_line
            if len(sent_preview) > 500:
                sent_preview = sent_preview[:500] + f"... [truncated in UI log, total sent: {len(raw_line):,} bytes]"

            self._send_json({
                "ok": res.get("ok", False),
                "sent": sent_preview,
                "response": res,
                "connection": conn.to_dict(),
            })
            return

        self._send_json({"error": "Unknown endpoint"}, status=404)


def main():
    parser = argparse.ArgumentParser(description="Testing GUI for Distributed Job Broker v2")
    parser.add_argument("--port", type=int, default=8080, help="HTTP port for the GUI (default: 8080)")
    parser.add_argument("--broker-host", type=str, default="127.0.0.1", help="Broker host (default: 127.0.0.1)")
    parser.add_argument("--broker-port", type=int, default=7000, help="Broker TCP port (default: 7000)")
    args = parser.parse_args()

    global BRIDGE
    BRIDGE = BrokerBridge(args.broker_host, args.broker_port)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), TestingGUIHandler)
    print(f"============================================================")
    print(f" Distributed Job Broker v2 — Testing & Security Lab GUI")
    print(f" GUI URL     : http://127.0.0.1:{args.port}")
    print(f" Target Node : {args.broker_host}:{args.broker_port}")
    print(f"============================================================")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down GUI server...")
        server.server_close()


if __name__ == "__main__":
    main()
