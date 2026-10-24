# Testing & Security Lab GUI (`testing_gui/`)

A lightweight, zero-dependency local web console for testing and auditing the **Sandboxed Job Execution Broker (v2)**.

## Quick Start

1. **Start the Rust Broker** (in Terminal 1):
   ```bash
   # Using Local sandbox (no Docker needed) or Docker sandbox:
   cargo run --release -- --async --local 127.0.0.1:7000
   ```
   *(Tip: To test fast idle or key-expiry connection kicking in the GUI, run with `DISTJOB_L2_TTL_SECS=15` or `DISTJOB_IDLE_TIMEOUT_SECS=15`)*

2. **Start the Testing GUI Server** (in Terminal 2):
   ```bash
   python3 testing_gui/app.py --port 8080 --broker-host 127.0.0.1 --broker-port 7000
   ```

3. **Open in Browser**:
   Navigate to **[http://127.0.0.1:8080](http://127.0.0.1:8080)**

## What You Can Test
- **Panel 1 — Persistent TCP Connections (`peer_auth_map`)**:
  - Open multiple persistent TCP connections and see each socket's `client-<ip>:<port>` identifier.
  - Click **REGISTER** once on a socket to mint a `secret_key`, then click **REGISTER** a second time on the *same* socket to watch `peer_auth_map` reject it.
  - Leave a registered socket idle past `DISTJOB_IDLE_TIMEOUT_SECS` / `DISTJOB_L2_TTL_SECS` and watch its status automatically flip to `KICKED / CLOSED` with the server's disconnect reason.
- **Panel 2 — Interactive Command & Job Playground**:
  - Submit Python/Bash/JS jobs, pass `stdin`, poll results, edit the `secret_key` field to test forged keys, or paste another client's `job_id` to verify cross-tenant ownership rejection.
