#!/usr/bin/env bash
set -euo pipefail

echo "=== [1/4] Installing Redis, Netcat, and System Tools ==="
sudo apt-get update
sudo apt-get install -y --no-install-recommends redis-server netcat-openbsd

echo "=== [2/4] Starting Redis L2 Cache Server ==="
sudo service redis-server start || redis-server --daemonize yes

echo "=== [3/4] Building Pre-Baked Sandbox Image (distjob-python:latest) ==="
docker build -t distjob-python:latest .
docker pull alpine:latest

echo "=== [4/4] Compiling Broker in Release Mode (--release) ==="
cargo build --release

echo ""
echo "=================================================================="
echo "  Codespace Ready! Quick Start Commands:"
echo "  1. Start Broker (Release + Async + 8 Workers + 8 Warm Pool):"
echo "     cargo run --release -- --async -w 8 -p 8"
echo ""
echo "  2. Run Stress Test (in a second terminal):"
echo "     ./stress_test.sh --preset standard"
echo ""
echo "  3. Launch Testing Workbench GUI (auto-forwarded on port 8080):"
echo "     python3 testing_gui/app.py"
echo "=================================================================="
