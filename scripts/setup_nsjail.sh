#!/bin/bash
set -e

# ==============================================================================
# setup_nsjail.sh - Automated Environment Setup for Nsjail Sandbox
#
# Works on both:
#   1. Native Linux (Direct host nsjail execution)
#   2. macOS (via privileged Docker dev-container shim)
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOTFS_DIR="$PROJECT_ROOT/rootfs"
IMAGE_NAME="distjob-python:latest"

echo "======================================================================"
echo "          Setting up Nsjail Sandbox Environment                      "
echo "======================================================================"

cd "$PROJECT_ROOT"

# 1. Verify Docker is available for extracting the rootfs
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker is required to prepare the rootfs image."
    exit 1
fi

# Ensure docker daemon is responding
if ! docker info &> /dev/null; then
    echo "[ERROR] Docker daemon is not running. Please start Docker first."
    exit 1
fi

# 2. Build or verify the base image
if ! docker image inspect "$IMAGE_NAME" &> /dev/null; then
    echo "[*] Base image '$IMAGE_NAME' not found. Building from Dockerfile..."
    docker build -t "$IMAGE_NAME" .
else
    echo "[✓] Found base image '$IMAGE_NAME'."
fi

# 3. Export rootfs if not already present
if [ -d "$ROOTFS_DIR/bin" ] && [ -d "$ROOTFS_DIR/usr" ]; then
    echo "[✓] Rootfs directory already exists at $ROOTFS_DIR."
else
    echo "[*] Exporting filesystem from '$IMAGE_NAME' to $ROOTFS_DIR..."
    mkdir -p "$ROOTFS_DIR"
    CONTAINER_ID=$(docker create "$IMAGE_NAME")
    docker export "$CONTAINER_ID" | tar -C "$ROOTFS_DIR" -xf -
    docker rm "$CONTAINER_ID" > /dev/null
    echo "[✓] Rootfs extracted successfully."
fi

# 4. OS-Specific Setup
OS_TYPE="$(uname -s)"
echo "[*] Detected Operating System: $OS_TYPE"

if [ "$OS_TYPE" = "Linux" ]; then
    if ! command -v nsjail &> /dev/null; then
        echo "[!] 'nsjail' command not found on host."
        echo "    Install on Debian/Ubuntu with:"
        echo "      sudo apt-get update && sudo apt-get install -y nsjail"
        echo "    Or build from source: https://github.com/google/nsjail"
    else
        echo "[✓] Native 'nsjail' found at: $(command -v nsjail)"
    fi

    echo ""
    echo "======================================================================"
    echo "  Setup Complete! To launch the broker with Nsjail on Linux:"
    echo "======================================================================"
    echo "  cargo run --release -- --nsjail --rootfs ./rootfs --auto --async"
    echo ""

elif [ "$OS_TYPE" = "Darwin" ]; then
    echo "[*] Configuring macOS nsjail bridge via Docker container 'nsjail-dev'..."

    # Ensure shim is executable
    chmod +x "$PROJECT_ROOT/scripts/nsjail_docker_shim.sh"

    # Check if nsjail-dev is already running
    if docker ps --format '{{.Names}}' | grep -q "^nsjail-dev$"; then
        echo "[✓] 'nsjail-dev' container is already running."
    else
        # Remove stopped container if exists
        docker rm -f nsjail-dev 2>/dev/null || true

        echo "[*] Starting privileged 'nsjail-dev' container..."
        docker run -d --privileged --name nsjail-dev \
            -v "$PROJECT_ROOT":/workspace \
            -w /workspace \
            ubuntu:latest bash -c "
                apt-get update -qq && \
                apt-get install -y -qq nsjail python3 > /dev/null && \
                echo 'nsjail container ready' && \
                sleep infinity
            "
        
        # Wait briefly for container setup
        echo "[*] Waiting for container packages to install..."
        for i in {1..30}; do
            if docker exec nsjail-dev which nsjail &> /dev/null; then
                break
            fi
            sleep 1
        done
        echo "[✓] Container 'nsjail-dev' initialized with nsjail."
    fi

    echo ""
    echo "======================================================================"
    echo "  Setup Complete! To launch the broker with Nsjail on macOS:"
    echo "======================================================================"
    echo "  cargo run --release -- --nsjail \\"
    echo "    --nsjail-bin ./scripts/nsjail_docker_shim.sh \\"
    echo "    --rootfs /workspace/rootfs \\"
    echo "    --auto --async"
    echo ""
fi

echo "To run the ultra-low-CPU stress benchmark with section-wise latency breakdown:"
echo "  python3 max_stress.py --clients 16 --duration 10 --workload mixed"
echo "======================================================================"
