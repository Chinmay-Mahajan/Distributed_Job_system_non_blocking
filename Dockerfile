# Sandboxed Python Execution Image with Pre-Baked Libraries
# Built for Sandboxed Job Execution Broker (v2)
FROM python:3.11-slim

# Prevent interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=0

# Install minimal system runtime dependencies (OpenMP for NumPy/SciPy math routines)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pre-install popular scientific, numerical, and algorithmic libraries.
# Under '--network none', user code cannot install packages at runtime;
# pre-baking them ensures instant, air-gapped availability.
RUN pip install --no-cache-dir \
    numpy \
    pandas \
    scipy \
    sympy \
    requests

# Pre-compile all installed packages to .pyc bytecode.
# Eliminates runtime parsing/compilation delay, dropping import times by 3x-5x.
RUN python3 -m compileall /usr/local/lib/python3.11/

# Unprivileged sandbox execution user
RUN useradd -m -u 1000 -s /bin/bash sandboxuser

WORKDIR /tmp

# Default command keeps the container alive in the pre-warmed standby pool
CMD ["sleep", "infinity"]
