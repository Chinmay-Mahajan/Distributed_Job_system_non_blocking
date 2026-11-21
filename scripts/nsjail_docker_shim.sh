#!/bin/bash
# Forward nsjail execution into the running nsjail-dev container on macOS
exec docker exec -i nsjail-dev nsjail "$@"
