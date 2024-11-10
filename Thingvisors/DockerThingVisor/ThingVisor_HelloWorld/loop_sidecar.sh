#!/bin/sh

if [ "$CONTAINER_ROLE" = "init" ]; then
    echo "Running in init mode..."
else
    echo "Running in sidecar mode..."
    while true; do sleep 3600; done
fi
