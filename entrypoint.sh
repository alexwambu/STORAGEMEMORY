#!/bin/sh
set -e

echo "Starting Storage Server..."
uvicorn server:app --host 0.0.0.0 --port 8000
