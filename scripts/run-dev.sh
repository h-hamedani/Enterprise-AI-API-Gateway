#!/usr/bin/env sh
set -eu

export LIFECYCLE_GRACE_TIMEOUT_SECONDS=30
uv run python -m app.server \
  --host 0.0.0.0 \
  --port 8000
