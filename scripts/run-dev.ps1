$env:LIFECYCLE_GRACE_TIMEOUT_SECONDS = '30'
uv run python -m app.server `
  --host 127.0.0.1 `
  --port 8000
