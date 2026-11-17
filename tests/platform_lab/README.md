# Offline M4-P1C certification lab (prepared, NOT executed)

Accepted revision: `4ff43a92d8b71640a13c3f531599b12597a5d5e6`.
The required `gateway-m4-cert:py312` image is absent. Stop after preparing these
files. No readiness, SIGTERM or P01–P14 result is implied.

## External supply contract

Use an authorized external Linux/amd64 builder and an approved Python 3.12 base
pinned by digest. Record its Python patch version, OS, image digest and provenance.
Do not run downloads/builds on the certification workstation under this task.

Create a clean context with `git archive` of the accepted revision (verify the
revision first). Add ONLY the two reviewed untracked files
`tests/integration/m4_platform_shutdown_worker.py` and
`tests/integration/test_m4_platform_shutdown.py`, and this lab directory. Never
copy the worktree wholesale: exclude .env, access logs, .git, Windows environments,
secrets and unrelated files. The original worker SHA256 is
`a5558f815c5928660bbed14ad51f0ea721d4b0c281277aa1d6beb3ec759015a1`;
the parent-test SHA256 is
`eaa82a77ff716eea4a3ef632f2f08939845c01ff9e2d228072029e5aaa1d3d9a`.

Supply `bundle/uv` (approved Linux x86_64 uv 0.12.10), `bundle/docker`
(approved Linux/amd64 Docker CLI compatible with Engine 29.9.2), and
`bundle/wheels/` with ALL Python-3.12/Linux-compatible wheels selected from the
unchanged uv.lock, including the dev group. Record names, versions, source URLs,
hashes, platform tags and tool provenance in `bundle/manifest.json`; verify wheel
hashes against the lock. Include SHA256SUMS for both tools and every wheel.
If any pinned wheel is missing, STOP: do not substitute a version or silently
build an sdist. A source build requires separately supplied pinned build
dependencies and reproducible evidence. Do not include Windows binary wheels.

Create `bundle/source.sha256` from the accepted exported app/, tracked tests/,
pyproject.toml, uv.lock, alembic.ini and the two added harness files. Independently
compare the export against the accepted Git tree; a revision label alone is not
source verification. This manifest is checked from /opt/gateway during build.

The Dockerfile exports hashed requirements from the frozen lock (all groups),
then installs only from the supplied wheelhouse with hash enforcement. Required
runtime groups are Alembic, cryptography, FastAPI, HTTPX, psycopg[binary], Pydantic,
pydantic-settings, Redis, SQLAlchemy, uuid-utils and Uvicorn. Dev/test tooling is
mypy, pytest, pytest-asyncio, pytest-cov, PyYAML and Ruff, including all locked
transitives. redis-py MUST be 8.1.0. The lock, not pyproject lower bounds, is the
version authority. Supply `uv pip freeze` and import-check output with the image.

On the external builder, with every artifact already verified locally:

```sh
docker build --network=none --pull=false --platform linux/amd64 \
  --build-arg PYTHON_BASE=python@sha256:<approved-python312-digest> \
  -f tests/platform_lab/Dockerfile -t gateway-m4-cert:py312 .
docker image inspect gateway-m4-cert:py312 > gateway-m4-cert.image.json
docker save -o gateway-m4-cert-py312.tar gateway-m4-cert:py312
sha256sum gateway-m4-cert-py312.tar > gateway-m4-cert-py312.tar.sha256
```

Supply tar, authenticated checksum, image metadata, artifact/source manifests,
build log and version/import evidence. Verify transfer checksum BEFORE load.

## Workstation import and isolated smoke (future execution)

PowerShell, after approved supply:

```powershell
Get-FileHash -Algorithm SHA256 <supplied-tar>
# Compare with the separately authenticated supplier checksum, then:
docker load -i <supplied-tar>
docker image inspect gateway-m4-cert:py312
docker run --rm --pull=never --network none --entrypoint python gateway-m4-cert:py312 -c "import sys,redis,fastapi,uvicorn,pytest,app.server; print(sys.version); assert sys.version_info[:2]==(3,12); assert redis.__version__=='8.1.0'"
$env:M4_CERT_PROJECT = 'm4-cert-' + [guid]::NewGuid().ToString('N')
docker compose -f tests/platform_lab/compose.yaml config
docker compose -f tests/platform_lab/compose.yaml up -d --no-build --pull never
docker compose -f tests/platform_lab/compose.yaml ps
docker compose -f tests/platform_lab/compose.yaml port gateway 8000
```

Do not start if existing redis/postgres tag IDs differ from the verified supply
record. At preparation: Redis image ID
`sha256:ff02b58f971e7d7d156a1267e283fcbbeee91773b6aa36c49dac28ecfe28eadf`,
Postgres ID
`sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b`.
Both are Linux/amd64. Confirm Redis reports 7.4.11 and PostgreSQL reports major 16
before use. No dependency port is published; only Gateway gets an ephemeral
host-loopback port. The unique project owns its internal network and volumes.

Actual lifespan constructs the SQLAlchemy engine; /health/ready runs SELECT 1.
The existing platform harness uses a fresh empty PostgreSQL database and synthetic
request configuration; it does not initialize schema or exercise DB-backed Admin
operations. No migration is required for that same smoke scope. If a broader
probe needs tables, stop and scope initialization solely to the disposable DB;
never migrate shared infrastructure. Initial DB creation uses only fixture
credentials above. No real credentials belong in the image.

Gateway's exec-form entrypoint makes the actual app.server interpreter PID 1.
After recording readiness, inspect the exact container ID, PID and process tree,
then send `docker kill --signal=TERM <verified-gateway-container-id>`. Capture
logs, timestamps, inspect exit state and owned children before cleanup. Stock
logs alone do NOT expose all Supervisor/disposal obligations: use the existing
observer worker for certification evidence. Do not call the smoke certified
solely from an exit code. G=30 matches scripts/run-dev.sh and the existing harness;
the Docker watchdog is not a new application deadline.

## Existing P01–P14 harness continuation (conditional, NOT ready)

Reuse the two original harness files unchanged. They launch the real app.server
via runpy in a Python subprocess and send genuine SIGTERM to that owned PID on
Linux. The worker observes production lifespan/Supervisor and uses synthetic
upstream hooks; this is distinct from a PID-1 smoke and full proxy certification.
P01–P14 include all 18 existing parametrized variants, not just one clean exit.

The parent fixture needs Docker CLI/daemon access and creates its OWN uniquely
labeled Redis/Postgres containers with host-loopback published ports. Compose
services above do not replace that fixture. A normal bridge-network runner cannot
reach those ports via its own loopback. On a dedicated native Linux Docker host,
a host-network test container with the dedicated daemon socket can run:

```sh
docker run --rm --pull=never --network host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v <absolute-test-owned-evidence-dir>:/evidence \
  -e M4_PLATFORM_CERTIFICATION=1 -e M4_PLATFORM_EVIDENCE_DIR=/evidence \
  --entrypoint python gateway-m4-cert:py312 \
  -B -m pytest -q -p no:cacheprovider tests/integration/test_m4_platform_shutdown.py \
  --basetemp /tmp/m4-platform
```

Daemon socket access grants broad daemon authority: use only a dedicated lab
daemon with the verified images already loaded. Do not expose a shared daemon
without separate authorization. Docker Desktop host-network behavior is NOT
assumed or enabled here. Verify that execution topology before using this path;
otherwise a separately scoped harness adapter is required. Do not silently
replace localhost, simulate SIGTERM or claim Desktop full-harness readiness.
Record PID-1 and subprocess signal evidence separately.

After captures, inspect exact Compose project labels and resources, then remove
ONLY this lab's project using the same M4_CERT_PROJECT and Compose file:
`docker compose -f tests/platform_lab/compose.yaml down --volumes --remove-orphans`.
This command is for the unique disposable lab ONLY, never the shared project.
The unchanged parent fixture independently ownership-checks its own cleanup.
No resource has been created by preparing this specification.
