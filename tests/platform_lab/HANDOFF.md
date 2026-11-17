# Image supplier handoff — external build required

Status: handoff specification only. No Gateway image, TAR, image ID or runtime
certification result has been produced. The existing Dockerfile, Compose file,
README and Windows-tested harness are preserved without edits.

## Exact source context

Use an independently verified clone on the authorized external builder. The
accepted Git revision is `4ff43a92d8b71640a13c3f531599b12597a5d5e6`.
Never send the complete local working directory. Prepare a NEW empty context:

```sh
REV=4ff43a92d8b71640a13c3f531599b12597a5d5e6
git cat-file -e "$REV^{commit}"
mkdir <new-empty-context>
git archive "$REV" app tests pyproject.toml uv.lock alembic.ini |
  tar -x -C <new-empty-context>
```

Transfer exactly these FIVE additional payload files by an approved channel:

| Relative path | Raw-file SHA-256 |
| --- | --- |
| tests/integration/m4_platform_shutdown_worker.py | a5558f815c5928660bbed14ad51f0ea721d4b0c281277aa1d6beb3ec759015a1 |
| tests/integration/test_m4_platform_shutdown.py | eaa82a77ff716eea4a3ef632f2f08939845c01ff9e2d228072029e5aaa1d3d9a |
| tests/platform_lab/Dockerfile | cc136206039b49862275ce0883132806779af5f454f870508c21f18089700c2e |
| tests/platform_lab/compose.yaml | e8b54a5ea6b17a7e8b17c11f28d0f4f2087b1219b5651f7b5d3665e2ac534bb0 |
| tests/platform_lab/README.md | c1b02904eeb26491d6620569ab9517809f5bd39e5eeb5292b5427ab4109370a8 |

These hashes cover current raw bytes, including line endings. Transfer bytes
unchanged. The five files are untracked and absent from a plain HEAD archive.
This handoff is instructions, not required image payload. Add no access logs,
.env, credentials, platform evidence, Windows JSON, .git or local environments.

Before overlay, compare every exported tracked file with `git show REV:path`
byte-for-byte. After overlay, verify the five hashes above. Produce a complete
context inventory (relative paths and SHA-256). Reject extra files outside the
archive, five-file overlay and declared bundle. Save the inventory outside the
context to avoid self-reference.

Create bundle/source.sha256 from every exported app/ and tests/ file plus
pyproject.toml, uv.lock and alembic.ini, AFTER overlay and BEFORE creating bundle/.
Use relative POSIX paths, with the standard `sha256sum` two-space separator.
The Dockerfile checks it from /opt/gateway. Supply both the manifest and its
independently authenticated digest; a self-authored manifest alone proves no
provenance. Build source must match the commit plus only the inventoried overlay.

## Dependency bundle and external validation

Follow README's bundle layout and offline Dockerfile. The external builder must
already hold the approved digest-pinned Python 3.12 Linux/amd64 base and verified
uv/Docker binaries. Select compatible wheels from the UNCHANGED uv.lock; include
runtime, dev and transitive dependencies. Record source URLs, versions, wheel
tags and lockfile SHA-256 matches. Missing wheels are a STOP, not permission to
resolve newer packages or invoke unpinned builds. No package download is needed
inside the resulting image at runtime.

Build with README's `--network=none --pull=false` procedure. Check all build logs;
record uv version, Python patch version, base digest and Docker CLI provenance.
Then run on the external builder (not performed here):

```sh
docker image inspect gateway-m4-cert:py312 > gateway-m4-cert.image.json
docker run --rm --pull=never --network none --entrypoint python gateway-m4-cert:py312 -c "import platform,redis,fastapi,uvicorn,pytest,pytest_asyncio,psycopg,app.server; assert platform.machine()=='x86_64'; assert platform.python_version_tuple()[:2]==('3','12'); assert redis.__version__=='8.1.0'; print(platform.python_version(),redis.__version__,fastapi.__version__,uvicorn.__version__)"
docker run --rm --pull=never --network none --entrypoint uv gateway-m4-cert:py312 pip freeze --python /opt/cert-venv/bin/python > installed.txt
docker run --rm --pull=never --network none --entrypoint python gateway-m4-cert:py312 -B -m pytest -q -p no:cacheprovider tests/unit/test_m4_p1c_f4b_lifecycle.py tests/unit/test_m4_p1c_f2_supervisor.py --basetemp /tmp/m4-supplier-tests
```

Compare installed.txt against the applicable Python-3.12 marker-selected export
of uv.lock; preserve results and reject missing, extra or substituted packages.
These tests are nondestructive in-memory checks, not platform acceptance.
Verify inspect reports Os=linux and Architecture=amd64. Record image ID and any
RepoDigests; a locally built image may have no registry RepoDigest. Do not invent
one. Image ID, archive checksum and registry digest are distinct identifiers.

## Offline delivery and recipient verification

Supply: image TAR, authenticated TAR SHA-256, image inspect JSON/ID, base identity,
context/source/harness manifests, dependency/artifact manifest, installed.txt,
import/test results and complete build provenance. Use README's docker save.
No TAR or final image checksum can be supplied until the image actually exists.

Recipient compares TAR hash before load, then compares imported image ID with
the authenticated supplier record. Inspect Linux/amd64, revision label and
entrypoint; run the network-disabled Python/import/version check above. Verify
source.sha256 inside the image from /opt/gateway and compare its digest with the
approved source manifest. Do not trust the revision label alone. Verify existing
Redis/Postgres image IDs against README before starting the unique Compose lab.

## Runner topology and certification boundary

The Compose Gateway has NO Docker socket and needs none. It connects directly
to redis:6379 and postgres:5432 on its own internal Compose network. Its exec
entrypoint is the actual app.server interpreter as PID 1; deliver container
SIGTERM by exact inspected container ID. Container PID-1 behavior requires its
own observation. Standard logs/exit status do not alone prove Supervisor cleanup.

The existing P01–P14 parent requires Docker daemon access, host-published
loopback ports and the ability to signal its own Python subprocess. It does NOT
use Compose DNS addresses. The child runs the observer worker then app.server
in the same process; parent sends SIGTERM to that owned child PID. Docker CLI
presence is not daemon access.

Preferred unchanged-harness arrangement: an external dedicated native Linux
host runner with the exact locked environment, Docker CLI and its own dedicated
daemon. Its Python process and dependency-published ports share the host network
namespace. Run pytest from the inventoried exported context, opt in and capture
evidence exactly as README specifies. Alternatively use README's dedicated
host-network test container; never mount the socket into the Compose Gateway.
This is NOT a claim that Docker Desktop host networking is available or enabled.

Current Compose supports PID-1 smoke, not the unchanged parent harness. If full
execution must stay on Docker Desktop without a verified host-network runner,
the smallest proposed test-only adjustment is: let the fixture explicitly accept
lab-only dependency endpoints and teach Process to use those endpoints instead
of hardcoded loopback. The fixture must retain exclusive ownership and cleanup
checks; compose DNS can then be used by an isolated runner. This adjustment is
NOT implemented or certified here and requires independent source review.

Do not translate subprocess SIGTERM evidence into PID-1 evidence, or synthetic
upstream instrumentation into full proxy/streaming acceptance. After image supply,
complete health/startup and observed shutdown smoke before the 18 P01–P14 variants.
Capture deadlines, renewal/release, closure confirmation, residual obligations,
signal origin, PID tree and exit status before process death. Stop on a production
defect. Source remains immutable; separate repair authorization would be needed.
