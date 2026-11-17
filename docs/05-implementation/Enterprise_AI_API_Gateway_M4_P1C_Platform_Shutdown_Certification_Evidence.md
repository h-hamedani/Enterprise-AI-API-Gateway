# M4-P1C Platform Shutdown Certification Evidence

Verdict: **M4-P1C PLATFORM SHUTDOWN INFRASTRUCTURE BLOCKED**.

Baseline: `feature/m3-runtime`, local/live origin `4ff43a92d8b71640a13c3f531599b12597a5d5e6`. Accepted production source and frozen contracts remain unchanged. This is operational acceptance evidence, not guarded TLS or overall M4 production approval. Windows console coverage below passed; Linux SIGTERM was not executed and is not certified.

## Interruption recovery

The two existing test files were read in full and preserved through localized harness corrections. Initial SHA-256:

- Worker: `500c56d9d2d0f30eee0cc6bb888b7cae3c79181231bee1b6e9ed148acf73b8d4`.
- Parent tests: `a0a433d13e6b6a77d484908fbf5c9cda530d02f7dee3e0a24927bd9f564cbd58`.

No orphan harness process or labeled container was found at recovery. Corrections were limited to the harness: canonical request IDs, identity-checked uncertain-creation cleanup, final/failure evidence persistence, interpreter-child ownership checks, real-clock normal cases, stronger case assertions and one native 30-second cutoff variant. No production repair was made.

## Actual process and signal boundary

The child executes `runpy.run_module("app.server", run_name="__main__")`, the actual managed entrypoint, with the original application lifespan and supervisor. A loopback-only test ASGI adapter supplies synthetic resolver/policy/upstream/circuit hooks to the accepted RequestLifecycle. Actual request body reception, downstream HTTP sockets, guarded concurrency facade, Redis semaphore/transport and managed Redis disposal are exercised. This is not production Normal API forwarding or streaming-backpressure acceptance.

The Windows venv launcher and actual interpreter may have distinct PIDs. Both are captured along with the pytest parent. CTRL_BREAK is delivered by a helper attaching only to the fixture's hidden console. No prior signal handler is replaced to manufacture exit code zero. The installed Uvicorn 0.52.4 restores handlers and re-raises captured signals after shutdown; these stock Windows runs exit **3**, after the captured final snapshot. This is distinct from a fixture kill: every final artifact has `forced=false`. P13 invalid configuration exits **1** before listening.

`G=30` is the existing explicit process-owned setting. P01/P02/P03/P12 and P03_cutoff_real use the production MonotonicClock. Boundary cases use a controlled monotonic clock and explicit acknowledgements; no new duration or extension is introduced. Parent waits are test watchdogs, not Gateway grace periods.

## P01–P14 matrix

All successful-request cases use `test_managed_process_shutdown[variant]`; P13 uses `test_invalid_managed_configuration_fails_before_listening[None/0/inf]`. All Windows signal cases use the actual entrypoint and console CTRL_BREAK. Every Linux row is **BLOCKED**, not PASS or inferred from Windows.

| Case | Windows result and evidence | Linux |
| --- | --- | --- |
| P01 | PASS: live/ready HTTP, no active request, original deadline, confirmed listener and both Redis-pool disposal | BLOCKED |
| P02 | PASS: synthetic definitive upstream result, active pre-delivery request, finish after OS signal, real HTTP 200/body, one original release | BLOCKED |
| P03 | PASS: active committed HTTP delivery with real guarded renewal; ordinary finish and separate native-clock cutoff variant | BLOCKED |
| P04 | PASS: connection preparation gate, zero renewal EVALSHA writes, no late release, physical obligation retained at cutoff | BLOCKED |
| P05 | PASS: separate drain/response gates after actual EVALSHA; uncertainty, phase, physical ownership and no late release | BLOCKED |
| P06 | PASS: actual post-write TCP abort, UNKNOWN history survives successful release and confirmed pool disposal; cleanup not falsely complete | BLOCKED |
| P07 | PASS: original-handle facade release pending at a test gate before backend dispatch; one invocation, unconfirmed result at cutoff, no fabricated Redis result | BLOCKED |
| P08 | PASS: delayed cancellation crosses request cutoff, real connection quarantined; original authority retained, guarded disposal failed/unconfirmed and Redis reported unclosed | BLOCKED |
| P09 | PASS: OS signal establishes deadline=30; exact controlled now=30; no Redis cleanup entry; listener/lifespan unconfirmed | BLOCKED |
| P10 | PASS: controlled now=31 after deadline=30; no fresh window or Redis cleanup entry; unfinished resources reported | BLOCKED |
| P11 | PASS: two concurrent managed request/cleanup scopes with deadlines 30/31 under process deadline 32; each release attempted once and remains unconfirmed | BLOCKED |
| P12 | PASS: two real console signals; deadline unchanged, one release and one Redis close, no duplicate cleanup | BLOCKED |
| P13 | PASS: missing/zero/infinite required grace rejected before ready/listening, actual process exit 1 | BLOCKED |
| P14 | PASS: final pre-exit snapshot retains historical/physical uncertainty, quarantined connection and unclosed Redis; OS exit does not certify cleanup | BLOCKED |

The native P03 cutoff run observed 30.0 seconds from shutdown activation to final snapshot, 19 guarded dispatches before the deadline, zero late release attempts and truthful unfinished request/lifespan/listener accounting. There was no fixture kill.

## Per-process record

Raw sanitized JSON artifacts are in `release-evidence/m4/p1c-platform/win32-*.json`. They retain signal time/type, PIDs, fixed deadline, renewal dispatch, issuing-scope match, original-handle release ordering, pool confirmation categories, quarantine counts, final obligations, exit code and forced-cleanup flag. No credentials, raw handles, URLs, UUID labels or raw exceptions are persisted.

| Variant | Parent / launcher / interpreter PID | Exit | Clock | Final accounting | Classification |
| --- | --- | --- | --- | --- | --- |
| P01 | 24532 / 24492 / 24804 | 3 | MonotonicClock | deadline=271422.171; residuals=0; unclosed=none | clean disposal before stock SIGBREAK exit |
| P02 | 24532 / 26192 / 23536 | 3 | MonotonicClock | deadline=271426.953; residuals=0; unclosed=none | clean disposal before stock SIGBREAK exit |
| P03 | 24532 / 20760 / 24708 | 3 | MonotonicClock | deadline=271433.343; residuals=0; unclosed=none | clean disposal before stock SIGBREAK exit |
| P03_cutoff_real | 24532 / 26344 / 1016 | 3 | MonotonicClock | deadline=271439.75; residuals=0; unclosed=application_lifespan,listener_0 | bounded exit; unresolved obligations reported |
| P04 | 24532 / 17988 / 15912 | 3 | Clock | deadline=31.666666666666668; residuals=0; unclosed=application_lifespan,listener_0 | bounded exit; unresolved obligations reported |
| P05_drain | 24532 / 16072 / 6584 | 3 | Clock | deadline=31.666666666666668; residuals=0; unclosed=application_lifespan,listener_0 | bounded exit; unresolved obligations reported |
| P05_response | 24532 / 13908 / 25376 | 3 | Clock | deadline=31.666666666666668; residuals=0; unclosed=application_lifespan,listener_0 | bounded exit; unresolved obligations reported |
| P06 | 24532 / 21892 / 24296 | 3 | Clock | deadline=31.666666666666668; residuals=1; unclosed=none | bounded exit; unresolved obligations reported |
| P07 | 24532 / 23812 / 4880 | 3 | Clock | deadline=30.0; residuals=0; unclosed=application_lifespan | bounded exit; unresolved obligations reported |
| P08 | 24532 / 26408 / 5092 | 3 | Clock | deadline=63.333333333333336; residuals=1; unclosed=redis | bounded exit; unresolved obligations reported |
| P09 | 24532 / 3176 / 18788 | 3 | Clock | deadline=30.0; residuals=0; unclosed=application_lifespan,listener_0 | bounded exit; unresolved obligations reported |
| P10 | 24532 / 24652 / 22044 | 3 | Clock | deadline=30.0; residuals=0; unclosed=application_lifespan,listener_0 | bounded exit; unresolved obligations reported |
| P11 | 24532 / 26148 / 16536 | 3 | Clock | deadline=32.0; residuals=2; unclosed=none | bounded exit; unresolved obligations reported |
| P12 | 24532 / 22236 / 24468 | 3 | MonotonicClock | deadline=271522.437; residuals=0; unclosed=none | clean disposal before stock SIGBREAK exit |
| P13-0 | 24532 / 13144 / 10584 | 1 | startup rejection | no ready/listener event | invalid configuration rejected before listening |
| P13-inf | 24532 / 1928 / 16856 | 1 | startup rejection | no ready/listener event | invalid configuration rejected before listening |
| P13-None | 24532 / 10912 / 9640 | 1 | startup rejection | no ready/listener event | invalid configuration rejected before listening |
| P14 | 24532 / 19132 / 2688 | 3 | Clock | deadline=63.333333333333336; residuals=1; unclosed=redis | bounded exit; unresolved obligations reported |

## Ownership, resource and evidence limits

All guarded invocations observed the correct request ContextVar scope. Release events asserted that the renewal worker and invocation were already stopped, and the issued handle matched the request's original handle. No fallback, retry policy, deadline, G or historical-retention policy was added. Physical writer confirmation and execution uncertainty remain separate. Fixtures never mark OS process exit as protocol confirmation.

Disposable PostgreSQL/Redis use unique names/labels, tmpfs storage and test-owned loopback ports. Redis INFO asserts 7.4.11 and redis-py asserts 8.1.0. Container teardown resolves exact names, verifies ownership labels and removes only inspected immutable IDs, including uncertain creation outcomes. Interpreter teardown checks PID, parent and command line before any watchdog termination; no termination by executable name is used. No final run required forced process cleanup. Shared Gateway services were not stopped or flushed.

Test-owned control IPC and the test route exist only in the isolated child. No production endpoint was added. Resolver/upstream/circuit outcomes are synthetic; P07's pending-release gate precedes Redis dispatch. Exact controlled-clock boundaries do not alone prove native timing; P03_cutoff_real supplies that independent 30-second native-clock evidence. Windows service-host/SCM behavior is not supported or certified by this console harness.

Historical-only residual scopes may still retain complete lifecycle graphs across completed requests. That accepted non-blocking operational debt is unchanged and requires future owner-approved disposition. No TTL or evidence deletion is introduced.

## Linux infrastructure block

Docker Linux Engine is available, but local Python images inspected were 3.11. The project requires >=3.12. One bounded pull of `python:3.12-slim` failed on Docker Hub request timeout; no repeated pull was assumed to succeed. Ubuntu WSL has Python 3.14.4, no Python 3.12 in checked runtime/cache locations, no pip/uv and none of the project Redis/Uvicorn/FastAPI/SQLAlchemy/pytest dependencies. No suitable approved project Linux runtime was available. No system packages or host configuration were installed/changed. Windows results do not certify Linux SIGTERM. Provide a prepared approved Linux runtime with pinned dependencies to close this gate.

## Verification

- Syntax/import collection: initial 17 variants collected; final harness has 18 variants.
- Prerequisite safe lifecycle/supervisor/lifespan suite: 179 passed.
- Final Windows platform harness: 18 passed in 129.51s (final harness).
- Existing F3 Windows process tests: 2 passed in 16.73s; their custom prior signal-handler behavior is not substituted for the stock-handler evidence above.
- Safe P1C/F4B/F4A/F7/M3 regressions: 511 passed in 16.10s.
- Ruff PASS; format check PASS (186 Python files); compileall PASS using external PYTHONPYCACHEPREFIX.
- Default opt-in safety: 18 skipped in 0.32s without creating infrastructure. An unnecessary elevated launch timed out in automatic approval before starting; the same safe opt-out check succeeded without escalation.
- Final UTF-8/no BOM/no NUL and explicit untracked whitespace checks: PASS for both harness files and this document. `git diff --check` PASS. All 27 accepted raw SHA-256 values and eight access-log hashes match. Index empty and no tracked changes.
- Final resource inventory: zero `m4.platform.owner` containers and zero interpreter processes matching this harness or the F3 child signature. Shared Gateway services were not stopped, recreated or flushed.
- Evidence consists of 18 sanitized JSON files under the ignored release-evidence directory, all matching the final worker hash and all recording `forced=false`. No Linux case is represented by a Windows artifact.

No staging, commit, push, P1D, guarded TLS or overall M4 acceptance is claimed. A Windows result is not an overall platform PASS while Linux remains blocked. Independent review of this test harness/evidence remains required.

## Final harness identity

- `tests/integration/m4_platform_shutdown_worker.py`: `a5558f815c5928660bbed14ad51f0ea721d4b0c281277aa1d6beb3ec759015a1`.
- `tests/integration/test_m4_platform_shutdown.py`: `eaa82a77ff716eea4a3ef632f2f08939845c01ff9e2d228072029e5aaa1d3d9a`.

The evidence document and both test files remain untracked. The 18 JSON artifacts are ignored by the repository release-evidence rules and are reported explicitly rather than omitted from the generated-file inventory. No stage, commit or push occurred.


## Linux runtime unblock attempt — 2026-11-17

Verdict remains **M4-P1C PLATFORM SHUTDOWN INFRASTRUCTURE BLOCKED**. No Linux server or SIGTERM certification scenario was executed. The preceding Windows records and artifacts are preserved; they are not Linux substitutes.

### Python and lock compatibility

`pyproject.toml` and `uv.lock` both specify **Python >=3.12**, not exactly Python 3.12. Ubuntu WSL provides CPython **3.14.4**, x86_64, ordinary GIL-enabled `cp314`, glibc **2.43**, OpenSSL **3.5.5**. It has `venv` but no `ensurepip`, pip, uv or installed gateway dependencies.

Static analysis of all 47 lock records applicable under Python resolution markers found compatible candidate wheel tags for this interpreter/platform. In particular: redis 8.1.0 and Uvicorn 0.52.4 have universal wheels; pydantic-core 2.46.5, psycopg-binary 3.3.5, uuid-utils 1.0.0 and greenlet 3.5.5 have applicable Linux cp314 wheels; cryptography 50.0.1 has applicable abi3 wheels. This is **static candidate compatibility**, not an installed, resolved or import-tested environment. Python 3.14 is not rejected merely for being newer, and compatibility is not inferred solely from the minimum marker.

### Bounded discovery/provisioning record

| Step | Result |
| --- | --- |
| A: existing Docker images | Available Python and prepared application images inspected contain Python 3.11, unsuitable for >=3.12 certification |
| B: WSL | Ubuntu Python 3.14.4 available; required project dependencies and package tooling absent |
| C: existing runtime/cache | No Python 3.12 found in checked system, uv-cache or isolated-runtime locations |
| D: pinned container attempt | One `docker pull python:3.12.14-slim-bookworm` attempt reached the explicit 60-second bound without a usable image; no indefinite retries |
| E: project-local WSL tooling | Official PyPI metadata for uv **0.12.10**, matching the existing Windows tooling version, was retrieved. Download of the Linux wheel did not complete before the 65-second outer bound; no executable was installed and no locked dependency installation occurred |
| Dependency connectivity probe | A uniquely labeled disposable Redis was published on the private Windows/WSL gateway address; the WSL TCP connection attempt timed out. Root cause (routing/firewall/readiness) was not established; no host-network or firewall change was made |

Expected official wheel metadata SHA-256 was `f7d6248ad9f2d282fea795f248da8fa666ab382df50d8385bd98e01049440a0c`. It is **not** a claim that the timed-out wheel download was received or verified. No Linux `uv sync --frozen`, dependency imports or Linux pytest execution occurred. No dependency pin or lockfile changed.

The Docker CLI is callable through existing Windows/WSL interoperability, but this does not supply a Linux application runtime or prove the dependency TCP path. No prepared Linux certification environment resulted. Further provisioning stopped at the explicit infrastructure block.

### Linux P01–P14 status

All use the existing `test_managed_process_shutdown[variant]`, except P13's `test_invalid_managed_configuration_fails_before_listening[None/0/inf]`. Each row is **BLOCKED before process launch**: no Linux managed server PID, delivered SIGTERM, Redis renewal/release trace, supervisor final snapshot or exit code exists.

| Case | Existing variant(s) | Linux result |
| --- | --- | --- |
| P01 | P01 | BLOCKED |
| P02 | P02 | BLOCKED |
| P03 | P03, P03_cutoff_real | BLOCKED |
| P04 | P04 | BLOCKED |
| P05 | P05_drain, P05_response | BLOCKED |
| P06 | P06 | BLOCKED |
| P07 | P07 | BLOCKED |
| P08 | P08 | BLOCKED |
| P09 | P09 | BLOCKED |
| P10 | P10 | BLOCKED |
| P11 | P11 | BLOCKED |
| P12 | P12 | BLOCKED |
| P13 | None, 0, inf | BLOCKED |
| P14 | P14 | BLOCKED |

### Exit semantics and scope preservation

Windows exit **3** is a nonzero native signal exit, not a normal zero exit and not, by itself, proof of clean shutdown. The actual entrypoint uses Uvicorn's `capture_signals`: it restores the original handlers and re-raises captured signals after server shutdown. The console harness does not replace that prior handler. Clean disposal is established separately from pre-exit listener/pool/request evidence; bounded unresolved cases are explicitly non-clean. `forced=false` alone does not establish either classification.

For Linux the existing test expectation permits a native subprocess return of `-SIGTERM` after stock signal replay, but that is only an expectation derived from source. **No actual Linux exit status was observed or certified.** No production exit behavior was changed to manufacture zero.

The worker and parent harness remain byte-identical to the Windows-tested versions. The 18 original Windows JSON artifacts remain intact. No Windows rerun was needed because no shared harness code changed. No Windows Service/SCM, guarded TLS, full proxy/backpressure or overall M4 acceptance is claimed. Historical-only residual graph retention and redis-py private-API sensitivity remain unchanged operational debt.

### Cleanup, verification and next prerequisite

The exact labeled Redis probe container was removed by inspected immutable ID. No matching Linux bootstrap process survived the timeout. Its only project-local setup directory (`tmp/m4-linux-cert-a115ad3d556344cf808c9992db1bfaac`) was empty and removed with non-recursive `rmdir` after verifying its exact parent/name. No unrelated process or shared Gateway service was terminated. No system packages, host networking or firewall configuration were changed.

Safe in-memory lifecycle/supervisor regressions: **177 passed in 4.64s**. Linux process tests remain unexecuted. Final checks passed: Ruff; format validation (186 files); compileall using external PYTHONPYCACHEPREFIX; UTF-8/no BOM/no NUL; tracked and untracked whitespace. All 27 accepted implementation hashes, both harness hashes and eight access-log hashes remain unchanged. Branch/local HEAD and live origin match `4ff43a92d8b71640a13c3f531599b12597a5d5e6`; index is empty and no tracked changes exist. Final Docker inspection found no containers with the platform-test ownership label. A prepared compatible Linux runtime with the unchanged locked dependencies and a verified isolated Redis/PostgreSQL connection path is required to proceed. Offline runtime/wheelhouse delivery or separately authorized infrastructure preparation can close that prerequisite; Windows evidence cannot.

## Local Docker build and PID-1 smoke — 2026-11-17

Local image sha256:48cc6e6d1f4b92d5d10daf877cddad71f31db572f0374268ad7afea9b2c46ffd verified (Linux/amd64, Python 3.12.16, 45 exact locked packages). Safe Linux tests: 177 passed. Real PID-1 SIGTERM smoke: PASS, exit 0, no residual/unclosed resources; legacy disposal completed. Guarded pool was not initialized in this smoke. P01–P14 remain unexecuted pending verified runner topology. Full commands, versions and raw evidence: `release-evidence/m4/local-build-0b9867c967124213ae3f193aecc60515/REPORT.md`. Prior Windows evidence remains unchanged. No platform certification or overall M4 PASS is claimed.

## Linux P01–P14 completed — 2026-11-17

**M4-P1C PLATFORM SHUTDOWN CERTIFICATION PASS — INDEPENDENT REVIEW READY**. This current result supersedes the historical Linux infrastructure-blocked entries above; their Windows records remain unchanged. Windows-owned dependency orchestration and a test-only endpoint provider allowed the unchanged worker/scenario assertions to run in the verified Linux image without Docker socket access. Readiness: 1 passed. Full Linux suite: **18 passed in 125.74s**, no skips; signal children exit -15, P13 exits 1, all forced=false. Native cutoff: 30.008 seconds, 18 renewal writes before cutoff and zero release attempts afterward. Uncertainty/quarantine cases truthfully retain residual/unclosed obligations. Both isolated runs were cleaned up. Five endpoint-adapter tests and 177 Linux regressions pass.

Full per-process matrix, commands, ownership, signal, Redis/Supervisor observations and limitations: `release-evidence/m4/linux-platform-321a710ffa2c44cebddc715221ece92a/REPORT.md`; raw Linux artifacts: `release-evidence/m4/linux-platform-321a710ffa2c44cebddc715221ece92a/full/linux-*.json`. Earlier PID-1 smoke remains distinct. No Windows Service/SCM, guarded TLS, proxy/backpressure or overall M4 acceptance is claimed. Independent review remains required.
