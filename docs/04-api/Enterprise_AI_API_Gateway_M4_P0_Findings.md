# M4-P0 — lifecycle design and validated-peer feasibility

Baseline: `feature/m3-runtime`, `0425ead4717edc35854b482e5a48b811ed1d9baa`, initially clean. R1–R27 remain authoritative; this note introduces no new semantics. No production, Admin, OpenAPI or migration changes.

## Request-local ownership (design, not implementation)

One request execution owner holds canonical request ID/context, immutable R18 execution snapshot, acquired provenance handles, renewal task, upstream operation, downstream delivery, terminal arbitration and cleanup. Its sole receive pump is the **only caller of ASGI receive**: it forwards bounded body messages to the body guard and signals disconnect to the execution owner. Once body reception completes, that same pump continues disconnect observation; never start a second receive consumer. Receive failures and disconnects enter R9 arbitration, not an invented upstream outcome. Body retention is bounded under R6/R15; the pump must not create an unbounded message queue.

Stages: request ID → authentication/tenant binding → coherent service/route authorization/configuration → cumulative rate checks → concurrency acquire and immediate renewal → bounded body reception → deterministic local preparation (including destination validation/credential selection) → circuit admission → one upstream operation → applicable definitive circuit completion → terminal cleanup/observers. No circuit probe is acquired for a local body/preparation rejection.

The execution owner starts R8 renewal immediately on acquisition, owns cancellation of request-local upstream/delivery tasks under R9, and never closes the shared client to cancel one request. It stops and awaits renewal before the single provenance-aware release attempt. Definitive upstream outcome and terminal downstream outcome are distinct: successful upstream completion cannot be rewritten by a subsequent delivery failure. First terminal decision wins; post-commit failure terminates delivery without another envelope. Raw response streaming, bounded backpressure and body-forbidden response handling remain R16/R17 implementation work.

R25 insertion is a bounded terminal-only Route-attributed operation; R26 primary terminal logging and R27 counters/timers each have one request-local owner. Freeze duration/outcome once; observers cannot change the selected business response or produce duplicate completion. Keep secrets, query/body/header values, destination topology and uncontrolled exception text out of operational observations. Cleanup belongs in the owner's terminal path, not independent middleware finalizers competing to release handles.

## Executable transport seam

Locked/installed HTTPX 0.28.1 and httpcore 1.0.9 support a custom `AsyncBaseTransport`, one `AsyncConnectionPool`, `create_connection`, `AsyncHTTPConnection` and `AsyncNetworkBackend`. The isolated test adapter preserves logical URL origin/Host; its backend dials only an approved numeric IP and checks the actual peer. httpcore independently supplies the logical hostname to TLS SNI and certificate verification. No rewrite of the URL authority to an IP is needed.

Each request validates its complete synthetic resolver answer set before pool entry. Each connection checks its actual peer against that request's approved set before dispatch; an incompatible connection is closed and rejected with `ConnectionNotAvailable` before request bytes, allowing pool selection of a new connection. This is not an HTTP retry. Connection-local dispatch serialization and task-local immutable dial plans avoid sharing request DNS plans across concurrent dispatches. Logical origins remain segregated. One pool retains the R5 100/20 bounds, with retries disabled. Trust context is process startup-owned and shared, not per-service mutable state.

The tests use real loopback TCP/TLS, a synthetic CA/server certificate, real SNI/Host/wire-target observation, and server EOF signals to verify closure. A resolver fixture supplies complete deterministic answer sets; a socket resolver guard rejects any logical hostname lookup. Mixed allowed/protected answers deny before socket creation. DNS-answer changes cannot reuse an earlier peer outside the new approved set. Environment proxies point to a controlled listener that receives no connection; `trust_env=False`, redirect non-following and ignored certificate environment discovery are explicit. Positive custom-CA verification and negative hostname/untrusted-CA verification use actual TLS handshakes. Raw encoded path/query are compared to the received HTTP request line.

This is feasibility evidence, not a production R11 inventory implementation. The synthetic protected set stands in for deployment inventory; production must canonicalize/validate the entire actual resolver result, reject protected IP+port pairs, and fail closed on invalid/missing inventory. No production DNS, secrets or endpoints are used. HTTP/1.1 only; no claim about HTTP/2 multiplexing, full proxy runtime, ASGI backpressure, production exception mapping or complete R17 protocol compliance. Custom transport integration must preserve R10 phase timeouts and explicit HTTPX exception translation; the proof deliberately exposes httpcore exceptions. Locked-stack upgrades require these tests again.

## R18 extension (design only)

Existing seams: `app/main.py` lifespan owns shared client and managed M3 tasks; `app/redis/protection_facade.py` owns provenance-aware protection calls; `app/control_plane/config_subscriber.py` owns the only observed-version store, dirty/pending state and certificates; `config_reconciler.py` owns fenced PostgreSQL passes. Existing async app-lifespan/ASGITransport tests exercise these on one event loop, but do not establish socket streaming/backpressure.

The smallest later registry extension is one named required tenant consumer with SERVICE/ROUTE event interests and an independent `reconcile_current_state(tenant_id)` callback, rather than fabricated resource UUIDs or separate body/timeout consumers. Registration participates in existing pending membership, invalidation epochs and stable-pass snapshots. Candidate service/route/active-credential metadata must be published only through successful start/end-fenced certification. Add an atomic capture operation under the same tenant coherence fence returning immutable values plus their certificate/generation, or fail closed. Do not check `is_tenant_reconciled()` separately from mutable reads. Late registration remains pending. No second numeric version store, DB-per-request configuration bypass or Redis projection. Capture retains exact credential identity, never decrypted secret material.

## Separate Admin/compatibility gates (not fixed)

- `app/control_plane/normal_api_registry.py` create/patch Route: integrate the required ACTIVE-set RouteOverlapValidator before config-version advancement.
- Route Admin pattern validation: R22 duplicate-slash and R23 NFC acceptance need separately scoped validation/compatibility tests; runtime must reject unusable legacy patterns without poisoning unrelated routes.
- Service credential Admin validation: enforce R3 reserved STATIC_HEADER names in a separately scoped change; runtime must still fail closed.
- `app/core/errors.py` generic HTTP exception mapping: public R19 405 must preserve 405/Allow rather than fall through generic 400 handling. Public runtime integration remains future work.

Next gates: production-quality validated-peer adapter/inventory with cancellation and pooled-request race tests; R18 required-consumer/capture implementation with real DB fences; then request-owner vertical slices. P0 does not implement those components or declare the Normal API engine complete.

## P0A evidence closure

| Claim | Evidence category | Boundary |
| --- | --- | --- |
| Approved numeric TCP peer; logical Host; socket EOF after close | Real TCP | Controlled IPv4 loopback listeners, not deployment inventory |
| Wrong connected peer rejected before any HTTP bytes | Real TCP plus controlled dial injection | Plan approves 127.0.0.2; test seam actually dials 127.0.0.1; actual socket remote address is read unchanged |
| Mismatch stream closed and absent from reusable pool | Real TCP | EOF observed, including incomplete-read partial-byte inspection; later valid request uses a fresh connection |
| Original mismatch survives cleanup error | Controlled injection over real TCP | Wrapper closes the real socket, then raises; safe PeerMismatch remains the reported failure |
| Handler failure and cancellation during listener teardown | Controlled injection over real TCP | Event-gated handler failure/block; teardown cancels handlers before waiting for server closure; no surviving tracked tasks |
| Obsolete pooled peer rejected; logical origins separated | Real TCP | Sequential requests only; no concurrent pooled-connection rebinding proof |
| Logical SNI, trusted custom CA, wrong-host/untrusted-CA rejection | Real TLS | Synthetic local CA/certificates; no production secrets |
| Proxy environment cannot redirect; redirects not followed | Real TCP/TLS | Existing explicit trust_env=False/custom transport, empty NO_PROXY; not a deployment-wide proxy matrix |
| Exact accepted path and raw query serialization | Real wire bytes | Literal plus, %20, duplicates, blanks, bare names, percent-escape case, %252F, UTF-8 and ordering compared against a fixed byte literal |
| Security rejection independent of Python assert | Executed explicit runtime condition | PeerMismatch replaces assert; optimization-mode mismatch tests exercise the same rejection |
| Ingress validation, complete R11 inventory, R18 publication/capture | Deferred production integration | No implementation or acceptance claim |

The query/path test compares both HTTPX's constructed raw target and the server's received request line to the original fixed byte literal, not a parsed query dictionary or a possibly normalized derived expectation. Canonical path examples contain encoded UTF-8, spaces, a percent sign and explicit segment boundaries. The double-encoded query value remains `%252F`; this does not implement or certify R1/R21 ASGI ingress canonicalization.

Negative expectations are tripwires: mismatch must raise before a response, wrong-host/untrusted-CA must raise verification errors, and a normalized wire target must fail the fixed-byte comparison. Cleanup-error injection does not simulate a socket that refuses all closure: it verifies failure preservation after actual closure. A listener cancellation probe exposed a fixture teardown-order deadlock; cancellation/await of handlers now precedes the bounded server-close wait. This is isolated fixture repair, not a production change.

Verification: focused P0/P0A suite **10 passed**; combined P0/P0A, existing R5 client/lifespan and M3 consumer/reconciliation/provenance/recovery regressions **99 passed**. Python `-O` mismatch selection **2 passed** (expected pytest optimization warning). Separate process-local mutations, never written to source, demonstrated the negative controls: disabled peer validation **1 expected failure**; disabled TLS verification **2 expected failures**; changed query `+` to `%20` **2 expected failures**. Each control required pytest exit 1 and was discarded with its process; these are not failures of the unmodified suite. Ruff, changed-source formatting and external-cache compile checks passed.

Explicit limitations retained: **no production R11 enforcement; no complete R18 consumer; no ASGI ingress canonicalization implementation; no ASGI streaming/backpressure proof; no concurrent pooled-connection rebinding proof; no M4 runtime acceptance.**
