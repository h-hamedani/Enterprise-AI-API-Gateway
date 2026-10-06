# M4-R10 — Normal API Upstream Timeout Policy Contract

Status: **FROZEN RUNTIME SEMANTICS; API/OPENAPI RECONCILIATION REQUIRED BEFORE M4 TIMEOUT IMPLEMENTATION.** Contract documentation only; no runtime, test, OpenAPI, or migration change is made here.

## 1. Authority and existing fields

The M4 Normal API Engine Execution Pack V1.1 fixes V1 defaults of 5 seconds connect, 5 seconds pool acquisition, 30 seconds write, 60 seconds read/response idle, and 120 seconds until response start/first downstream commitment. It requires healthy bounded generic response streams to continue beyond 120 seconds while read-idle does not expire, maps pre-commit Gateway timeouts to 504 `upstream_timeout`, and terminates a partially committed stream without a second error response. [M4-R2](Enterprise_AI_API_Gateway_M4-R2_Normal_API_Circuit_Outcome_Classification_Contract.md) fixes circuit classification of transport timeouts and local pool timeout; [M4-R5](Enterprise_AI_API_Gateway_M4-R5_Upstream_HTTP_Client_Pool_Contract.md) fixes one shared process-lifespan HTTPX client but explicitly defers pool-timeout behavior. [M4-R7A](Enterprise_AI_API_Gateway_M4-R7_Normal_API_Protection_Admission_Ordering_Contract.md), [M4-R8](Enterprise_AI_API_Gateway_M4-R8_Normal_API_Concurrency_Lease_Renewal_Contract.md), and [M4-R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md) govern admission, renewal, and terminal cancellation/cleanup.

The existing non-null `normal_api_services` timeout fields and ORM/Admin create defaults are exactly:

| Service field | Default | M4 V1 meaning |
| --- | ---: | --- |
| `connect_timeout_seconds` | 5 | HTTPX connection establishment timeout |
| `pool_timeout_seconds` | 5 | HTTPX local pool-slot acquisition timeout |
| `write_timeout_seconds` | 30 | HTTPX per-write inactivity timeout |
| `read_idle_timeout_seconds` | 60 | HTTPX per-read inactivity timeout, including response streaming |
| `pre_response_timeout_seconds` | 120 | Gateway request-scoped deadline until first downstream response commitment |

All five are positive integers (`>= 1`) in Admin create/PATCH and database checks, with no frozen maximum. Their defaults are product/service creation defaults, not new deployment environment settings. Admin PATCH of a service timeout uses its existing non-null field semantics; an omitted field preserves the stored value. The Alembic schema has these columns, so the contract adds none.

`normal_api_routes.timeout_ms` is a nullable positive integer (`NULL` or `>= 1`) in the model, database check, Admin create/PATCH, and static OpenAPI. Admin create omission stores `NULL`; route PATCH omission preserves the current value, explicit `null` clears the override, and a positive integer sets it. Route reads return the stored integer or null. No upper bound, automatic clamp, or persisted rewrite is introduced. Existing static OpenAPI has the correct shape/bound but **does not describe its effective runtime meaning** in the Route, RouteCreate, and RoutePatch representations. That public semantic description must be reconciled separately before claiming M4 timeout implementation readiness; no shape or database change is required.

## 2. Exact route/service precedence

M4 V1 selects the narrow **pre-response-budget override** model. A route timeout never overrides the HTTPX connect, pool, write, or read-idle dimensions. Its only effect is to replace the selected service's pre-response-start deadline when non-null. For one resolved ACTIVE service `s` and route `r`:

```text
effective_connect_seconds = s.connect_timeout_seconds
effective_pool_seconds = s.pool_timeout_seconds
effective_write_seconds = s.write_timeout_seconds
effective_read_idle_seconds = s.read_idle_timeout_seconds
effective_pre_response_seconds =
    s.pre_response_timeout_seconds, if r.timeout_ms IS NULL
    r.timeout_ms / 1000,          otherwise
```

`NULL` means inheritance of the service pre-response budget, not an infinite timeout. A route value below the service pre-response value shortens that deadline; one above it lengthens that deadline. It is a replacement, **not** `min(route, service)`, not an override of all timeout dimensions, and not a service ceiling. The positive route value is valid even if it is greater or smaller than the service value. HTTPX phase timeouts continue to apply independently; whichever applicable terminal timeout is observed first wins under Section 5. The route override does not lengthen an effective connect, pool, write, or read-idle timeout. Neither service nor route timeout config defines a true end-to-end request lifetime limit.

This model follows the existing separate per-service transport fields, the singular nullable route `timeout_ms`, and the pack's service/route configuration wording without inventing fields. The precise route meaning is a new M4-R10 reconciliation decision, **not** a claim that the old field name or OpenAPI already documented it.

## 3. Phase clocks and HTTPX mapping

Rate checking, concurrency acquisition, bounded inbound body guard, deterministic local preparation, and circuit admission under M4-R7A do **not** consume any upstream timeout budget. The separate pre-response monotonic deadline starts immediately before the single HTTPX upstream send attempt is dispatched. It includes HTTPX pool waiting, DNS/connect/TLS work within HTTPX's connect phase, request writes, waiting for the final upstream response, and Gateway processing needed before the first downstream response-start commitment. It stops at the first committed downstream response start (`http.response.start`), including a valid bodyless response; upstream headers received but not yet safely committed downstream do not stop it. Informational upstream `1xx` alone does not satisfy it. This is a **pre-response** budget, not a whole-request deadline. M4's generic response stream can outlive it after commitment.

The four service transport values are passed **per upstream request**, not baked into the M4-R5 shared client or used to create a client per service. Use the installed HTTPX API's `httpx.Timeout(connect=..., pool=..., write=..., read=...)` in seconds, attached when building/sending that request. HTTPX's connect timeout bounds connection establishment as defined by HTTPX/httpcore, including its DNS/socket/TLS connect path where performed there; M4-R10 does not choose DNS address policy or TLS trust. Pool timeout begins when HTTPX waits to obtain an available pool connection and stops on acquisition or failure; it has no upstream socket outcome. HTTPX write timeout is an inactivity/progress limit for an individual send/write operation, including request-body upload, not one cumulative upload deadline. HTTPX read timeout is an inactivity/progress limit for each receive/read operation, including response headers and subsequent streamed response-body chunks; activity resets the wait for the next read. Neither is an overall phase-duration limit.

The pre-response deadline is a separate request-scoped monotonic timer; HTTPX has no fifth `Timeout` field for it. Do not misuse `read`, `write`, or `pool` to emulate it. After downstream response commitment, this separate timer is disarmed. The per-read idle timeout continues for generic upstream response streaming, and upstream write timeout continues while request upload is still active if the transport permits overlap. Downstream backpressure is not itself an upstream read-idle failure; M4 must not fabricate an HTTPX read timeout while it is not awaiting an upstream read. No total end-to-end M4 V1 request deadline is introduced. M4-R8 renewal remains active while the request still owns concurrency, including all timeout waits and downstream delivery.

## 4. Circuit and client-visible timeout results

| First observed terminal timeout | M4-R2 circuit class | Before downstream commitment | After commitment |
| --- | --- | --- | --- |
| HTTPX connect timeout | `FAILURE` | 504 `upstream_timeout` | terminate delivery; no second envelope |
| HTTPX write timeout | `FAILURE` | 504 `upstream_timeout` | terminate delivery; no second envelope |
| HTTPX read/response-idle timeout | `FAILURE` | 504 `upstream_timeout` | terminate delivery; no second envelope |
| HTTPX local pool-acquisition timeout | `NO_COMPLETION` | 504 `upstream_timeout` | not ordinarily post-commit; if observed there, terminate delivery |
| Gateway pre-response-start deadline | `NO_COMPLETION` unless an independent definitive M4-R2 upstream outcome was already recorded | 504 `upstream_timeout` | deadline is already disarmed; no new timeout result |

The separate pre-response deadline is a **Gateway-local budget cancellation**, not a verified upstream socket stall. It therefore makes no circuit success/failure call by itself. This is distinct from an HTTPX connect/write/read timeout, which M4-R2 already classifies as a transport `FAILURE`, and from pool timeout, which M4-R2 already classifies as `NO_COMPLETION`. If the budget interrupts an in-progress phase before that phase reports its own definitive transport timeout, use `NO_COMPLETION`; do not infer one from the phase name. An existing HALF_OPEN probe with `NO_COMPLETION` remains subject to the M3 probe lease, without invented abort/success/failure. If the definitive upstream outcome was recorded first under M4-R9, classify and complete from that outcome at most once and continue its selected downstream result; a later local deadline cannot replace it with 504.

All pre-commit 504 responses use the existing safe Gateway error envelope, canonical request ID, and `upstream_timeout` code/type. Pool timeout is publicly a Gateway timeout even though it does **not** count as upstream circuit failure; public error mapping and circuit classification are separate. After downstream commitment, terminate the stream/connection and record a safe category rather than sending a second 504. Client disconnect is M4-R9 cancellation, not a timeout or circuit failure. An M4-R8 ownership-loss cancellation uses its existing 500 boundary if it wins first, not 504.

## 5. Simultaneous events and cleanup

The M4-R9 single request-local terminal-decision gate serializes timeout, definitive upstream outcome, ownership loss, disconnect, and shutdown decisions. **First recorded terminal decision wins**. If two applicable timeout events are observable in the same decision step, choose the more specific HTTPX phase result in `pool → connect → write → read` order before the separate pre-response budget; this deterministic tie rule changes no phase's M4-R2 classification. A mere elapsed deadline not yet recorded cannot retroactively replace a previously recorded definitive upstream outcome. No second circuit completion, public response, or concurrency release is emitted.

Any winning timeout ends request-scoped upstream work under M4-R9; the shared AsyncClient stays open. M4-R8's renewal worker is stopped/awaited, then the request scope makes its one provenance-aware concurrency release attempt. The terminal circuit action, if any, uses only the first definitive M4-R2 result. A timeout never triggers a second upstream attempt, retry, or fallback.

## 6. API, configuration, compatibility, and later evidence

The existing Admin APIs already create, PATCH, and return each persisted timeout with positive bounds, including nullable route override. M4-R10 requires **API/OpenAPI description reconciliation** for the route field's replacement-only pre-response meaning and `NULL` inheritance, including the fact that route values may raise or lower that deadline without altering four service transport timeouts. The current static OpenAPI's type and minimum are accurate but its absent semantic description is material to Admin clients. The M4 execution pack's existing 504 `upstream_timeout` family covers every pre-commit timeout above, so no new status/code/response schema is required. Do not edit OpenAPI as part of this contract-only task.

No database migration or deployment timeout setting is required: all five service fields and the nullable route column already exist. Their persisted defaults apply to newly created services; existing persisted values remain authoritative. The future **single required Normal API service/route M3-R3A current-state consumer** must include all five service timeout values and nullable route `timeout_ms` in its authoritative snapshot and reconcile it from PostgreSQL. No separate timeout consumer or new configuration resource is added. Version equality alone does not certify that consumer if it is dirty/pending.

Bounded timeout observability may include canonical request ID, timeout category (`connect`, `pool`, `write`, `read_idle`, `pre_response`), circuit classification, and permitted tenant/service/route correlation in the existing telemetry schema. Do not use those identifiers as metric labels; never record credentials, bodies, raw headers/URL/query, raw exception text, or unbounded timing labels. No new metrics infrastructure or audit action is defined.

Later unit tests must cover service defaults; route null, lower, and higher replacement behavior; positive-bound validation; per-request `httpx.Timeout` mapping; separate pre-response monotonic deadline; no charging of M4-R7A local stages; disarming at downstream commitment; long healthy response streams; each HTTPX timeout category; pool timeout `NO_COMPLETION`; pre-response deadline `NO_COMPLETION`; transport `FAILURE`; first-winner/tie race; pre-/post-commit mapping; unchanged shared client; M4-R8 renewal stop/await; one release; and no retry. Local synthetic-upstream acceptance should exercise delayed response start, stalled streamed body, controlled slow upload, and pool starvation with `max_connections=1`; delayed connect/accept may be tested only where the local environment can reliably force it. Assert actual call order, classification, public result, and cleanup without external internet or arbitrary sleeps as correctness proof.

## 7. Narrow supersession and exclusions

M4-R10 fills the M4 pack's previously unspecified service/route precedence and M4-R5's deferred request-time pool timeout. It does not change M4-R2's classifier, M4-R7A admission order, M4-R8 renewal cadence, M4-R9 cancellation/terminalization mechanics, M3 protection algorithms, M6 retry/fallback, response-size policy, or outbound redirect, SSRF, DNS/IP, and TLS trust decisions. Those transport/security topics remain separate M4 gates.
