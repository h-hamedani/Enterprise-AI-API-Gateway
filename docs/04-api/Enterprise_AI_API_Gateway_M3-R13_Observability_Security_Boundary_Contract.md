# M3-R13 — M3.10 Observability and Security Boundary Contract

Status: FROZEN. Documentation only. M3.10 implementation is separate.

This addendum closes the M3.10 observability/security boundary identified by
the read-only audit. It qualifies the M3 Execution Pack, M3-R1–M3-R12 where
relevant, and the frozen OpenAPI without changing protection algorithms,
database or Redis formats, or public HTTP contracts. Existing M3-R7 dependency
classification, M3-R9 recovery safety, M3-R10 provenance, M3-R11 unknown-handle
results, and M3-R12 completion lifetime remain authoritative.

## 1. Telemetry architecture and required capability

M3.10 implements backend-neutral, injectable telemetry hooks using the existing
runtime telemetry-protocol pattern. It does not select Prometheus, an
OpenTelemetry metrics exporter, StatsD, a vendor SDK, a metrics HTTP endpoint,
a tracing backend, a SIEM, or a log-shipping backend. No new production
observability dependency is required. Later infrastructure may bind these
hooks to a concrete backend without changing runtime protection semantics.

The hooks must make these frozen event families observable:

- Redis dependency operation outcome and numeric latency;
- rate decisions and concurrency acquire, renew, and release outcomes;
- circuit eligibility, transition, and completion outcomes;
- Control Plane pre-auth protection;
- local degraded capacity exhaustion and traffic-mode/degradation transitions;
- recovery outcomes;
- configuration invalidation publication/subscription and reconciliation
  outcomes.

This contract freezes semantic events and safe dimensions, not external metric
names, histogram names or buckets, exporter protocol, scrape endpoint, or
aggregation backend. Tests assert semantic event recording and dimension
bounds, not vendor-specific names. M3.10 adds no `/metrics` equivalent.

## 2. Bounded dimensions and normalization

Every metric/event dimension must come from a finite internal vocabulary.
Allowed examples are dependency, operation class/outcome, circuit state,
normalized target kind, enum rate/concurrency scope type, fixed local-store
category, traffic mode, recovery outcome, reconciliation outcome, and
subscriber outcome. Numeric latency is an observation value, never a unique
label. An implementation must normalize a source string that can originate
outside a closed internal enum before using it as a dimension; unknown values
map to one fixed category such as `other` or `unknown`. Incoming
`resource_type`, target strings, callback names, and exception-derived strings
must not pass through as arbitrary dimensions.

The following are prohibited metric dimensions: tenant, resource, route,
service, provider, provider-target, model, alias, admin-user, admin-token,
or API-key IDs; request ID; config version; lease, probe, circuit-incarnation,
or local-generation ID; Redis key; raw client IP or HMAC client identity
digest; token, credential, or URL; arbitrary callback name or resource type;
dynamically unbounded exception class; exception message; and SQL, Redis, or
provider error text. Opaque provenance handles and M3-R12 monotonic deadlines
are not metric identifiers.

## 3. General operational logging and correlation

M3.10 assumes no protected identifier-bearing log sink. Security-sensitive
runtime logs must therefore be safe in a general operational destination.
They must not contain raw tenant, resource, route, service, model, provider,
admin-user, admin-token, API-key, lease, probe, incarnation, or local-generation
IDs; Redis keys; raw client IPs; or HMAC IP digests. Do not hash UUIDs merely
to retain identifier-bearing operational logs. Existing configuration
publication failure logs that include tenant/resource UUIDs must instead use
bounded categorical context and, where available, the canonical request ID.

The server-generated UUIDv7 `request_id` may appear in structured logs for
correlation. It is not secret, is not derived from a client-provided raw
identifier, never becomes a metric label, and does not replace bounded
telemetry dimensions. Universal request-ID propagation and distributed
tracing are not required by M3.10.

Logs, telemetry, health output, and HTTP errors must never emit Redis
credentials or password, a credential-bearing Redis URL, bearer Authorization,
`adm_` or `gw_` token/key plaintext, API-key secret, provider credential
plaintext, encrypted credential blob, or sensitive request-header value.

For M3 runtime protection, authentication, configuration coordination,
recovery, and dependency paths, failure logs use fixed bounded categories
such as `redis_unavailable`, `dependency_error`, `publish_failed`,
`subscriber_failed`, `reconcile_failed`, `callback_failed`, or
`recovery_failed`. Do not log `str(exc)`, `repr(exc)`, arbitrary exception
messages, Redis/PostgreSQL server payloads, or stack traces containing
uncontrolled dependency values. An unexpected exception may be represented by
a fixed category; M3.10 does not create a universal exception framework.
Existing typed HTTP error handling remains authoritative and must not expose
dependency exception text.

## 4. Recovery, degraded mode, and Control Plane pre-auth

M3.9 recovery outcomes must be observable with a finite vocabulary including
equivalents of `recovery_started`, `redis_not_ready`,
`subscriber_not_ready`, `config_not_reconciled`,
`final_redis_check_failed`, `certificate_invalidated`,
`recovery_succeeded`, and `recovery_failed`. No tenant/generation/handle ID,
Redis key, or exception text may accompany them as a dimension.

Preserve M3.8's distinction: `local_capacity_exhausted` is emitted only when
a bounded local store actually cannot reserve/create an entry after required
cleanup and safe eviction. Ordinary rate denial, concurrency rejection, OPEN
circuit rejection, and active-probe denial are not capacity exhaustion.

Control Plane pre-auth telemetry uses fixed operation/scope and bounded
outcomes such as `allowed`, `rejected`, and `dependency_error`. Neither raw
client IP nor its HMAC-derived Redis-key identity may appear in logs or
telemetry. The HMAC identity remains valid only for internal keying.

## 5. Configuration and Pub/Sub logging

Publication, subscription, callback, and reconciliation logs must not expose
tenant/resource IDs, arbitrary callback names, raw Pub/Sub payloads, or
database/Redis exception text. Allowed fields include bounded operation and
outcome, a resource class normalized to a finite internal vocabulary, and
`request_id` where already available. Config version may appear in a
diagnostic structured log if useful, but never as a metric dimension; omit
it when it adds no operational value.

## 6. Redis keys, values, and Lua

Runtime Redis keys may not contain secrets, bearer/API token plaintext, raw
IP, credential material, or arbitrary user-provided fragments. UUID-bearing
coordination keys already permitted by frozen M3 contracts remain allowed.
The canonical namespace is `gw:v1:`. The frozen M3.6 legacy dual-publication
channel `gateway:config` is the intentional compatibility exception.

`ResolvedRatePolicy.key_override` must not remain a general arbitrary
caller-supplied Redis-key escape hatch in production runtime use. Preserve
the frozen M3.5 pre-auth key semantics behind a dedicated internally
constructed type, validated internal key object/helper, or equivalent
non-arbitrary representation. Reusable callers must be unable to supply raw
arbitrary key text. If that requires a wider public runtime API redesign than
a narrow internal type or validation change, stop and report the conflict.

M3.10 does not redesign Lua algorithms. Security evidence must establish
static version-controlled scripts, explicit supplied `KEYS`/`ARGV`, no
user-value concatenation into executable Lua, no keyspace scan, no persisted
secret payload, no result/error echo of sensitive input, and preservation of
existing numeric safety bounds. Redis coordination values must remain free of
credentials, request/provider content, and raw sensitive headers.

## 7. Circuit-key expiry disposition

The current circuit state/failure-key expiry discrepancy is **not** resolved
by M3.10. Do not invent circuit TTLs: expiry could change M3-R4B incarnation
and generation lifetime, normal-token validity, and OPEN/HALF_OPEN behavior.
Track it as a separate circuit-contract reconciliation item before final M3
release acceptance. M3.10 may proceed without changing circuit expiry;
M3.11 final GO must resolve or explicitly disposition the discrepancy, not
silently waive it.

## 8. Health, HTTP errors, and request identity

Frozen OpenAPI remains authoritative. M3.10 must make `/health/ready`'s
successful response conform to `HealthResponse`, including its
`additionalProperties: false` rule; bound the PostgreSQL readiness operation
to the frozen explicit one second; and return no dependency address or
exception text. No OpenAPI change is authorized. Preserve `/health/live` as
bounded process liveness and `/health/traffic` as bounded status/mode with the
M3.9 names, without identifiers, URLs, credentials, or exception text.

Test that Redis/DB dependency failures, authentication failures, and internal
runtime failures do not expose exception contents, Redis keys, SQL, credentials,
or stack traces through typed HTTP errors. No new HTTP error schema is
required absent a demonstrated mismatch. Client-supplied `X-Request-ID` does
not become authoritative internal identity; canonical UUIDv7 request ID
remains a log correlation field, never a metric dimension.

## 9. Acceptance evidence

Deterministic M3.10 tests must prove at least:

| ID | Required observation |
| --- | --- |
| A | Every required event family carries only bounded dimensions. |
| B | Tenant/resource/request/lease/probe/generation IDs are absent from metric labels. |
| C | Arbitrary incoming `resource_type` is normalized, never an unbounded label. |
| D | Redis latency is a numeric observation, not a label. |
| E | Recovery outcome vocabulary is finite. |
| F | Ordinary policy/circuit denials do not emit `local_capacity_exhausted`. |
| G | Auth/pre-auth logs contain no raw IP, token, or HMAC identity. |
| H | Publication failure logs contain no tenant/resource UUID. |
| I | Redis dependency failure logs contain no URL, password, or error text. |
| J | Callback/reconciliation failures contain no exception payload. |
| K | Typed HTTP dependency errors contain no raw exception content. |
| L | Health output contains no secret, identifier, or error text. |
| M | `/health/ready` success shape matches frozen OpenAPI. |
| N | PostgreSQL readiness obeys the frozen one-second bound. |
| O | Arbitrary unsafe rate-key override is rejected. |
| P | The legitimate internally constructed pre-auth key still works. |
| Q | Redis keys contain no secret, raw IP, or token. |
| R | Lua source is static; user values are never injected into its source. |
| S | Redis coordination values contain no credential or request/provider content. |
| T | `request_id` may correlate logs but is never a telemetry dimension. |
| U | M3.9 handles, generation IDs, and deadlines are absent from labels. |
| V | M3-R11 `UNKNOWN_HANDLE` never logs raw token/probe content. |
| W | The available Gitleaks scan runs successfully where the environment permits. |

Repository secret scanning is a required quality/security gate. M3.10
validation should run the available Gitleaks scan where possible, without
adding a CI platform integration solely for this package. Full M3/M3.11
release acceptance includes the frozen repository secret-scan gate.

## 10. Milestone boundary and compatibility

M3.10 owns backend-neutral bounded telemetry, dimension enforcement, runtime
redaction and log hardening, Redis key/Lua/value security tests, health
contract correction, and rate-key-override hardening. M3.11 owns controlled
Redis outage/restart, multi-instance chaos, final integration/chaos, and final
release security gates. Previously missing real Redis outage/restart evidence
is M3.11 work, not an inferred M3.10 blocker.

No database migration, DB schema change, Redis state-machine format change,
OpenAPI change, new metrics/tracing dependency, or external metrics endpoint
is expected. If backend-neutral telemetry cannot be implemented with current
mechanisms, stop and report why.
