# M3-R4 Distributed Circuit State Contract Reconciliation

Status: M3.4 contract handoff only; no circuit runtime is implemented here.
[M3-R4A](Enterprise_AI_API_Gateway_M3-R4A_Circuit_Configuration_Source_Contract_Reconciliation.md)
controls configuration and
[M3-R4B](Enterprise_AI_API_Gateway_M3-R4B_Circuit_Generation_Eligibility_Token_Contract_Reconciliation.md)
controls normal eligibility, generation, incarnation, stale-result rejection,
and exact-integer overflow. This document adds transition and identity details
without changing those addenda.

## Circuit identity and Redis keys

The canonical key is exactly
`gw:v1:cb:{tenant}:{target_kind}:{target_id}:{dimension}`. `{tenant}` is the
tenant UUID Redis Cluster hash tag. Construction is typed and tenant-scoped;
callers cannot provide arbitrary Redis keys. Keys contain no credentials or
secret-bearing URLs. Any auxiliary metadata/failure key retains the same
`{tenant}` hash tag so all atomic operations stay in one Cluster slot.

The frozen M3/HLD dimensions map as follows:

| Workload | `target_kind` | `target_id` | `dimension` | Frozen identity |
| --- | --- | --- | --- | --- |
| Normal API | `route` | tenant-owned `normal_api_routes.id` | its `service_id`, the V1 service backend/upstream target | tenant + Route + Upstream Target |
| LLM | `provider_target` | tenant-owned `llm_provider_targets.id` | tenant-owned `llm_models.id`, the model/deployment identity | tenant + Provider Target + Model/Deployment |

The V1 Normal physical model has a service backend, not a separate upstream
target table; M4 identifies that service backend as the V1 upstream target.
The route's tenant-aware service FK establishes its backend relationship. The
LLM model's tenant-aware provider-target FK establishes its relationship.
Runtime identity construction must verify those relationships and never accept
an unrelated route/service or provider-target/model pair. Do not use mutable
names, URLs, provider deployment strings, or raw request values as IDs. No
additional target kind is introduced.

## Configuration, state, and clock

Consume typed, resolved `CircuitConfig` from M3-R4A. Its deployment-wide
settings are `failure_threshold`, `failure_window_ms`, `open_duration_ms`,
`half_open_probe_limit`, `successes_to_close`, and
`probe_lease_duration_ms` (names conceptual). Defaults belong to startup
configuration, not hard-coded algorithm branches. V1 requires exactly one
HALF_OPEN probe and one successful probe to close.

Minimal Redis metadata holds `state`, `incarnation_id`, `generation`,
`open_until_ms`, `probe_owner`, and `probe_expires_at_ms` as applicable. A
separate sorted set holds failure history: score is Redis failure timestamp in
milliseconds; member is a unique opaque collision-resistant event ID, even
for simultaneous failures in the same millisecond. The event ID contains no
body, credential, secret endpoint, or exception text. Metadata and history
must be same-slot and each correctness-sensitive transition atomic across
both. CLOSED admission from missing state atomically materializes a fresh
random incarnation and generation 1 before returning a normal token.

Redis TIME is the sole distributed correctness clock. Within each atomic
operation, compute `now_ms = seconds * 1000 + floor(microseconds / 1000)`.
Use it for failure events, pruning, OPEN and probe deadlines, and expiry
boundaries. Python wall time cannot decide circuit transitions.

## Three internal logical operations

`check_or_claim_eligibility` returns a typed result with `eligible`, `state`,
and exactly one of normal eligibility token, `probe_id`, or neither. A CLOSED
normal admission returns M3-R4B's circuit-scoped `incarnation_id + generation`
token. OPEN denies admission while `now_ms < open_until_ms`. At
`now_ms >= open_until_ms`, exactly one caller atomically claims the HALF_OPEN
probe; all others are denied. An unexpired HALF_OPEN owner also denies others.
`record_success` and `record_failure` accept the matching normal token or
probe ownership ID and return `applied` and `resulting_state`. These are
internal results, not public HTTP fields.

Every admission/materialization, outcome validation and mutation, OPEN to
HALF_OPEN claim, and expired-owner replacement is one Redis-side atomic
operation. No client read-decide-write sequence is valid.

## CLOSED outcomes and sliding window

For `W = CircuitConfig.failure_window_ms`, active failures are strictly
`(now_ms - W, now_ms]`; prune timestamps `<= now_ms - W` before inserting a
current eligible failure. A unique opaque event member prevents same-ms
collisions. If active count is `< CircuitConfig.failure_threshold`, remain
CLOSED. If it is `>= CircuitConfig.failure_threshold`, atomically move CLOSED
to OPEN with `open_until_ms = now_ms + CircuitConfig.open_duration_ms` and
clear/normalize failure history. The generation and incarnation do not change
on this trip. This is a sliding window, not fixed buckets.

Normal success applies only when current state is CLOSED and both token
incarnation and generation match; it clears failure history and remains
CLOSED. Normal failure has the same applicability predicate, then prunes,
inserts, counts, and possibly trips. A non-CLOSED state or stale token returns
`applied = false` with no mutation, including no failure insertion or history
clear. A result admitted in CLOSED(7) is rejected in OPEN(7), HALF_OPEN(7),
and recovered CLOSED(8). A previous incarnation is rejected after Redis
state loss even if generation again equals 1.

Failure classification is supplied by later invocation integrations: timeout,
connection/provider unavailable, and selected upstream/provider 5xx may be
eligible; provider 429 is fallback-eligible but not a circuit failure by
default. Provider-caused mid-stream disconnect/timeout is eligible even after
output commitment; client cancellation/disconnect is not. This state store
does not implement invocation or classify raw exceptions.

## OPEN and HALF_OPEN

At `now_ms >= open_until_ms`, exactly one distributed winner atomically sets
HALF_OPEN, an opaque new `probe_id`, and
`probe_expires_at_ms = now_ms + CircuitConfig.probe_lease_duration_ms`.
Probe ownership is independent of normal eligibility tokens. An owner is
expired at `probe_expires_at_ms <= now_ms`; its later outcome is stale. One
new caller can immediately replace an expired owner with a fresh probe ID and
deadline, without restarting OPEN cooldown. Generation and incarnation remain
unchanged on claim or replacement.

Only the exact current, unexpired probe owner can apply an outcome. On success,
atomically set CLOSED, increment generation exactly once, preserve
incarnation, and clear probe owner, probe deadline, open deadline, and failure
history. V1 needs no HALF_OPEN success counter because
`CircuitConfig.successes_to_close == 1`. On failure, atomically reopen with a
new `open_until_ms = now_ms + CircuitConfig.open_duration_ms`, clear owner and
probe deadline, normalize failure history, and preserve generation and
incarnation. Probe failure reopens immediately without reaching the CLOSED
failure threshold. Wrong, previous, expired, or missing ownership returns
`applied = false` without altering the valid owner or state. A normal token
never authorizes a probe result.

M3-R4B's exact safe generation range ends at `9007199254740991`. At that
value, probe success returns its typed circuit-state error before any partial
mutation; it never wraps or collides with an ancient normal token. There is
no additional independent epoch beyond incarnation plus generation.

## Restart, configuration change, and failure boundaries

Missing/lost Redis state is logically CLOSED; first normal admission creates
a new incarnation with generation 1. Old normal tokens and probe IDs cannot
apply to rematerialized state. M3.4 does not reconstruct missing shared
circuit state. M3.9 owns broader Redis recovery coordination.

M3-R4A requires process restart for deployment config changes. Existing
absolute `open_until_ms` and `probe_expires_at_ms` retain their meaning;
future transitions use the newly resolved `CircuitConfig`, and the next
CLOSED failure observation prunes using its new failure window. No deadline
is rewritten merely because configuration changed.

If Redis is unavailable, return a typed circuit dependency failure: do not
assume CLOSED or OPEN, keep process-local authoritative state, or silently
apply degraded behavior. M3.8 owns conservative degraded local protection.

## Telemetry, security, and scope

Use bounded operation labels (`eligibility`, `success`, `failure`) and bounded
state/outcome labels (`closed`, `open`, `half_open`, `probe_granted`,
`probe_denied`, `applied`, `stale`, `error`). `target_kind` and `dimension`
may be labels only if enum-bounded; the UUID dimensions here are not. Never
label metrics with tenant/target UUIDs, incarnation, generation, probe ID,
Redis key, or exception text. Do not log opaque ownership tokens by default.
Redis state/logging must not contain API keys, Admin tokens, provider
credentials, Authorization, request/provider response bodies, or raw exception
contents.

Redis owns circuit runtime state. There is no circuit database table,
configuration/generation table, Alembic migration, Control Plane API, public
HTTP field, provider/upstream invocation, Lua/Python implementation, local
degraded circuit, or recovery barrier in this contract artifact.
