# M3-R6 Local Degraded Protection Contract

Status: authoritative M3.8 contract addendum. This document freezes local
degraded protection semantics before implementation. It introduces **new**
bounded-state TTL, entry-count, and capacity-exhaustion decisions; those
numerical bounds were not specified by the earlier PRD, HLD, ERD, physical
model, or M3 Execution Pack. No M3.8 runtime or database change is made here.

## Inherited M3 direction

Redis unavailability must not silently bypass protection. Degraded rate,
concurrency, pre-auth, and circuit state is process-local, non-durable, and
independent across gateway instances; the cluster-wide allowance need not
equal the normal Redis-coordinated limit. A process restart starts with fresh
local state. No degraded state is persisted to PostgreSQL, Redis, disk, or
`config_versions`.

For a DB-backed policy, use its persisted `degraded_factor`. The physical
model stores a non-null factor with default `0.25` and requires
`0 < factor <= 1`. If an otherwise valid compatibility/test representation
omits it, use `0.25`, never bypass protection. For positive configured
integer capacity `C`, degraded capacity is exactly
`max(1, floor(C * factor))`; use deterministic arithmetic, not nearest
rounding or ceiling. Apply this independently to `requests_per_window` and
`max_concurrency`. The original `window_seconds` does not change. With factor
`0.25`, capacities 1, 2, 3, 4, 5, 8, and 20 yield 1, 1, 1, 1, 1, 2, and 5.

Local rate limiting retains M3.2's deterministic policy ordering and
all-or-nothing layered request consumption. Evaluate all applicable policies
together. If every layer allows, consume request cost from every bucket; if
any rejects, consume request cost from none. Refill state may still change,
and retry delay is the maximum among rejecting layers. Use a local token
bucket, original policy windows, canonical tenant/scope isolation, and
deterministic monotonic-time arithmetic without admission-changing
floating-point drift. Do not use Redis/Lua, Redis TIME, wall-clock correctness
timestamps, or cross-instance coordination in this fallback.

Local concurrency acquisition is likewise all-or-nothing across applicable
policies, with idempotent release and no partial ownership after rejection.
Existing configured `concurrency_lease_duration_ms` determines each local
owner's monotonic expiry. Preserve lease identity compatibility where useful,
but do not claim a distributed lease or write local ownership back to Redis.

The M3.5 pre-auth guard retains its trusted-proxy-derived effective client
identity and shared `ADMIN_AUTH_PROTECTED` class. Its normal baseline is 20
attempts per 60 seconds; degraded capacity is 5 per 60 seconds per process
and effective identity. Do not key local state by raw bearer credentials or
weaken trusted-proxy handling. DB-backed `ADMIN_TOKEN` policies use their own
factor and the ordinary degraded rate/concurrency scaling; no new policy
scope is added.

Preserve M3.4 circuit target identity: tenant plus route and service/backend
target for Normal API; tenant plus provider target and model for LLM. A target
without valid previous local state begins `DEGRADED_HALF_OPEN`, not `CLOSED`,
and admits at most one local probe in flight. Reuse the validated M3.4
failure threshold, failure window, open duration, single half-open probe,
one-success-to-close rule, and probe lease duration where meaningful. All
local circuit timing is monotonic; no distributed generation or incarnation
guarantee is claimed. Do not query unavailable Redis to reconstruct state.

Only Redis/dependency unavailability activates local fallback. Invalid
policies, cross-tenant policy sets, unsupported scopes, malformed IDs, and
programming defects retain their validation/internal failure classes. A
single attempt is enforced by a usable normal Redis result or by local
degraded protection after no usable Redis result. A timeout may follow a
Redis-side mutation, so fallback prioritizes safety without claiming exact
cross-mode counter equivalence; normal and degraded counters are not merged.
Use canonical, validated policy/client/target identities before creating
bounded state, never arbitrary untrusted strings.

Redis failure with functioning degraded protection does not by itself make
readiness false. Expose at least the internal state concepts
`mode_degraded=true` and `reason=redis_unreachable`, with bounded telemetry
dimensions and sanitized errors. Do not use tenant, policy, target, client IP,
token, or Redis-key values as metric labels. Full observability hardening is
outside M3.8.

## New M3-R6 bounded-state decisions

These rules are newly frozen by M3-R6, not inherited numerical defaults.
Each process has four independent stores: rate, concurrency, circuit, and
pre-auth. Each store has its own default maximum of **10,000 entries**. One
deployment/runtime setting conceptually equivalent to
`degraded_local_max_entries_per_store=10000` applies independently to all
four. Validate it as a positive bounded integer. A pre-auth identity flood
must not consume the rate, concurrency, or circuit store's entry budget. No
shared four-store pool or database column is introduced.

| Store | Monotonic TTL and refresh rule | Unsafe-to-evict state |
| --- | --- | --- |
| Rate | Idle TTL `2 * window_seconds`, refreshed whenever that bucket is evaluated, allowed or rejected. For a 60-second window, TTL is 120 seconds. | A bucket whose eviction would prematurely restore rate capacity. |
| Pre-auth | Idle identity TTL 120 seconds, refreshed whenever that identity is evaluated. | An identity still carrying effective throttling state. |
| Concurrency | Each owner expires at its configured lease duration. An **empty** per-scope container has idle TTL `2 * concurrency_lease_duration_ms`; refresh activity on acquire attempt, successful release, supported renewal, and expiry cleanup. | Any active unexpired ownership, regardless of container idle age. |
| Circuit | Once quiescent `CLOSED` with no active probe or relevant failure history, idle TTL is `max(2 * failure_window_ms, 2 * open_duration_ms, 2 * probe_lease_duration_ms)`, refreshed on meaningful evaluation or transition. Current defaults yield 120 seconds. | `OPEN`, `HALF_OPEN`, `DEGRADED_HALF_OPEN`, an active probe, or unexpired failure-window history. |

Expired inactive entries may be purged lazily during normal operations; no
dedicated cleanup thread is required. A generic cache TTL must never erase
active ownership or circuit safety state. All TTL and expiry decisions use a
process monotonic clock, not wall time, Redis TIME, or PostgreSQL time. Tests
must control that clock.

Before allocating a new entry when its store is full, first purge expired
entries **in that store**. If still full, evict the least-recently-used safe
inactive entry in that store. Rate buckets and pre-auth identities may be
LRU-evicted only when expired or otherwise safely inactive; an empty/inactive
concurrency container may be evicted, never one with active ownership. Only
quiescent safe `CLOSED` circuit state is LRU-evictable. Do not evict an `OPEN`
or half-open circuit, active probe, or valid failure history. If no safe
inactive entry exists, do not allocate beyond the bound:

| Full store without a safe slot | Conservative result |
| --- | --- |
| Rate | Reject the request locally. |
| Pre-auth | Reject the attempt locally. |
| Concurrency | Reject acquisition without partial ownership. |
| Circuit | For an unseen target, admit no probe and treat it as locally ineligible for that attempt. |

Expose a bounded internal outcome such as `local_capacity_exhausted`, not
internal map details to clients. Never bypass protection, overwrite active
state, or silently treat an unknown target as `CLOSED`.

## Ownership boundary

M3.8 supplies local enforcement when Redis is unavailable. It does not
redesign Redis failure detection, convert ordinary validation errors into
degraded mode, or implement recovery convergence. M3.9 owns the recovery
barrier, PostgreSQL reconciliation before return to shared mode, Redis/local
cutover, degraded-state discard, and protection-gap prevention. M3.8 must
leave interfaces/state that M3.9 can consume without implementing those
operations now.

## Required M3.8 acceptance

Tests must cover:

1. Default factor 0.25, policy override, floor rounding, minimum-one rate
   and concurrency capacity, and unchanged rate window.
2. Rate TTL `2W`, pre-auth TTL 120 seconds, and expiry/refresh on both allowed
   and rejected evaluations.
3. Empty concurrency-container TTL twice the lease duration, owner expiry,
   and non-eviction of active ownership.
4. Default quiescent circuit TTL 120 seconds; non-eviction of `OPEN`,
   half-open/probe, and valid failure-history state.
5. Independent 10,000-entry bounds for all four stores; expired-first
   cleanup, safe inactive LRU, and fail-closed exhaustion for rate,
   pre-auth, concurrency, and circuit.
6. Multi-layer local rate and concurrency all-or-nothing behavior, maximum
   rejecting rate retry delay, deterministic ordering, monotonic time, and
   per-instance isolation.
7. Trusted-proxy identity unchanged; pre-auth degraded 5 per 60 seconds;
   unknown circuit `DEGRADED_HALF_OPEN`, a single local probe, and configured
   circuit transitions.
8. Redis unavailability never silently bypasses protection, while policy
   validation failures do not activate degraded mode. Ambiguous Redis
   timeout does not claim exact cross-mode accounting.
9. No persistence, fresh state after process restart, bounded/sanitized
   telemetry, and no M3.9 recovery behavior in M3.8.

This addendum is documentation only. It adds no runtime implementation,
test, database migration, or durable state.
