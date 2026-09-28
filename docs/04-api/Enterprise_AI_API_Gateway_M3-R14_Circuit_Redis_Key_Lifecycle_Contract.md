# M3-R14 — Redis Circuit Key Lifecycle and Orphan Cleanup Contract

Status: FROZEN. Documentation only; cleanup implementation is separate.

This addendum resolves the circuit-key expiry discrepancy deferred by M3-R13.
It qualifies the M3 Execution Pack's requirement that all runtime keys have an
explicit TTL/expiry strategy and that orphaned coordination state self-heal.
An expiry strategy does **not** require a Redis TTL on every key. For circuit
coordination, the strategy is **authoritative identity lifecycle cleanup**,
not time-based expiry of active state. M3-R4, M3-R4A, and M3-R4B remain
authoritative for circuit transitions, configuration, and token fencing.

## 1. Circuit keys and active identities

The primary circuit hash retains its canonical pattern:
`gw:v1:cb:{tenant}:{target_kind}:{target_id}:{dimension_id}`. Its associated
failure-history sorted set is the same key with `:failures` appended. The
existing tenant hash tag and key format do not change.

A circuit identity is **active** for Redis lifecycle purposes while
authoritative PostgreSQL configuration permits that exact identity to be
constructed and routed. For Normal API, the route and its owning service
relationship must remain authoritative and routable. For LLM, the provider
target and its owning model relationship must remain authoritative and
routable. Exact checks follow the existing resource lifecycle, routing, and
tenant-ownership rules; Redis does not define a second business-activity
model.

Neither active primary state nor its failure-history key receives an
inactivity or independently inferred runtime TTL. No `EXPIRE` or `PEXPIRE` is
added to active primary state, and no arbitrary multiplier or refresh policy
is added to failure history. An active circuit may remain CLOSED indefinitely,
OPEN beyond its deadline while idle, or HALF_OPEN with probe-related state
under the frozen algorithm. Time passing alone does not physically delete it.

This preserves same-incarnation/same-generation CLOSED normal-token validity,
OPEN cooldown and first post-deadline HALF_OPEN probe behavior, probe
ownership/replacement, and incarnation/generation lifetime. The next
legitimate circuit operation performs the frozen transition. In particular,
the first eligibility evaluation after an idle OPEN deadline must observe
persisted OPEN state, not fresh CLOSED state caused by key expiry. Expiration
of a probe lease does not retire an active identity; the next operation may
replace its expired owner under the existing rules.

Failure scores continue to be pruned on eligible CLOSED failure observation,
and existing transition paths continue to delete failure history. A nonempty
history key may physically persist while idle even after its scores fall
outside the currently configured window. This is intentional: M3-R4A permits
a larger failure window after coordinated configuration restart, and the next
observation applies that new window. History may also be removed when the
authoritative identity is retired. No independently derived failure-key TTL
may erase events merely because they are outside the previous window.

## 2. Retirement and cleanup

An identity is **lifecycle-retired** only when authoritative PostgreSQL
configuration proves that the exact identity can no longer legitimately be
constructed for new routing. Examples are permanent removal of a constituent
resource, removal of its authoritative relationship, or a lifecycle state
that makes the exact identity permanently unroutable under existing frozen
routing rules. Mere circuit inactivity, absence of requests, Redis idle time,
OPEN, HALF_OPEN, Redis recovery, local degraded operation, provider failure,
or temporary resource unavailability is not retirement.

Once retired, its primary hash and associated `:failures` set are orphan
coordination state and may both be deleted for that exact identity. Treat
them as one logical cleanup, atomically where practical. Cleanup is not a
circuit transition and must not reconstruct state or migrate values. Deleting
an already-missing primary key, failure key, or both is successful idempotent
cleanup. Multiple gateway instances may independently discover and clean the
same orphan; no leader election or process-local ownership transfer is needed.

Retirement cleanup may make previously issued Redis circuit handles stale.
That is intentional for an identity no longer eligible for authoritative new
routing. An old completion cannot recreate business routing or resurrect the
old incarnation; existing M3-R4B missing-state/incarnation checks still
fence its outcome. No special token migration is introduced. For active
identities, M3-R4B Redis normal-token validity is unchanged and has no new
TTL. M3-R12's fixed completion deadline applies only to LOCAL degraded normal
completion wrappers, not Redis normal tokens.

## 3. Event-driven and reconciliation cleanup

Orphan self-healing uses two complementary mechanisms:

1. **Event-driven retirement cleanup** is the fast path. Runtime cleanup MUST
   attempt deletion after the authoritative PostgreSQL change commits when
   that committed change retires a circuit identity. Failure to clean Redis
   must not roll back the committed business change. Pub/Sub is an
   invalidation signal, not durable truth.
2. **A bounded reconciliation sweep** repairs missed invalidations, process
   downtime, Redis reconnect, historical pre-M3-R14 keys, and temporary
   orphan recreation by stale replicas. It discovers candidate canonical
   circuit keys in bounded batches, validates the candidate identity against
   PostgreSQL, and MAY delete the circuit-key pair only after authoritative
   retirement has been confirmed. It is cancellation-safe, tolerates
   duplicate cleanup, and does not require exactly-once deletion.

The sweep must not use Redis `KEYS` or an unbounded full-keyspace blocking
operation. If keyspace discovery is needed, use incremental `SCAN` or an
equivalently bounded mechanism with canonical circuit-prefix matching and
bounded COUNT/batch processing. SCAN is a repair mechanism, not authority or
a transactional-completeness guarantee. Do not log discovered raw keys or use
their contents as telemetry dimensions. Unrelated `gw:v1:` keys are outside
cleanup scope.

Immediately before deletion, cleanup **must revalidate** that the exact
candidate identity is still non-authoritative/unroutable. Discovery-time
classification is insufficient. If the same UUID identity has become
authoritative again, current PostgreSQL state wins and its keys must not be
deleted. If authority cannot be proven, including on database failure,
retain Redis state, record only bounded failure telemetry, and retry later.
Fail safe toward retention.

A stale replica may temporarily recreate state for an already-retired
identity before its local configuration catches up. M3-R14 introduces no
global cleanup lock or tombstone for that race. PostgreSQL remains authority;
M3.6/M3.7/M3-R3A configuration repair and a later cleanup sweep remove the
orphan again. Such recreation is not valid durable circuit truth.

## 4. Rollout, keyspace, and security

No database migration is required. The sweep may lazily remove historical
orphan keys created by older runtime versions after authoritative validation.
Do not flush the circuit namespace, blindly assign TTLs to existing keys, or
delete a key solely because it was discovered. Active historical circuit
state remains valid.

Historical accumulation is bounded by eventual reclamation of identities
that authoritative configuration can no longer route. This does not cap the
number of active identities below configured topology. Expected key count is
approximately current valid identities plus temporarily unreconciled orphans,
not every identity ever observed. Failure-history value growth continues to
follow the existing circuit pruning and transition-deletion algorithm.

Cleanup telemetry uses bounded categories only, such as `candidate_seen`,
`still_active`, `orphan_deleted`, `already_missing`,
`authority_unavailable`, `redis_unavailable`, and `cleanup_error`. Redis
keys, tenant/target/model/service UUIDs, and raw exception text must not
appear as metric dimensions or operational identifiers. Existing circuit
keys/values contain coordination identifiers, not secrets; cleanup must not
add credentials, bearer/API tokens, raw user content, or provider
prompts/completions to Redis or logs. M3-R13's telemetry and redaction
boundary remains authoritative.

Redis loss or other missing state still results in fresh incarnation and
generation on later eligibility under M3-R4B. Lifecycle cleanup is **not**
declared equivalent to arbitrary Redis loss: it is permitted only after
PostgreSQL proves the identity no longer valid for routing. M3-R9 recovery
still performs no local-to-Redis circuit writeback or merge, and M3-R10/R11
handle provenance/completion results remain unchanged.

## 5. Implementation boundary

An eventual implementation may add a bounded orphan-cleanup service,
post-commit lifecycle hooks, a bounded periodic/repair sweep, and a narrow
atomic two-key delete helper. Names and placement are implementation-defined.
Do not mix cleanup into normal eligibility/outcome Lua transitions. M3-R14
does not change the CLOSED/OPEN/HALF_OPEN algorithm, threshold, window
calculation, open duration, probe lease, generation/incarnation checks,
Redis key format, or normal Lua transition semantics. No schema, migration,
or OpenAPI change is required by this contract.

## 6. Required acceptance evidence

| ID | Required evidence |
| --- | --- |
| A–D | Active CLOSED, OPEN, and HALF_OPEN primary hashes and active failure history have no inferred TTL. |
| E–F | Idle OPEN survives its deadline and next evaluation uses HALF_OPEN/probe; HALF_OPEN/probe state survives idle time under frozen lease/replacement semantics. |
| G | Active Redis normal-token validity remains M3-R4B-compatible. |
| H–I | Authoritative retirement deletes both keys; cleanup of either/both already-missing keys is idempotent. |
| J–K | PostgreSQL authority failure causes no deletion; a candidate that becomes active before final revalidation survives. |
| L–M | Duplicate multi-instance cleanup is safe; cleanup failure cannot roll back the committed resource/configuration change. |
| N–O | Sweep iteration is bounded and uses no Redis `KEYS`; unrelated `gw:v1:` keys survive. |
| P–R | Active historical state survives rollout; historical or stale-replica-recreated orphans are eventually removed. |
| S | Cleanup telemetry contains no raw key, UUID, or exception text. |
| T–U | Circuit transition Lua semantics remain unchanged; there is no schema, migration, or OpenAPI change. |

## 7. M3.11 final release gate

Before Final M3 GO, M3.11 needs evidence that this contract is implemented:
active circuit keys remain non-expiring; retired/orphan identities are
eventually reclaimed; the bounded sweep handles historical churn;
concurrent cleanup is safe; controlled Redis loss/restart preserves M3-R4B
fencing; and tested retired identities do not remain in keyspace
indefinitely. The existing real Redis outage/restart evidence gap remains a
separate M3.11 item. This document freezes the contract only; it does not
implement cleanup or close the release gate.
