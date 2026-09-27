# M3-R9 — Redis Recovery and Safe Cutover Contract

Status: FROZEN. Documentation only. M3.9 implementation remains separate.

This addendum qualifies earlier wording about immediate local-state discard,
recovery mode naming, point-in-time configuration completeness, and completion
of pre-cutover local ownership. All unrelated M3-R1–M3-R7 and M3-R3A rules
remain authoritative. In particular, M3-R7 dependency classification, M3-R6
fail-closed degraded protection, M3-R5 bounded tenant membership, and M3-R3A
fencing are not weakened.

## 1. Process-local traffic states

The only externally reported traffic states are `NORMAL`, `DEGRADED_REDIS`,
and `RECOVERING_REDIS`, matching the frozen OpenAPI. The M3.8 internal
`DEGRADED` spelling must be reconciled to `DEGRADED_REDIS` during later
implementation; this document changes no runtime code.

| State | Backend for **new** protection decisions | Meaning |
| --- | --- | --- |
| `NORMAL` | Redis | Rate, concurrency acquisition, circuit eligibility, pre-auth, and `ADMIN_TOKEN` all select Redis. Retired local completion ownership may still exist internally. |
| `DEGRADED_REDIS` | Active process-local M3.8 degraded generation | Redis is not opportunistically retried from request paths. Separate health probes do not themselves change traffic mode. |
| `RECOVERING_REDIS` | The same active local degraded generation | Preparation only: no new Redis-backed traffic protection is admitted until atomic cutover. |

No fourth externally visible state is required. Local state is never merged
with or written into Redis.

## 2. Recovery trigger and preparation

A process-local background recovery coordinator, not arbitrary user traffic,
may start a recovery attempt after a bounded Redis dependency availability
probe succeeds while `DEGRADED_REDIS`. At most one attempt may run per process.
Attempts are bounded and no tighter than once per second. Exact cadence is an
operational configuration detail; reuse any already frozen cadence rather
than creating a second scheduler. The initial probe permits entry into
`RECOVERING_REDIS`, not cutover.

Preparation establishes Redis connectivity, the canonical M3.6 Pub/Sub
subscriber's operational subscription, and configuration completeness. A
connected Redis client without a re-established invalidation subscription is
insufficient. Pub/Sub is a fast path, not durable history; no historical event
replay is required or permitted. PostgreSQL reconciliation repairs missed
configuration history.

During preparation, all new protection decisions remain on the active local
generation. Failure at any preparation prerequisite returns to
`DEGRADED_REDIS` with that generation intact. There is no interval with
neither local nor Redis protection.

No arbitrary consecutive-probe threshold is required. A second, final
bounded Redis availability check must succeed immediately before cutover,
after subscriber and configuration preparation. One old successful probe
cannot authorize a later cutover.

## 3. Configuration prerequisite and cutover coordination

Recovery must run an immediate M3.7/M3-R3A reconciliation pass for the
currently registered tenants. At the reconciliation boundary, every locally
registered tenant must satisfy `is_tenant_reconciled(tenant_id) == true`.
Neither observed-version equality, generic `pass_success`, Redis
availability, nor subscriber readiness substitutes for this predicate.

Genuinely empty membership vacuously satisfies this **process** prerequisite:
there is no full PostgreSQL tenant-table scan, fabricated tenant, or
fabricated consumer, and no tenant is certified by an empty pass.

The M3-R3A predicate is a point-in-time local certificate, not permanent
truth. Recovery coordination must keep the completeness check valid through
the atomic cutover under an appropriate bounded coordination/barrier window.
Checking the predicate, releasing all coordination, waiting arbitrarily, and
cutting over later is forbidden. If an event, required-consumer registration,
or callback replacement invalidates a certificate before cutover, that
attempt may not cut over on the stale certificate. Reconciliation must run
again before a later cutover. The internal flow may remain
`RECOVERING_REDIS` or return to `DEGRADED_REDIS`; no particular retry loop is
mandated.

PostgreSQL read failure, current-state callback failure, M3-R3A fence
failure, pending required initialization, or any unreconciled registered
tenant prevents cutover. The mode returns/remains `DEGRADED_REDIS`, active
local state is preserved, and a later bounded attempt may retry.

## 4. Atomic process-wide cutover

Once preparation succeeds, the coordinator must perform the final bounded
Redis availability check, confirm subscriber readiness remains valid, and
confirm configuration completeness remains eligible under the same recovery
coordination. It then atomically changes the backend for **new** protection
decisions from local to Redis. The transition is process-wide: rate,
concurrency acquisition, circuit eligibility, pre-auth, and `ADMIN_TOKEN`
cannot independently recover or select different admission generations.
One process-local recovery generation controls the switch.

After that atomic point, no newly started protection operation may select
the retired local generation for admission. New operations select Redis;
pre-cutover ownership-bearing operations retain their issuing backend.
There is no partial activation or unprotected intermediate state.

If the final Redis check or subscriber prerequisite fails, no cutover occurs:
mode returns/remains `DEGRADED_REDIS`, the active local generation remains
authoritative for new decisions, no local traffic state is discarded, and no
Redis protection path is partly activated. Redis failure earlier in
`RECOVERING_REDIS` has the same no-partial-cutover outcome.

If the first Redis traffic operation **after a completed cutover** fails with
an M3-R7-classified dependency error, the completed cutover is not rolled
back into the retired local generation. Normal M3.8 fallback semantics start
a **new** active degraded generation for new decisions. The prior retired
generation remains completion-only for handles it issued.

## 5. Issuance provenance and retired generations

Any ownership-bearing handle retains the backend and local generation that
issued it. Completion routing must use that provenance, not the current
process mode. This includes local concurrency lease IDs, local circuit
normal eligibility tokens, and local circuit probe IDs.

At cutover the former active local generation becomes **retired,
completion-only**. It cannot admit new decisions; it remains process-local
and holds only bounded state needed to complete pre-cutover ownership. Its
metadata is discarded when no longer needed. Local rate and pre-auth state
carry no completion ownership and may be discarded after cutover.
“Discard local state before normal” in earlier wording means remove the old
generation from **new admission authority** at cutover, not destroy metadata
still needed by its outstanding handles. Thus traffic may report `NORMAL`
while retired-local completion metadata remains.

A local concurrency lease issued before cutover is released or renewed against
its retired issuing generation. It is never materialized into Redis, counted
as a Redis semaphore slot, or passed to a Redis lease API as a Redis-issued
lease. New acquisitions use Redis. This intentionally permits bounded old
local completion alongside new shared Redis admissions, without merging
state.

Late success or failure for a pre-cutover local circuit token/probe must not
mutate Redis circuit state. It may complete against retained local state or
become a safe stale-local no-op under existing local ownership semantics;
it is never reinterpreted as Redis-issued. Local OPEN state, failure history,
generation, incarnation, and probe ownership are not copied to Redis. New
circuit eligibility decisions use Redis.

Local rate-bucket consumption, pre-auth counters, and `ADMIN_TOKEN` counters
are not copied, merged, or replayed into Redis. At cutover, new evaluations
use existing valid Redis state; recovery neither resets/reseeds Redis buckets
nor credits or debits them from local history. These three domains switch at
the same process-wide point as concurrency and circuit admission.

A later Redis outage starts a new degraded generation (`G+1`) for new local
admissions; retired generation `G` is never reactivated for admissions. It
may continue bounded completion routing until drained. Generations are not
merged.

## 6. Process scope, startup, and health

Recovery is process-local. Replicas may simultaneously be `NORMAL`,
`DEGRADED_REDIS`, and `RECOVERING_REDIS`. Each proves its own prerequisites
before returning new traffic to shared Redis protection. There is no leader
election, global barrier, cross-instance local-state exchange, or convergence
protocol.

Startup is not recovery from durable degraded state. Local generations do not
survive restart. If Redis is unavailable when protection is first needed,
M3.8 selects `DEGRADED_REDIS`; if available, existing startup lifecycle and
configuration initialization may select `NORMAL`. A process that later
enters degraded mode uses this recovery path. A restart while degraded loses
old local counters and ownership; none is reconstructed or written to Redis.
An old handle cannot recreate terminated-process state. Existing local
lease/token expiry semantics remain the safety bound. No durable degraded
metadata is introduced.

`/health/traffic` reports the three frozen OpenAPI names. During both
`DEGRADED_REDIS` and `RECOVERING_REDIS`, it remains accepting when
conservative local protection remains available; during recovery, new
traffic is still locally protected. Successful atomic cutover reports
`NORMAL`. `/health/ready` remains a dependency diagnostic; a successful
Redis readiness probe alone does not change traffic mode.

## 7. No traffic-state writeback or merge

Recovery must not write local rate tokens, pre-auth or `ADMIN_TOKEN`
counters, concurrency ownership or lease expiry, circuit state or failure
history, probe ownership, circuit incarnation, or circuit generation into
Redis. Only **configuration** is reconciled from PostgreSQL. After cutover,
Redis traffic state is the valid shared state already present under
M3.2–M3.4; no traffic-protection reconciliation occurs.

## 8. Later implementation responsibilities

M3.9 implementation is expected to provide one process-local coordinator;
the three explicit modes; one in-flight attempt at most; generation-aware
local protection; issuance provenance; subscriber readiness; immediate
M3-R3A reconciliation; certificate-invalidation awareness; atomic
new-admission backend cutover; retired-local completion routing; and no
state merge. These are responsibilities, not prescribed Python classes or
module names.

The implementation remains out of scope for this document. This contract
does not define M3.10 metrics beyond minimal bounded recovery outcomes,
M3.11 chaos testing, M4 Normal API proxy, M5/M6 LLM execution, a global
recovery coordinator, durable degraded state, historical invalidation
replay, or global replica convergence.

## 9. Required M3.9 acceptance evidence

Later deterministic acceptance tests must cover at least:

| ID | Scenario and required observation |
| --- | --- |
| A | Redis outage enters `DEGRADED_REDIS`. |
| B | Initial successful probe enters `RECOVERING_REDIS`; new decisions still use local protection. |
| C | PostgreSQL/configuration reconciliation failure prevents cutover. |
| D | A registered tenant with a false `is_tenant_reconciled` predicate prevents cutover. |
| E | Certificate invalidation after reconciliation but before cutover prevents stale cutover. |
| F | Final Redis availability failure prevents cutover and preserves the active local generation. |
| G | Subscriber readiness failure prevents cutover. |
| H | Successful recovery makes one atomic Redis switch for all **new** protection decisions. |
| I | No newly started operation selects retired local admission state. |
| J | Pre-cutover local concurrency lease release routes to its issuing retired generation. |
| K | Pre-cutover local concurrency lease renewal routes to its issuing retired generation. |
| L | Late local circuit-token success/failure never mutates Redis circuit state. |
| M | Late local probe outcome remains local or a safe stale-local no-op. |
| N | Local rate state is not copied to Redis. |
| O | Local pre-auth and `ADMIN_TOKEN` counters are not copied to Redis. |
| P | Recovery does not reset or reseed Redis rate, circuit, or concurrency state. |
| Q | Redis failure during `RECOVERING_REDIS` preserves local admission and makes no partial cutover. |
| R | Redis failure after cutover creates a new degraded generation rather than reactivating the retired one. |
| S | A retired generation never admits new decisions. |
| T | Empty tenant membership causes no PostgreSQL full scan and does not alone block recovery. |
| U | A configuration event during recovery invalidates the certificate and prevents stale cutover. |
| V | Required-consumer registration or replacement during recovery prevents stale cutover until reconciliation. |
| W | Replicas recover at different times without a leader or global barrier. |
| X | `/health/traffic` reports `DEGRADED_REDIS`, `RECOVERING_REDIS`, and `NORMAL` at the appropriate phases. |
| Y | `/health/ready` Redis success alone does not switch traffic mode. |
| Z | No local traffic-protection state is written back or merged into Redis. |

Race-sensitive tests must use controlled synchronization primitives, not
arbitrary sleeps as correctness evidence.
