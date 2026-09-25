# M3-R5 Reconciler Activation Boundary Contract

Status: authoritative addendum for the M3.7 production activation boundary.
It clarifies that M3.7 ships as production-wired reconciliation infrastructure
without requiring an M4 or later business configuration-cache consumer. It
does not change the M3-R2 PostgreSQL reconciliation, M3-R3 applied-version, or
M3-R4 registration-lifecycle rules.

## Infrastructure-only production activation

M3.7 shall ship in M3 with its registry, PostgreSQL lookup, reconciliation
logic, and managed background task wired into the production application.
Production reconciliation membership may initially be empty. This is a valid
M3 boundary, not an incomplete implementation: M3 establishes reusable
runtime configuration infrastructure, while later workload milestones supply
their actual configuration-cache consumers.

The application starts one reconciler task and its first pass immediately,
without blocking readiness. If membership is empty, that pass succeeds as a
no-op, makes no `config_versions` full-table query, and invokes no callback.
The task remains healthy and continues its normal reconciliation cadence.
When a legitimate consumer registers later in the same process, M3-R4 adds
its tenant to membership; subsequent passes include that tenant without an
application restart.

Membership must arise only through M3-R4 registration. Do not register a
bootstrap tenant merely because it exists, enumerate all PostgreSQL tenants
or `config_versions` rows, create an artificial default tenant, or introduce
a fake cache consumer. In particular, do not install a no-op tenant-wide
production callback solely to populate the registry or claim to invalidate
caches that do not yet exist. No production cache consumer means no
production callback.

## Acceptance with controlled consumers

Empty production membership does not weaken M3.7 acceptance. Unit and
integration tests may register controlled test-only tenant-wide
callbacks/caches through the real registry interface. These fixtures are
not production business components. Acceptance must prove the complete
callback, applied-version, pending-initialization, race, and later-registration
semantics of M3-R2, M3-R3, and M3-R4.

Real PostgreSQL missed-event evidence must use the real registry and
reconciler: register a test tenant-wide consumer, establish local applied
version N, persist authoritative PostgreSQL version N+K, deliberately omit
the corresponding Pub/Sub event for that registry, then reconcile. The
consumer must run once, the shared local version must advance to N+K, and
intermediate events must not be replayed.

An explicit production-style empty-membership test must show that the
immediate first pass completes, no full-table `config_versions` scan or
fabricated callback occurs, the background task remains alive, and a later
registration enters reconciliation scope for a following pass.

M3.7 may pass with zero production tenants registered only when its
production lifecycle, empty-set behavior, registration interfaces,
PostgreSQL lookup and reconciliation, callback/version/race tests, real
PostgreSQL missed-event repair, and later registration are all verified.
No later-milestone consumer may be fabricated to meet this criterion.

## Later consumer boundary and database impact

Future Normal API or LLM runtime components may register their real
tenant-scoped cache/reload consumers through the M3 registry interfaces
without redesigning M3.7. They inherit M3-R2 reconciliation, M3-R3
success-before-advancement, and M3-R4 registration and late-initialization
semantics. M3.7 does not implement Normal API route/service or LLM
provider/model/alias caches, a Data Plane resolver, a business configuration
loader solely for reconciliation, or other M4/M5/M6 runtime behavior.

This activation boundary requires no migration, default membership row,
startup tenant enumeration, or durable consumer registry.
