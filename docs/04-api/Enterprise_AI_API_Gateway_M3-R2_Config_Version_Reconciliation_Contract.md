# M3-R2 PostgreSQL Config Version Reconciliation Contract

Status: authoritative addendum for M3.7 configuration reconciliation. This
resolves gaps in the M3 Execution Pack V1.1 without rewriting that pack, the
M3-R1 invalidation-channel addendum, or historical M2 artifacts. The existing
M3-R2 token-bucket addendum governs a separate subject.

## Gaps and authoritative resolutions

The M3 Execution Pack establishes PostgreSQL `config_versions` as truth,
30-second reconciliation with bounded jitter, and tenant-cache invalidation on
version mismatch. It leaves the local tenant set, numerical jitter bound, first
pass timing, and treatment of a registered tenant without a local version
unspecified. M3.7 applies the following rules.

### Local tenant scope

Reconcile only tenants registered in this gateway process's runtime
configuration/invalidation registry. A tenant enters this ephemeral set when
tenant-scoped runtime configuration/cache state or a tenant-level invalidation
consumer is registered locally. Do not periodically scan every PostgreSQL
`config_versions` row. Query the registered tenant IDs as a set where practical.
Each instance repairs only its own local state; a database inventory worker is
not needed for V1 single-tenant or future multi-tenant deployments.

### Cadence and first pass

The base cadence is 30 seconds. Independently for each subsequent cycle, select
a uniform delay from the inclusive range 27–33 seconds (±10%, or ±3 seconds).
The jitter bound is fixed for M3.7: no database policy or additional user-facing
setting is introduced for it. Timing and jitter selection must be controllable
in tests.

Run one normal reconciliation pass immediately after the managed background
task starts. It does not block application readiness and is a no-op when no
tenants are locally registered. After the pass completes, wait for the next
jittered interval. Run at most one pass per process at a time; do not create
overlapping jobs or a busy retry loop.

### Version observation and tenant-wide invalidation

For a registered tenant with no local observed version, perform one conservative
tenant-wide invalidation/reload notification, then record the PostgreSQL
version. Do not silently establish a baseline. If no tenant-wide consumer is
registered, there is no cache side effect, but the observed version is still
initialized. Do not repeat this initialization after a local version exists.

The existing M2 config-version read contract treats a tenant without a
`config_versions` row as having effective version 0. M3.7 preserves that rule:
an existing row supplies its `version`; a missing row supplies authoritative
version 0. Missing rows are not query errors and reconciliation never inserts
one. In particular:

| Local observation | Missing PostgreSQL row means | Result |
| --- | --- | --- |
| No local version | Authoritative version 0 | One tenant-wide initialization opportunity, then local version 0. |
| Local version 0 | Authoritative version 0 | Equal-version no-op; no callback. |
| Local version greater than 0 | Authoritative version 0 | Stale observation; retain local version, with no callback or database write. |

If no tenant-wide consumer is registered for the first case, the local version
still becomes 0 without a cache side effect. That initialization is not
repeated. A stale missing-row observation may produce only bounded, sanitized
anomaly telemetry where the observability contract permits it; local runtime
state must not be written back to PostgreSQL.

For an existing local version, compare the PostgreSQL version as follows:

| PostgreSQL compared with local | Result |
| --- | --- |
| Equal | No-op; no repeated invalidation. |
| Greater | One tenant-wide invalidation; advance directly to the PostgreSQL version. |
| Lower | Stale observation; no callback and no backward movement. |

A gap from local 10 to PostgreSQL 15 produces one invalidation and observed
version 15. Versions 11–14 are not replayed. The tenant version reveals that
something changed, not which resource changed. M3.7 must not fabricate a
`resource_type` or `resource_id`.

The M3.6 resource-event path and the M3.7 tenant-wide path share one monotonic
tenant-version state and atomic comparison/advancement with callback
eligibility. If reconciliation reads 12 and Pub/Sub applies 13 before that read
is used, applying 12 is stale: local remains 13 and no tenant-wide callback for
12 runs. Tenant A's observation and callbacks cannot affect tenant B.
The internal M3.7 version-0 baseline does not change M3.6 Pub/Sub event
validation or make a version-0 Pub/Sub payload valid.

## Ownership and failure boundaries

Local registration is process-local and ephemeral. It is rebuilt as consumers
register after restart; membership is not persisted in PostgreSQL or Redis.
Every gateway instance reconciles its own registered tenants independently.
No leader election, Redis lock, or PostgreSQL advisory lock is required.

M3.7 reads only the existing `config_versions` truth. It does not increment
versions, insert a missing row, modify mutations, or write checkpoints. No
database migration is required. A PostgreSQL read failure
leaves local versions and valid runtime state unchanged, does not fabricate a
version, and is retried only on the next normal cycle. Failure telemetry must
be sanitized and bounded; it must not expose connection strings, raw exceptions,
secrets, or tenant/resource IDs as metric labels. Redis Pub/Sub availability is
not a prerequisite for a PostgreSQL reconciliation pass.

M3.6 remains the fast, resource-specific, best-effort Pub/Sub path. M3.7 is the
periodic, PostgreSQL-authoritative, tenant-wide correctness repair for missed
signals. This addendum does not make Pub/Sub durable or define M3.8 degraded
protection, M3.9 recovery sequencing, or later workload runtimes.

## Implementation and acceptance impact

- Extend the M3.6 registry architecture with local tenant registration and a
  tenant-wide invalidation hook that shares its monotonic version coordination;
  do not create a second independent version store or cache mechanism.
- Use the application's managed database engine and read only `tenant_id` and
  `version` for locally registered tenants. No new global engine, table,
  checkpoint, or Alembic migration is required.
- Manage one cancellation-friendly reconciler task in the application lifespan.
  Its first pass is immediate; later passes use independently selected 27–33
  second delays. Database errors wait for the next scheduled pass.
- Test no-local-version initialization, equal/newer/stale versions, one-step gap
  repair, duplicate cycles, tenant isolation, a Pub/Sub/reconciliation race,
  database failure recovery, immediate startup behavior, and clean shutdown.
  Include missing-row cases: absent local version initializes to 0 with one
  tenant-wide opportunity; local 0 is a no-op; local 3 remains 3 with no
  callback or database write; and absent local version with PostgreSQL 4
  initializes to 4 with one tenant-wide opportunity.
  Use real PostgreSQL to prove repair after a missed local Pub/Sub observation;
  keep timing and jitter tests deterministic.
