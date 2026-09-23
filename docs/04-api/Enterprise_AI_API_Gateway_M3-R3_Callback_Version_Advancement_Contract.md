# M3-R3 Callback Success and Version Advancement Contract

Status: authoritative addendum for the shared M3.6/M3.7 configuration
invalidation registry. This resolves callback failure and version-advancement
ordering without rewriting the M3-R2 PostgreSQL reconciliation contract or
historical M3.6 artifacts. M3-R3 supersedes the existing M3.6 runtime behavior
that advances a version before invoking its resource callback.

## Meaning of the observed version

The process-local tenant observed version is the highest configuration version
whose applicable invalidation or reload work has **successfully completed** in
this process. It is not merely the highest event or PostgreSQL version seen.
Both M3.6 resource-specific Pub/Sub events and M3.7 tenant-wide reconciliation
use the same applied-version state, monotonic comparison, per-tenant
coordination, and callback-success-before-advancement rule. There is no second
version store or persistent in-flight state.

## Application and failure ordering

For an M3.6 resource event, acquire per-tenant coordination and compare the
candidate with the current applied version. An equal or lower candidate is a
no-op. For a newer candidate, invoke its applicable resource callback, then
advance the version only if the callback succeeds. If that callback fails,
retain the previous version and propagate or report the failure through
internal runtime semantics. If no applicable callback exists, there is no
local invalidation work to fail, so advancement is allowed.

For M3.7, compare the PostgreSQL candidate with the same applied version. An
equal or lower candidate is a no-op. For a newer candidate, or for a registered
tenant with no local version, invoke its tenant-wide invalidation/reload
callback if one is registered. Record the candidate only after callback
success. On failure, retain the previous version (or leave it absent during
initialization). If no tenant-wide callback exists, baseline or advance the
version without a cache side effect. These rules also apply when a missing
`config_versions` row supplies effective PostgreSQL version 0; they do not
make a version-0 Pub/Sub event valid.

Callback failure must not permanently terminate the gateway background task,
must not mark the candidate applied, and must be reported with bounded,
sanitized telemetry. Do not log configuration bodies, secrets, or raw
exception details. A subsequent event or normal reconciliation cycle may
retry the failed candidate; there is no aggressive immediate retry loop.

For example, if local version 10 and PostgreSQL version 12 cause a tenant-wide
callback failure, local remains 10. The next normal reconciliation may retry
12 and, on success, advance to 12. A later candidate 13 may instead succeed
and advance directly to 13; successful replay of 12 is not required because
invalidation/reload acts on current authoritative state, not intermediate
version history.

## Per-tenant concurrency

For one tenant, the coordination boundary includes version comparison,
callback eligibility and execution, success or failure, and version
advancement. At most one version-application callback may be active for that
tenant; later candidates wait and recheck after the current operation. A
process-local per-tenant async lock or equivalent is permitted to span the
local callback. Callbacks must be bounded local cache invalidation/reload
operations, not arbitrary long-running business work. Different tenants may
proceed independently; no distributed lock is required.

This serialization covers Pub/Sub/reconciliation races. If reconciliation
candidate 12 succeeds before Pub/Sub candidate 13, the applied version moves
10 → 12 → 13 when both callbacks succeed. If 12 fails, it remains 10 until
13 succeeds, then becomes 13. It never moves backward or records a failed
callback's version. A duplicate candidate arriving during an in-flight
callback rechecks afterward: it is a no-op if the first attempt succeeded and
may retry if the first failed. No separate durable in-flight marker is needed.

Callbacks should be idempotent. A failure may leave callback side effects
whose successful completion was not recorded, so later retry is at-least-once
rather than exactly-once execution.

## Implementation and acceptance consequences

The later M3.7 implementation must correct M3.6's advance-before-callback
ordering as part of the shared registry change. It must test:

1. Local 10, callback 12 succeeds: local becomes 12.
2. Local 10, callback 12 fails: local stays 10.
3. Callback 12 fails, then callback 13 succeeds: local becomes 13 without
   replaying 12.
4. Tenant-wide callback 12 fails, then a normal reconciliation retries 12
   successfully: local becomes 12.
5. Duplicate 12 arrives during an in-flight successful 12: one effective
   application and the later duplicate is a no-op.
6. Duplicate 12 arrives during an in-flight failed 12: the later attempt may
   retry 12.
7. M3.7 candidate 12 races with M3.6 candidate 13: if 13 succeeds, final
   local version is 13 and never moves backward.
8. Initialization callback fails: local version remains absent, including
   when the effective PostgreSQL version is 0.
9. No applicable callback is registered: the candidate may baseline or
   advance immediately.

This contract introduces no database change, migration, checkpoint, or
persistent callback status. Runtime implementation and tests are deferred to
M3.7; this addendum changes documentation only.
