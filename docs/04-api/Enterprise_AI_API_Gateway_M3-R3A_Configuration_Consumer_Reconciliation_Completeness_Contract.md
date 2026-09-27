# M3-R3A — Configuration Consumer Reconciliation Completeness Contract

Status: FROZEN
Precedence: This addendum supersedes conflicting behavior in M3-R2, M3-R3,
and M3-R4 only where explicitly stated below.

## 1. Purpose

This contract separates configuration version observation from configuration
consumer completeness.

A tenant's locally observed configuration version MUST NOT be interpreted as
proof that every required registered consumer has applied authoritative
current state.

This contract closes the case where:

1. a required consumer callback for version N+1 fails;
2. another consumer callback for version N+2 succeeds;
3. the tenant observed version advances to N+2;
4. the first consumer remains stale.

## 2. Observed Version

The existing tenant configuration version remains the single monotonic
observed-version store.

A later successfully observed event MAY advance the tenant observed version
even when reconciliation_required is true.

No second configuration version store is introduced by this contract.

The existing `applied_version` accessor is a historical name for observed
progress, not a completeness certificate.

Observed version represents version/event progress only.

It does not represent consumer reconciliation completeness.

## 3. Required Consumer

A configuration consumer participating in tenant correctness MUST be
registered as a required consumer.

Each required consumer MUST expose a current-state reconciliation capability.

The capability MUST be able to reconcile that consumer for a tenant from
authoritative current state without requiring:

- a fabricated resource identifier;
- reconstruction of an unknown missed event;
- replay of configuration event history.

A resource-specific event callback and a current-state reconciliation
callback are distinct capabilities.

## 4. Current-State Reconciliation Callback

Each required consumer MUST provide an operation equivalent to:

    reconcile_current_state(tenant_id)

The implementation MAY use its own authoritative repositories or loaders,
but completion MUST mean that the consumer has refreshed itself to the
authoritative current configuration state for that tenant.

The reconciliation coordinator MUST NOT invent resource IDs or synthesize
resource-specific invalidation events.

## 5. Reconciliation Required

Each locally registered tenant MUST maintain a process-local
reconciliation_required condition.

If processing of a required consumer callback fails:

    reconciliation_required = true

A later successful event-specific callback MUST NOT clear
reconciliation_required.

Advancement of observed_version MUST NOT clear reconciliation_required.

If a newly accepted event skips a version (observed N, accepted version
greater than N+1), reconciliation_required MUST become true before its
resource-specific callback runs. Successful processing of that event MUST
NOT clear the condition. A duplicate or stale event MUST NOT clear it.

## 6. Clearing Reconciliation Required

reconciliation_required MAY be cleared only after a successful
current-state reconciliation pass covering every required consumer that is
part of the tenant's reconciliation membership for that pass.

Every required consumer reconciliation callback in that pass MUST succeed.

The pass MUST fence current-state work with two authoritative PostgreSQL
version reads for the tenant: one before callbacks and one after all callbacks.
The tenant coordination lock covers both reads and the stable callback
snapshot, so event delivery and consumer registration cannot interleave with
that pass. Only equal start/end versions may certify completion. A changed or
failed end read leaves the tenant uncertified, dirty, and pending consumers
pending; a subsequent pass must retry. A PostgreSQL version below the already
observed version cannot roll observed progress back or certify completeness.

If any required consumer fails:

- the pass fails;
- reconciliation_required remains true;
- no completeness claim may be emitted.

Partial success MUST NOT clear reconciliation_required.

## 7. Equal-Version Reconciliation

This section explicitly supersedes the unconditional equal-version no-op
rule in M3-R2.

When:

    postgres_version == local_observed_version

and either:

    reconciliation_required == true

or:

    required consumer initialization is pending

the reconciliation pass MUST NOT return `up_to_date` solely because the
versions are equal.

It MUST execute the required current-state reconciliation work.

Equal-version no-op remains valid only when:

- reconciliation_required is false; and
- no required consumer initialization is pending; and
- the tenant already holds a successful fenced completeness certificate.

Even that no-op requires the PostgreSQL end read to confirm that the version
did not change during the pass.

## 8. Required Consumer Registration

This section qualifies the M3-R4 resource-only registration rule.

A required resource-specific consumer MAY continue to register without a
tenant-wide event callback.

However, it MUST provide its own current-state reconciliation capability.

Registration of a new required consumer creates pending initialization for
that consumer until its current-state initialization succeeds.

A resource-specific consumer MUST NOT be initialized by fabricating a
resource ID or replaying an unknown missed event.

## 9. Reconciliation Membership

A reconciliation pass MUST operate over a stable snapshot of the required
consumer membership for that tenant at the beginning of the pass.

Consumers registered after that snapshot MUST enter pending initialization
and MUST NOT be silently certified by the in-progress pass.

They MAY be initialized by a subsequent pass.

## 10. Tenant Reconciled Predicate

A tenant is reconciled only when all of the following are true:

1. authoritative PostgreSQL version/state lookup succeeded;
2. the required version relationship is satisfied;
3. reconciliation_required is false;
4. no required consumer has pending initialization;
5. all required consumers in the applicable reconciliation pass completed
   current-state reconciliation successfully.

Version equality alone is insufficient.

A reconciler result such as `pass_success` MUST NOT imply this predicate
unless all conditions above are satisfied.

The process MUST expose `is_tenant_reconciled(tenant_id)` separately from
`observed_version(tenant_id)`. Its certificate is process-local boolean state
associated with the most recent successful fenced pass, not another numeric
version authority. A new accepted event, consumer registration/replacement,
failed callback, failed PostgreSQL lookup, or fence mismatch invalidates the
certificate. The predicate is point-in-time local evidence, not a promise
that PostgreSQL cannot change after the pass.

## 11. Database Failure

If the authoritative PostgreSQL read fails:

- observed version MUST NOT be fabricated or advanced;
- reconciliation_required MUST NOT be cleared;
- pending initialization MUST NOT be cleared;
- no tenant may be certified reconciled by that pass.

Existing conservative state remains unchanged.

## 12. Empty Membership

The legitimate empty-membership behavior from M3-R5 remains valid.

If there are no locally registered tenants or required consumers:

- the reconciler MUST NOT scan the full configuration table;
- it MUST NOT fabricate tenant or consumer state;
- it MAY complete as an empty successful reconciliation pass.

An empty pass certifies no tenant.

## 13. Process Scope

All reconciliation completeness state defined here is process-local.

This contract introduces no:

- leader election;
- replica coordination;
- cross-process consumer registry;
- global convergence guarantee;
- Redis state merge;
- event-history replay.

## 14. M3-R8 Dependency

Redis recovery and cutover MUST NOT rely solely on:

    postgres_version == local_observed_version

or on the current generic `reconcile_once() == pass_success`.

M3-R8 MAY use the reconciled predicate defined in this contract only after
the required-consumer reconciliation behavior is implemented and tested.
