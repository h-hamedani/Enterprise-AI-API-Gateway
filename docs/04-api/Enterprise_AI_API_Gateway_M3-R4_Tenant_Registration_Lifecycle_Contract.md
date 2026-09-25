# M3-R4 Tenant Registration Lifecycle Contract

Status: authoritative addendum for M3.7 process-local reconciliation
membership and late tenant-wide callback initialization. This resolves the
registration-lifetime gap left by M3-R2 and composes with M3-R3's shared
applied-version and per-tenant callback-success rules. It does not rewrite
either earlier addendum or change the M3.6 Pub/Sub event contract.

## Membership and registration lifetime

Once a tenant enters this process's reconciliation set, membership lasts until
process shutdown. It is process-local, ephemeral, and monotonic over that
lifetime. M3.7 V1 has no reconciliation-membership unregister API: temporary
cache replacement, callback replacement, or consumer churn must not silently
remove a tenant from coverage. True dynamic tenant unload requires a separate
future contract. Membership is never persisted in PostgreSQL or Redis.

An M3.6 resource-specific callback registration for tenant T means this
process holds tenant-scoped runtime/configuration state for T and therefore
adds T to M3.7 reconciliation membership. This remains true without a
tenant-wide callback. Resource registration does not itself create such a
callback or cause historical Pub/Sub events to be replayed. Any initial load
needed by that resource consumer belongs to its own registration/load
lifecycle. Coarse reconciliation must neither fabricate resource IDs nor
invoke resource-specific callbacks for unknown missed events. If no
tenant-wide callback exists, M3-R2 permits baselining or advancing the shared
version without a tenant-wide cache side effect.

Registering a tenant-wide invalidation/reload callback also adds T to
membership for the process lifetime. On process restart, the registry starts
empty; runtime consumers register anew, tenant-wide callbacks start pending
initialization, and PostgreSQL remains authoritative. No persistent
membership or consumer checkpoint is needed.

## Late tenant-wide callback initialization

A newly registered tenant-wide callback is a new consumer even if M3.6
resource events already advanced the shared tenant applied version. It must
receive one initialization/reload opportunity against authoritative
PostgreSQL state; it must not wait for a higher version. Registration itself
does not query PostgreSQL or synchronously invoke the callback. It marks the
callback pending initialization and ensures tenant membership. The next
reconciliation opportunity performs the initialization after its normal
PostgreSQL version read.

While initialization is pending, reconciliation invokes that callback even
when the database version equals or is lower than the shared applied version.
On callback success, clear pending initialization and set the shared version
to `max(current_applied, database_version)`, treating an absent local version
as needing a baseline. On callback failure, keep pending initialization and
leave the shared version unchanged; the next normal reconciliation cycle may
retry. M3-R3's success-before-advancement and at-least-once/idempotent
callback rules still apply. Once initialization succeeds, an unchanged later
cycle does not invoke the initialization callback again.

For example, if shared version 7 already exists when a tenant-wide callback
registers and PostgreSQL also reports 7, invoke the callback once; on success
the shared version stays 7. If shared version 9 exists but PostgreSQL reports
8, still initialize the new consumer once, but retain version 9. The callback
performs its normal current-state reload; the shared version never rolls
back. A missing `config_versions` row supplies effective database version 0
under M3-R2: successful late initialization baselines an absent shared
version to 0 or preserves any existing version greater than 0.

If the registry supports replacement of a tenant-wide callback object or
handler, replacement is a new consumer registration and marks initialization
pending again. The replacement must not be presumed to inherit the previous
callback's cache state. This rule does not require adding a separate
replacement API when none exists.

## Coordination boundary

Registration or replacement, pending-initialization state, resource-event
application, reconciliation callbacks, and version advancement for a tenant
must compose under the same process-local per-tenant coordination domain
defined by M3-R3. A late registration must not be lost behind an equal-version
check or race with an in-flight callback into an inconsistent state. Different
tenants remain independent; no distributed lock is introduced.

## Later implementation acceptance

M3.7 tests must establish:

1. Resource-specific registration adds its tenant to reconciliation membership,
   without creating a tenant-wide callback.
2. Tenant-wide registration adds membership and marks that callback pending
   initialization.
3. With shared version 7 and PostgreSQL version 7, a late tenant-wide callback
   initializes once; equal-version deduplication does not skip it.
4. If that initialization fails, it stays pending and the next normal cycle
   retries without advancing the shared version.
5. With shared version 9 and PostgreSQL version 8, late initialization runs
   once and shared version remains 9.
6. With a missing PostgreSQL row, late initialization uses effective version
   0: an absent shared version becomes 0 on callback success; an existing
   shared version 5 remains 5.
7. After initialization success, an unchanged later cycle does not repeat the
   callback.
8. Membership persists for the process lifetime despite consumer churn; a
   fresh process rebuilds it only through new registrations.
9. If callback replacement is supported, the replacement receives its own
   initialization opportunity.

This addendum changes documentation only. It introduces no database change,
migration, membership table, consumer checkpoint, or persistent callback
initialization state. Runtime code and tests remain deferred to M3.7.
