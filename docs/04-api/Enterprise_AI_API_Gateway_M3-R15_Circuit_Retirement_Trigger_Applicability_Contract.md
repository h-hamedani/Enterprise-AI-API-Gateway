# M3-R15 — Circuit Retirement Trigger Applicability Contract

Status: FROZEN. Documentation only; M3-R14 cleanup implementation is separate.

This addendum qualifies when M3-R14's event-driven retirement fast path applies
to the current M3 product surface. It does not change the circuit algorithm,
Redis key format or TTL rules, M3-R4B fencing, PostgreSQL schema, OpenAPI,
routing eligibility, or resource-status semantics.

## 1. Current product-surface ruling

The current M3 Control Plane has **no mutation designated as a permanent
circuit-identity retirement trigger**. Its status mutations are reversible:
`ACTIVE` and `DISABLED` may be changed through existing APIs. No current
permanent delete/detach mutation for circuit-bearing resources or their
required relationships is exposed. Consequently, M3-R14's event-driven
cleanup fast path has **no current producer or hook**. This is an
applicability ruling, not permission to omit a hook for a known retirement
mutation.

`DISABLED` alone MUST NOT classify a resource or relationship as
lifecycle-retired or authorize circuit-key deletion. Existing APIs permit
`DISABLED -> ACTIVE`; deleting state on disablement would discard valid
coordination state for an identity that may become routable again. Thus
`DISABLED` means **retain circuit Redis state** for cleanup purposes. The
question is not whether traffic is enabled now, but whether authoritative
PostgreSQL still represents the exact identity such that it can validly
exist again without a permanent identity-recreation event. A temporarily
non-routable but still authoritative identity is retained.

Do not attach cleanup to `ACTIVE -> DISABLED`, route/service/model/provider
disablement, dependency failure, OPEN/HALF_OPEN state, or local degraded
mode. Do not invent a fake retirement mutation or post-commit hook merely
to satisfy M3-R14's event-driven acceptance item.

## 2. Current cleanup authority and scope

For the current M3 implementation, M3-R14 orphan self-healing consists of
the **bounded reconciliation sweep only**. This does not repeal M3-R14's
event-driven rule; no existing mutation currently satisfies its permanent
retirement trigger predicate.

The sweep may classify an exact circuit identity as `RETIRED` only when a
successful authoritative PostgreSQL lookup proves it can no longer be
constructed from the required resource/relationship graph. The existing
schema and repository relationships determine the exact predicate:

- Normal API: the exact tenant-owned route and owning service relationship;
- LLM: the exact tenant-owned model and owning provider-target relationship.

Absence of a required authoritative resource row or ownership/association
relationship MAY prove retirement only if the queried relation is the
authoritative source, the lookup completed successfully, and absence is not
merely a transient query failure. If all required rows/relationships still
exist but one or more are `DISABLED`, classification is **not `RETIRED`**;
retain the keys. Current traffic eligibility is a separate routing question.
Database failure or uncertainty is `AUTHORITY_UNAVAILABLE`, never
`RETIRED`, and causes no Redis deletion.

M3-R14's double-validation boundary remains mandatory: evaluate candidate
authority, then revalidate current PostgreSQL authority immediately before
deleting the exact key pair. Only a final `RETIRED` result authorizes
deletion. A candidate that becomes authoritative again between discovery
and final validation must be retained. The sweep continues to obey M3-R14's
bounded SCAN, strict parsing, idempotency, telemetry, and security rules.

Because disablement does not delete state, `DISABLED -> ACTIVE` requires
neither circuit-state reconstruction nor a new incarnation solely due to the
status cycle. Existing Redis coordination state remains subject to normal
M3-R4/M3-R4B transitions. If an identity was truly retired because a required
resource or relationship was removed, but the same UUID tuple later becomes
authoritative again, final current PostgreSQL state wins: do not delete if
it is authoritative before final validation. If state was already deleted
before legitimate recreation, M3-R4B's missing-state rule creates a fresh
incarnation. No tombstone is introduced.

The bounded sweep discovers historical orphan keys, missed cleanup from
older deployments, stale-replica recreation, and identities removed through
authoritative changes not exposed as current Control Plane permanent
retirement mutations. M3-R15 does **not** authorize application reliance on
unsupported direct database mutations. If a historical orphan is present,
the sweep evaluates current PostgreSQL authority regardless of how the
orphan arose; it need not reconstruct the historical deletion event.

## 3. Future permanent-retirement producer

If a future version adds a committed mutation that permanently removes a
circuit-bearing resource or required relationship—such as deletion,
detachment, or another explicitly irreversible retirement operation—that
mutation MUST integrate M3-R14's post-commit event-driven cleanup fast path.
Its contract MUST explicitly define retirement semantics, transaction commit
boundary, affected exact circuit identities, and the post-commit cleanup
trigger **before implementation**. Do not infer permanence from a mutation's
name or a status value. A Redis cleanup failure must not roll back its
committed PostgreSQL/business change; reconciliation remains the repair
path.

M3-R14's acceptance item that event-driven cleanup happens only after the
commit boundary is **not applicable** to the current M3 runtime because no
permanent-retirement mutation exists. It becomes mandatory when such a
producer is introduced. Do not create a test-only mutation to claim this
evidence.

## 4. Current acceptance and release boundary

Current M3-R14 implementation MUST test at least:

| ID | Required evidence |
| --- | --- |
| A–B | ACTIVE and DISABLED exact identities are retained. |
| C–D | A missing required authoritative resource or relationship may classify a candidate as RETIRED after a successful authoritative lookup. |
| E | Authority query failure retains circuit state. |
| F | Candidate RETIRED on initial evaluation but ACTIVE on final revalidation is retained. |
| G | RETIRED on both evaluations deletes the exact primary/failure key pair. |
| H–I | Historical orphans are eventually reclaimed; historical ACTIVE/DISABLED identities are retained. |
| J | No event-driven cleanup hook is attached to reversible status patches. |
| K–L | No circuit TTL is added; all remaining M3-R14 scan, parser, idempotency, and security requirements are met. |

For the current product surface, Final M3 GO requires implementation evidence
for this ruling: retention of ACTIVE and DISABLED identities, reclamation of
truly absent authoritative identities, final revalidation against
reactivation/recreation, and an effective bounded historical-orphan sweep.
Evidence for a nonexistent permanent-retirement API is **not** required. If
such an API is added before M3 release, its event-driven fast path joins the
release gate.

M3-R15 requires no schema, migration, OpenAPI, circuit state-machine,
key-format, TTL, incarnation/generation, routing-eligibility, or status
semantics change. It resolves only M3-R14 retirement-trigger applicability.
