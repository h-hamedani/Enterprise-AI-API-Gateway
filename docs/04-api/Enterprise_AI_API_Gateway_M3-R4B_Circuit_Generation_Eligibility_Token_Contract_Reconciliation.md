# M3-R4B Circuit Generation and Eligibility Token Contract Reconciliation

Status: authoritative M3-R4 addendum for normal-outcome eligibility. This is a
contract only; it does not implement M3.4.

## The stale-CLOSED-cycle race

A normal request admitted in CLOSED period N can finish after other requests
take the circuit through CLOSED -> OPEN -> HALF_OPEN -> CLOSED. Checking only
`state == CLOSED` at outcome time would incorrectly apply that old result to the
new CLOSED period. Thus a state-only CLOSED check is explicitly insufficient.

## Internal identity and admission

The Redis-owned minimal circuit metadata contains `state`, `incarnation_id`,
`generation`, `open_until_ms`, `probe_owner`, and `probe_expires_at_ms` as
applicable. Failure history may be separate. The existing circuit identity and
key pattern remain `gw:v1:cb:{tenant}:{target_kind}:{target_id}:{dimension}`;
auxiliary keys share its tenant hash tag. Callers cannot supply arbitrary Redis
keys as circuit identities.

`incarnation_id` is a fresh, opaque, random UUID for each materialization after
missing state or state loss; timestamps alone are not sufficient. `generation`
is an exact positive Redis integer, initially 1, monotonically increasing
within that incarnation. Missing state is conceptually CLOSED, generation 1,
but CLOSED admission must atomically materialize metadata with `state = CLOSED`,
the new `incarnation_id`, and `generation = 1` before returning a token. A
concurrent initializer must return the winning persisted incarnation, never
its unpersisted candidate. Eligibility observation and initialization are one
Redis-side atomic operation.

Normal CLOSED admission returns an internal `NormalEligibilityToken` containing
at least `incarnation_id` and `generation`. The token is bound to the exact
circuit identity, by a typed call context, identity in the token, or an API
shape that makes cross-target use impossible. M3-R4 implementation must select
and test one of those equivalent shapes. It is not a public API field and does
not expose Redis keys. The token captured at admission is passed to the
normal-result operation; it is not reconstructed from current state.

## Transition and outcome rules

| Transition | Generation | Incarnation |
| --- | --- | --- |
| CLOSED -> OPEN | unchanged | unchanged |
| OPEN -> HALF_OPEN | unchanged | unchanged |
| Expired HALF_OPEN owner replaced | unchanged | unchanged |
| HALF_OPEN probe failure -> OPEN | unchanged | unchanged |
| Active-owner probe success -> CLOSED | increment exactly once | unchanged |
| Missing/lost state materialized | reset to 1 | fresh random UUID |

Normal `record_success` applies only if current state is CLOSED **and** both
token fields match current metadata. If eligible, it clears failure history and
returns `applied = true`. Normal `record_failure` has the same eligibility
predicate; if eligible, it prunes the sliding window, inserts the failure,
evaluates the configured threshold, may transition to OPEN, and returns
`applied = true`. A mismatch or non-CLOSED state returns `applied = false` with
no mutation, including no failure event, no history clear, and no transition.
Validation and mutation are a single Redis-side atomic operation, never a
client read-generation-then-write-outcome sequence.

This admits a same-incarnation, same-generation result while still CLOSED. It
rejects the old token in OPEN, in HALF_OPEN, and after successful recovery to
CLOSED because that recovery increments generation. It also rejects an old
token after Redis restart, eviction, or other state loss because rematerialized
state has a fresh incarnation even if generation returns to 1. Redis state
loss remains fail-open as the previously frozen missing-state behavior; the
incarnation protects attribution of late outcomes, not admission availability.

HALF_OPEN results instead require the exact active, unexpired `probe_id` owner.
The normal eligibility token does not grant probe ownership and must not be
used to apply a probe outcome. Probe success atomically clears probe owner,
probe expiry, open deadline, and failure history while entering CLOSED at
`generation + 1`. Probe failure returns to OPEN without incrementing.

## Exact integer and overflow

V1 uses exact positive integers in Redis Lua's safe integer range, through
`9007199254740991` (`2^53 - 1`). Every parsed generation must be validated as
an integer in `[1, 9007199254740991]`, with no floating-point rounding or
silent coercion. On an active-owner probe success when generation is already at
that maximum, the operation must return a typed internal circuit-state error
before any transition or partial mutation. It must not wrap, reset to 1 under
the same incarnation, or claim successful closure. Recovery requires an
explicitly defined state reinitialization path with a fresh incarnation;
M3-R4 implementation must keep this exceptional path fail-closed until that
path is designed. Ordinary state loss already creates a fresh incarnation.

## Configuration, security, and scope

M3-R4A's validated deployment `CircuitConfig` remains authoritative. Neither
generation nor incarnation is configurable; existing absolute deadlines and
future-transition configuration semantics are unchanged. Generation and
incarnation are Redis-owned ephemeral coordination state, not PostgreSQL
columns; no Alembic migration is needed. There is no Control Plane or public
HTTP schema change. Do not use the incarnation ID, generation, eligibility
token, or probe ID as metric labels, and do not log opaque ownership tokens by
default. The normal and probe mechanisms above are the M3-R4 handoff; this
artifact adds no circuit Python, Lua, provider invocation, or HTTP integration.
