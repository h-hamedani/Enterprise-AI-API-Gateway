# M3-R2 Atomic Token Bucket Algorithm Contract Reconciliation

Status: authoritative addendum to the M3 Execution Pack V1.1 for the M3.2
token-bucket algorithm only. It does not implement the algorithm or change the
physical database contract.

## Original ambiguities and precedence

The frozen artifacts require a Redis-backed token bucket, Redis server time,
atomic mutation, cumulative policy layers, and the maximum retry delay from all
rejecting layers. They did not define cross-layer atomicity, rejected-request
consumption, numeric representation, time precision, retry rounding, exact TTL,
or whether layer order changes mutation semantics.

This addendum freezes those details. It supersedes only unspecified M3.2
algorithm semantics; existing schema, scope vocabulary, and later-package
boundaries remain unchanged.

## Resolved-policy input boundary

M3.2 accepts already-resolved enabled rate policies. Each typed input contains
only `policy_id`, `tenant_id`, `scope_type`, `scope_id`,
`requests_per_window`, and `window_seconds`. M3.2 validates these values but
does not discover policies in PostgreSQL or resolve request applicability.
M3.5, M4, and M5 own request-specific policy resolution and HTTP integration.

An empty policy set has the neutral result `allowed = true` and
`retry_after_ms = 0`. M3.2 does not synthesize a default policy.

## Key construction and deterministic order

Every key is constructed internally from typed values using:

`gw:v1:rl:{tenant}:{scope_type}:{scope_id}`

All policies in one evaluation must have the same `tenant_id`. The `{tenant}`
hash tag intentionally places their keys in one Redis Cluster hash slot so one
multi-key script remains possible. Callers cannot provide arbitrary Redis key
text.

Script input follows the frozen workload layer order:

- Normal Data Plane: `API_KEY`, `ROUTE`, `SERVICE`
- LLM Data Plane: `API_KEY`, `LLM_ALIAS` and/or `LLM_MODEL`, `PROVIDER_TARGET`
- Authenticated Control Plane: `ADMIN_TOKEN`

Entries at the same logical position are ordered by `policy_id`. Ordering makes
script input, rejection metadata, and bounded telemetry deterministic. It gives
no policy mutation priority and cannot change consumption.

## One atomic all-or-nothing evaluation

All applicable policies for one request are evaluated in one Redis-side atomic
Lua invocation, or a semantically equivalent single Redis atomic operation,
across every applicable policy key.

- If all buckets have sufficient capacity, one request cost is deducted from
  every bucket.
- If any bucket rejects, no request-cost units are deducted from any bucket.

Sequential independent mutations are forbidden because they can drain an
earlier bucket when a later bucket rejects, making rejected-request consumption
depend on layer order.

Refilled and clamped state, with `last_refill_ms = now_ms`, is persisted for all
evaluated buckets even when the request is rejected. This persistence performs
no request-cost deduction.

## Redis time and exact integer representation

Redis `TIME` is the only shared clock authority. The script converts its
seconds and microseconds to integer milliseconds:

```text
now_ms = seconds * 1000 + floor(microseconds / 1000)
```

For each policy:

```text
C = requests_per_window
W = window_seconds * 1000
capacity_units = C * W
one_request_cost_units = W
```

One token equals `W` integer units and the exact refill rate is `C` units per
millisecond. Redis state stores only `available_units` and `last_refill_ms`.
Binary floating-point token balances are forbidden.

A missing key begins full:

```text
available_units = capacity_units
last_refill_ms = now_ms
```

The first allowed request therefore deducts `W` units.

## Refill and elapsed time

```text
elapsed_ms = max(0, now_ms - last_refill_ms)
effective_elapsed_ms = min(elapsed_ms, W)
refill_units = effective_elapsed_ms * C
available_units = min(capacity_units, previous_units + refill_units)
```

Clamping elapsed time to one complete refill window does not change the bucket
result: after `W` milliseconds even a fully depleted bucket is full. The clamp
also bounds exact integer arithmetic.

To avoid an overflowing intermediate sum, the script compares `refill_units`
with `capacity_units - previous_units`; it assigns capacity when refill reaches
that deficit and adds only otherwise.

## Decision and retry delay

A policy is sufficient when `available_units >= W`. The overall request is
allowed only when every evaluated policy is sufficient.

For each rejecting policy:

```text
deficit_units = W - available_units
policy_retry_after_ms = (deficit_units + C - 1) // C
```

This is exact ceiling division. Floor rounding and floating-point division are
forbidden. Only rejecting policies contribute. The overall value is:

```text
retry_after_ms = max(policy_retry_after_ms for every rejecting policy)
```

Allowed and empty-policy results have `retry_after_ms = 0`. Deterministically
ordered rejection metadata may identify bounded scope types, but never Redis
keys or raw tenant, scope, or policy identifiers in public errors or metric
labels.

## Expiry and cleanup

Every persisted bucket receives a millisecond TTL:

```text
ttl_ms = 2 * W
```

The TTL is refreshed on every evaluation that persists state, including a
rejection that persists refilled state. Redis expiry is the only M3.2 cleanup
mechanism; no cleanup worker is introduced.

## Exact numeric safety bound

Redis 7 Lua numbers are binary64 values, whose exact consecutive integer range
ends at `2^53 - 1` (`9007199254740991`). PostgreSQL `integer` columns alone do
not prevent `C * W` from exceeding this limit.

M3.2 must reject a resolved policy before script execution unless:

```text
C > 0
W > 0
C * W <= 9007199254740991
```

This is a runtime policy-validation error, not a Redis dependency failure.
The elapsed clamp guarantees `refill_units <= capacity_units`. The guarded
addition avoids values above capacity. The retry numerator remains exact under
the same bound. Decimal strings passed through Lua arguments must be parsed and
validated as integers; scientific notation and fractional values are invalid.

No database constraint or migration is added. A future contract may choose a
stricter persistence constraint, but M3.2 correctness does not depend on one.

## Redis failure, restart, and script cache

Redis unavailability produces a typed runtime dependency failure. M3.2 does
not fail open, create a local bucket, or apply the 25 percent degraded factor;
M3.8 owns degraded behavior.

Bucket state is ephemeral and may disappear after Redis restart. A missing key
then follows the frozen full initial-state rule. If implementation uses
`EVALSHA`, `NOSCRIPT` permits one bounded script reload and one re-evaluation.
M3.2 adds no recovery barrier or state merge.

## Security, telemetry, and database impact

Redis bucket values contain only `available_units` and `last_refill_ms`.
Telemetry dimensions are limited to limiter operation, outcome, and bounded
`scope_type`. Tenant UUIDs, scope or policy IDs, Redis keys, token values, IPs,
credentials, URLs, and exception text are forbidden as metric labels or logs.

PostgreSQL remains authoritative for policy configuration. This reconciliation
adds no table, column, constraint, migration, public endpoint, HTTP 429 wiring,
Lua script, runtime limiter, policy loader, degraded fallback, or later M3
package behavior.
