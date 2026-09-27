# M3-R11 — Unknown Circuit Completion Result Contract

Status: FROZEN. Documentation only; M3.9 implementation remains separate.

## 1. Boundary and precedence

M3-R10 requires a provenance-aware M3.9 circuit facade to call neither Redis
nor a guessed local generation for an unknown completion handle. With no
backend consulted, no authoritative resulting circuit state exists. This
addendum defines the facade's truthful result for that case and resolves the
explicit M3-R10 stop condition. It does not alter M3-R9 recovery/cutover or
M3-R10 provenance decisions.

## 2. Preserve primitive results

`RedisCircuitStore` and `LocalCircuitStore` retain the M3.4 primitive result:

```text
OutcomeResult:
    applied: bool
    resulting_state: CircuitState
```

A primitive completion operates against a known backend and returns a
concrete state. Primitive token formats, APIs, result shape, and tests are
not changed. In particular, primitive `resulting_state` is not made nullable.

## 3. Distinct orchestration result

The provenance-aware M3.9 facade exposes a separate runtime-internal result
equivalent to:

```text
CircuitCompletionResult:
    applied: bool
    resulting_state: CircuitState | None
    disposition: CircuitCompletionDisposition
```

The minimum dispositions are `APPLIED`, `STALE`, and `UNKNOWN_HANDLE`.
Exact Python names and validation mechanics may differ. Dependency failures
continue to use M3-R7 exception/error behavior; they are not converted to a
disposition. This is not an OpenAPI or HTTP result schema.

| Input to facade | Backend action | Orchestration result |
| --- | --- | --- |
| Known provenance; primitive returns `OutcomeResult(True, S)` | Route to issuing backend | `applied=True`, `resulting_state=S`, `disposition=APPLIED` |
| Known provenance; primitive returns `OutcomeResult(False, S)` | Route to issuing backend | `applied=False`, `resulting_state=S`, `disposition=STALE` |
| Unknown or unusable provenance | Call neither backend | `applied=False`, `resulting_state=None`, `disposition=UNKNOWN_HANDLE` |

For `UNKNOWN_HANDLE`, `None` means **no authoritative circuit state was
consulted or changed**. It does not imply `CLOSED`, `OPEN`, `HALF_OPEN`, or
`DEGRADED_HALF_OPEN`.

## 4. Unknown and missing-generation handling

Unknown provenance includes malformed wrappers, bare legacy raw normal
tokens or probe UUIDs presented where wrappers are required, pre-restart
handles, and an unusable `LOCAL(G)` handle whose safely retired generation G
no longer exists. The facade must call neither Redis nor a guessed local
generation; perform no circuit mutation; and return `UNKNOWN_HANDLE` as
defined above.

A missing generation is not recreated. Its raw token cannot fall back to
Redis or the current active local generation. After process restart, old
process-local handles have no valid current-process provenance and are not
reconstructed; they follow the same unknown-handle rule.

No unknown path may fabricate a circuit state from process mode, another
generation, unrelated cached state, or Redis health. It must not substitute
any of the four existing `CircuitState` values merely to fill a required
primitive result field.

## 5. Type and production-caller invariants

Construction of an internally inconsistent orchestration result must be
rejected:

```text
disposition in {APPLIED, STALE} => resulting_state is not None
disposition == APPLIED         => applied is True
disposition == STALE           => applied is False
disposition == UNKNOWN_HANDLE  => applied is False and resulting_state is None
```

Production M3.9 circuit completion callers consume
`CircuitCompletionResult`; they do not force a nullable state into primitive
`OutcomeResult`. Primitive tests may continue asserting `OutcomeResult`
directly.

## 6. Telemetry and redaction

`UNKNOWN_HANDLE` may emit only a bounded categorical outcome such as
`unknown_handle`. Logs and telemetry must not include raw circuit tokens,
probe UUIDs, incarnation UUIDs, generation/token contents, or tenant/resource
identifiers as unbounded dimensions.

## 7. Required deterministic M3.9 acceptance tests

| ID | Required evidence |
| --- | --- |
| A | Known Redis completion applied → `APPLIED` and concrete resulting state. |
| B | Known Redis completion stale → `STALE` and concrete resulting state. |
| C | Known local completion applied → `APPLIED` and concrete resulting state. |
| D | Known local completion stale → `STALE` and concrete resulting state. |
| E | Unknown/unwrapped normal token → `UNKNOWN_HANDLE`, false, `None`, zero Redis/local calls. |
| F | Unknown/unwrapped probe → the same unknown result and zero backend calls. |
| G | `LOCAL(G)` handle with missing retired G → `UNKNOWN_HANDLE`, no Redis or active-generation fallback. |
| H | Pre-restart handle submitted after restart → `UNKNOWN_HANDLE`, zero backend calls. |
| I | No unknown path constructs a `CircuitState`. |
| J | `APPLIED` and `STALE` cannot be constructed with `resulting_state=None`. |
| K | `UNKNOWN_HANDLE` cannot be constructed with `applied=True`. |

## 8. Compatibility and scope

M3-R10 remains authoritative for provenance wrappers, `REDIS` versus
`LOCAL(G)` ownership, no inference from bare handles, restart invalidation,
and zero-backend-call behavior for unknown provenance. M3-R11 supplies only
the truthful circuit facade result when routing is impossible.

M3-R9 remains authoritative for `NORMAL` / `DEGRADED_REDIS` /
`RECOVERING_REDIS`, atomic new-admission cutover, the recovery barrier,
local-generation retirement, and no traffic-state writeback or merge.

This addendum changes no OpenAPI or HTTP schema, database schema, Alembic
migration, Redis key format, or M3.4 primitive result contract. It does not
implement M3.9.
