# M3-R10 — Protection Handle Provenance Contract

Status: FROZEN. Documentation only. M3.9 implementation remains blocked until
its orchestration implements this contract alongside M3-R9.

## 1. Purpose and precedence

Ownership-bearing handles must retain their issuing backend across
`DEGRADED_REDIS` → `RECOVERING_REDIS` → `NORMAL` and across later local
degraded generations. A locally issued handle must never be sent to a Redis
primitive merely because process mode later becomes `NORMAL`.

This addendum supplies M3-R9's handle-provenance decision. M3-R9 remains
authoritative for recovery mode, coordination/barrier, atomic new-admission
cutover, retired generations, and no writeback/merge. M3-R10 governs only
ownership-bearing handle representation and completion routing. M3-R6/R7
and unrelated M3.3/M3.4 rules remain unchanged.

## 2. Primitive contracts remain opaque

The M3.3 Redis and local concurrency primitives continue to use raw UUID
`lease_id` values. The M3.4 Redis and local circuit primitives continue to
use raw `NormalEligibilityToken` or probe UUID values. Their primitive APIs,
token formats, Redis key/schema formats, and state transitions are not
changed by this contract.

Backend identity must **not** be inferred from or encoded into UUID version
bits or prefixes, incarnation UUIDs, circuit generation integers, Redis
keys, or other opaque random values.

## 3. Runtime-internal orchestration handles

The M3.9 protection facade must return a provenance-aware handle for each
successful ownership-bearing admission:

| Conceptual handle | Required fields |
| --- | --- |
| Concurrency handle | Raw lease UUID; backend `REDIS` or `LOCAL`; issuing local generation ID when `LOCAL`. |
| Circuit completion handle | Raw normal token or probe UUID; backend `REDIS` or `LOCAL`; issuing local generation ID when `LOCAL`. |

Exact Python names are implementation-defined. A rejected concurrency
acquire returns no handle. A Redis-issued lease/token/probe receives `REDIS`
provenance; a locally issued one receives `LOCAL(G)` provenance for its
issuing generation G. The wrapper, not a bare primitive value, is the
authoritative routing input.

These handles are process-local, runtime-internal, non-durable, and not an
HTTP or external API token format. They are not stored in PostgreSQL or
Redis, serialized into a new JSON protocol, signed, added to OpenAPI, or
reconstructed after restart. A future cross-process transfer requirement
needs a separate contract.

## 4. Completion routing

Routing depends on handle provenance, never the process's current mode:

| Operation | `REDIS` handle | `LOCAL(G)` handle |
| --- | --- | --- |
| Concurrency renew | `RedisConcurrencySemaphore` | Issuing local generation G |
| Concurrency release | `RedisConcurrencySemaphore` | Issuing local generation G |
| Circuit `record_success` / `record_failure` | `RedisCircuitStore` with the raw token/probe | Issuing local generation G with the raw token/probe |

A Redis lease remains Redis-owned while the process is degraded. If Redis is
unavailable on renew/release, M3-R7 dependency semantics apply; no local
lease is synthesized and no equivalent local ownership is created. Redis
lease expiry conservatively bounds failed completion.

Likewise, a Redis circuit token/probe remains Redis-owned during degraded
mode. An unavailable Redis outcome follows M3-R7; it is not converted into
a local circuit outcome and cannot mutate active local circuit state. Redis
expiry/generation rules remain authoritative.

After cutover, `LOCAL(G)` renew/release/outcome operations continue to route
only to retired generation G. They never reach Redis. G cannot admit new
operations, but may complete valid ownership or return the existing safe
stale/no-op outcome after natural expiry or invalidation. No local lease is
materialized into Redis or counted as a Redis semaphore owner.

## 5. Unknown and invalid provenance

The facade must not guess ownership for a bare, malformed, legacy, unknown,
or pre-restart handle. Current mode, UUID shape, and raw token contents are
not evidence of provenance.

Unknown concurrency renew returns **not renewed**; unknown release returns
**not released**. Neither invokes Redis nor guesses a local generation.
Unknown circuit completion is not applied and must use an existing truthful
stale/no-op orchestration outcome if one exists. If the current circuit
result contract cannot truthfully represent this case, M3.9 implementation
must **stop and report that exact result-contract conflict**, not invent a
synthetic circuit state. Unknown provenance never creates a generation,
converts a handle between backends, mutates Redis, or merges state.

## 6. Local generation identity and lifetime

Each active local generation has a process-local identity distinct from
later generations (G, G+1, …). A monotonic integer or opaque process-local
identifier may be used. It is not derived from user input and is not a
Redis/database identifier.

A retired generation remains addressable while concurrency or circuit
completion ownership may still be valid. It can be discarded when relevant
ownership has expired or become safely inactive. Rate, pre-auth, and
`ADMIN_TOKEN` state does not keep it alive. Generation and optional handle
registry retention must be bounded by the existing ownership/expiry
semantics; no unbounded accumulation is permitted.

A process-local registry may optimize lookup, but correctness must depend on
the provenance-aware wrapper, not on a bare UUID/token lookup. A registry
must be bounded, non-durable, cleaned with handle/generation lifetime, and
never used to recreate provenance after restart.

## 7. Restart and production routing boundary

Restart invalidates every orchestration handle from the terminated process,
including Redis-provenance handles. The new process reconstructs neither
local nor Redis lease/circuit provenance nor retired-generation ownership.
An old handle is therefore unknown and follows the conservative outcomes
above. No old local state is recreated.

M3.9 production ownership-bearing admission and completion paths must use
the provenance-aware facade. They must not bypass it to invoke a primitive
using a raw handle. Direct primitive access remains valid in internal tests
of `RedisConcurrencySemaphore`, `RedisCircuitStore`,
`LocalConcurrencyStore`, and `LocalCircuitStore`.

## 8. Security and telemetry

Unknown provenance may emit only bounded categories such as
`unknown_handle`, `stale_handle`, or `invalid_provenance`. Raw lease UUIDs,
circuit tokens, probe UUIDs, and other handle values must not appear in logs
or telemetry. Invalid provenance cannot cause Redis mutation, local
generation creation, backend conversion, or state merge.

## 9. Required deterministic M3.9 acceptance tests

| ID | Required evidence |
| --- | --- |
| A | Redis concurrency acquire returns a `REDIS` provenance handle. |
| B | Local generation G concurrency acquire returns `LOCAL(G)`. |
| C | `LOCAL(G)` release after return to `NORMAL` calls G only, with zero Redis calls. |
| D | `LOCAL(G)` renew after return to `NORMAL` calls G only, with zero Redis calls. |
| E | Redis lease release during `DEGRADED_REDIS` calls Redis only. |
| F | Redis lease renew during `DEGRADED_REDIS` calls Redis only. |
| G | Redis circuit normal token receives `REDIS` provenance. |
| H | Redis circuit probe receives `REDIS` provenance. |
| I | Local G circuit normal token receives `LOCAL(G)`. |
| J | Local G circuit probe receives `LOCAL(G)`. |
| K | `LOCAL(G)` circuit completion after cutover calls G only, with zero Redis calls. |
| L | Redis circuit completion during degraded mode calls Redis only and makes no local mutation. |
| M | Unknown bare concurrency UUID yields a conservative missing result and calls neither backend. |
| N | Unknown/unwrapped circuit completion calls neither backend; use a truthful stale/no-op result or stop on result-contract conflict. |
| O | Restart reconstructs no provenance registry or generations. |
| P | Handles from G and later G+1 never cross-route. |
| Q | Raw handle values are absent from logs and telemetry. |

Race-sensitive tests must use controlled synchronization, not UUID-version
heuristics or current-mode inference.

## 10. Compatibility and scope

This documentation addendum changes no M3.3 semaphore primitive, M3.4
circuit token format, OpenAPI schema, PostgreSQL schema/migration, Redis
schema/key format, or M3-R6/R7 behavior. It does not implement M3.9.
