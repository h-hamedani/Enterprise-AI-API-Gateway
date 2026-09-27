# M3-R12 — Local Circuit Completion Lifetime Contract

Status: FROZEN. Documentation only; M3.9 implementation remains separate.

## 1. Purpose and boundary

M3-R4B permits a normal circuit token to remain primitive-valid while its
circuit stays `CLOSED` with matching incarnation and generation. Neither that
raw token nor M3-R6's refreshable circuit idle TTL gives a finite completion
ownership bound. M3-R9/R10 nevertheless require retired local generations to
remain available for valid completion and to be reclaimed within a bound.
This addendum resolves that gap at the **M3.9 orchestration wrapper** only.

The raw `NormalEligibilityToken(identity, incarnation_id, generation)` is
unchanged: no primitive expiry or one-shot rule is added. `RedisCircuitStore`
and `LocalCircuitStore` retain M3-R4B token validity and primitive results.
Primitive tests may continue exercising those rules directly.

## 2. Fixed local normal-completion deadline

A provenance-aware `LOCAL(G)` normal circuit completion handle issued by the
M3.9 facade must carry an absolute, finite, process-local completion deadline
in addition to its raw token, `LOCAL` backend, and issuing generation G.
Exact Python fields and clock representation are implementation-defined.
The deadline belongs to the wrapper, never to the M3.4 raw token.

Add one validated process/deployment setting:

| Setting | Default | Minimum | Maximum |
| --- | ---: | ---: | ---: |
| `local_circuit_normal_completion_ttl_ms` | 300000 ms | 5000 ms | 3600000 ms |

Startup configuration outside the inclusive range fails validation. This is
not a database column, migration, Redis key, or Redis metadata field.

The facade calculates `completion_deadline = issuance_monotonic_time +
local_circuit_normal_completion_ttl_ms` with a process-local monotonic clock.
It is fixed at issuance. Record-success/failure, repeated completion, local
circuit touched-time refresh, mode changes, Redis recovery, and generation
retirement must not extend or restart it. Cutover also must not shorten it.
Changing the validated setting later affects only newly issued wrappers;
existing absolute deadlines stay unchanged.

Multiple wrappers may legitimately contain equivalent raw M3-R4B normal
token contents. Each wrapper has its own fixed issuance deadline. Raw-token
primitive validity and another wrapper's later deadline cannot extend an
individual wrapper's lifetime.

## 3. Completion routing and result

Before its deadline, a `LOCAL(G)` normal wrapper routes only to issuing
generation G under M3-R10. The local primitive's concrete result maps under
M3-R11: `applied=True` → `APPLIED`; `applied=False` → `STALE`. Both preserve
the primitive's concrete `CircuitState`.

At or after its deadline, the wrapper is unusable even if its raw token
remains primitive-valid. The facade makes **zero local and zero Redis calls**,
performs no circuit mutation, does not recreate G, and does not route to an
active G+1. It returns the M3-R11 orchestration result:

```text
applied = False
resulting_state = None
disposition = UNKNOWN_HANDLE
```

Here `UNKNOWN_HANDLE` includes expired, historically known provenance that
is **no longer safely routable**. It does not claim the past provenance was
literally unknown. No new `EXPIRED_HANDLE` disposition is required.

M3-R12 does not impose one-shot use. Repeated completion using the same
unexpired wrapper may continue to reach the local primitive if M3-R4B would
accept the raw token. After the fixed deadline, every submission takes the
zero-backend-call unknown path.

## 4. Other ownership lifetimes

The new TTL applies only to `LOCAL(G)` **normal** circuit completion wrappers.
Local probe ownership continues to use the frozen
`circuit_probe_lease_duration_ms`; local concurrency ownership continues to
use its frozen lease duration and expiry. Neither gets an additional TTL.

`REDIS` normal circuit handles retain M3-R10 Redis routing and M3-R4B
primitive validity. The local-retention setting does not limit Redis tokens
or require any local generation to be retained for them.

## 5. Retired-generation reclamation

When active G becomes retired, all its existing wrapper deadlines remain
unchanged. G remains addressable while completion-relevant ownership may be
valid: unexpired local concurrency leases, unexpired local probe leases, or
unexpired `LOCAL(G)` normal wrappers. Rate, pre-auth, and `ADMIN_TOKEN` state
does not retain G.

G may be physically reclaimed once **all** those ownership classes are
inactive or expired. For normal wrappers the upper bound is the latest
outstanding fixed completion deadline issued by G. Completion before that
deadline cannot move it. Underlying local circuit touched-time changes
cannot keep G alive past that bound.

The orchestration layer may track each generation's maximum outstanding
normal deadline for reclamation. Such tracking is process-local, bounded,
monotonic-clock based, non-durable, and removed with G. It need not key by raw
tokens. The wrapper's own deadline is authoritative for a submitted handle;
generation-level tracking cannot make an expired wrapper valid again.

After a later Redis failure creates G+1, G's deadlines and routing stay
unchanged. G+1 handles use their own issuance times and independent tracking;
ownership or deadlines are not transferred. Restart destroys all local
generations, wrapper deadlines, and retention tracking. Old handles are
unusable under M3-R10/R11; no deadline is reconstructed.

## 6. Telemetry and precedence

Expiry may emit only a bounded category such as `expired_handle` or
`unknown_handle`. Do not log raw tokens, incarnation UUIDs, target/resource
UUIDs, generation IDs as unbounded dimensions, or absolute monotonic values
as identifiers. Full metrics remain M3.10 scope.

M3-R4B remains authoritative for **primitive** token validity. M3-R9 governs
recovery, cutover, retirement, and no state merge/writeback. M3-R10 governs
backend and generation provenance. M3-R11 governs the orchestration result,
including `UNKNOWN_HANDLE` with no resulting state. M3-R12 adds only the
finite lifetime of locally issued normal completion wrappers.

## 7. Required deterministic M3.9 acceptance tests

Use a controllable monotonic clock, not arbitrary sleeps, for lifetime tests.

| ID | Required evidence |
| --- | --- |
| A | Issuance at monotonic T sets deadline to T plus configured TTL. |
| B | Setting below 5000 ms fails startup/configuration validation. |
| C | Setting above 3600000 ms fails startup/configuration validation. |
| D | `LOCAL(G)` normal completion before deadline routes to G. |
| E | Completion exactly at deadline returns `UNKNOWN_HANDLE` with zero backend calls. |
| F | Completion after deadline returns the same unknown result with zero backend calls. |
| G | Applied completion before deadline does not extend it. |
| H | Stale completion before deadline does not extend it. |
| I | Repeated valid completion before deadline does not move it. |
| J | Local circuit touched-time refresh does not alter wrapper deadline. |
| K | Cutover/retirement does not reset or shorten G wrapper deadline. |
| L | Later G+1 creation does not alter G wrapper deadlines. |
| M | Expired `LOCAL(G)` handle calls neither Redis nor active generation. |
| N | Local probe lifetime still uses only probe lease duration. |
| O | Concurrency lifetime still uses only concurrency lease semantics. |
| P | Redis normal handle behavior is unchanged by local TTL. |
| Q | G is reclaimed after all concurrency/probe ownership expires and its latest normal deadline passes. |
| R | Repeated normal-token completion cannot retain G past its fixed maximum normal deadline. |
| S | Configuration change affects new wrappers only. |
| T | Restart reconstructs no wrapper deadlines or generation tracking. |

## 8. External scope

This addendum changes no OpenAPI or HTTP schema, database schema, Alembic
migration, Redis key format, or M3-R4B primitive token/result. The one new
setting is process/deployment configuration only. No M3.9 runtime code or
tests are implemented here.
