# M3-R3 Leased Concurrency Semaphore Contract Reconciliation

Status: authoritative M3.3 addendum. It freezes previously unspecified lease
semantics without implementing the semaphore or changing the database.

## Original blockers and package boundary

The M3 artifacts required distributed leased concurrency but did not freeze
lease duration, renewal lifetime, repeated release, layered atomicity, shared
ownership, or key cleanup. This addendum resolves those points.

M3.3 accepts already-resolved enabled concurrency policies containing only
`policy_id`, `tenant_id`, `scope_type`, `scope_id`, and `max_concurrency`.
It validates them but does not query PostgreSQL or resolve request
applicability. Later workload packages own request lifecycle and renewal timing.

## Lease duration and identity

`concurrency_lease_duration_ms` is deployment runtime configuration with a
default of `30000`, minimum `5000`, and maximum `120000`. It is not a database
field, request parameter, or route/provider-timeout derivative. A lease is a
renewable crash-recovery mechanism, not a maximum request duration.

One logical acquisition generates one opaque UUIDv7 `lease_id`, independent of
credentials and scope identifiers. The same lease ID owns every acquired layer.

## Keys, state, and time

Typed internal construction uses exactly:

`gw:v1:sem:{tenant}:{scope_type}:{scope_id}`

All policies in one operation have the same tenant. The hash tag keeps layered
keys in one Redis Cluster slot. Callers cannot provide arbitrary keys.

Each policy key is a sorted set whose member is `lease_id` and score is absolute
`expires_at_ms`. There is no global lease registry. Redis `TIME` is the sole
clock authority:

```text
now_ms = seconds * 1000 + floor(microseconds / 1000)
```

`expires_at_ms <= now_ms` is expired. Every operation prunes scores through
`now_ms` before ownership or capacity evaluation. Expired leases cannot renew
or count as active ownership.

## Deterministic ordering

Normal ordering is `API_KEY`, `ROUTE`, `SERVICE`; LLM ordering is `API_KEY`,
`LLM_ALIAS`/`LLM_MODEL`, `PROVIDER_TARGET`; Control Plane ordering is
`ADMIN_TOKEN`. Equal-rank entries sort by ascending `policy_id`. Ordering is
only for deterministic script inputs, safe rejection attribution, and bounded
telemetry; it grants no acquisition priority.

## Atomic acquire

One Redis-side atomic operation obtains Redis time, prunes expired members,
checks every applicable limit, and then either adds the same lease ID to every
key or none. Sequential partial acquisition and client compensation are
forbidden. Rejection changes no active ownership except expiry pruning.

An acquired member receives `expires_at_ms = now_ms +
concurrency_lease_duration_ms`. An empty policy set returns `acquired=true` and
`lease_id=null` without Redis access.

## Atomic renewal

Renewal first obtains Redis time and prunes expiry. It succeeds only when the
exact lease ID is active in every requested layer. Success atomically assigns
every layer:

```text
new_expires_at_ms = now_ms + concurrency_lease_duration_ms
```

Renewal never extends from the old expiry. Missing or expired ownership in any
layer renews none and returns `renewed=false`; an expired lease cannot be
resurrected. There is no maximum renewal count or cumulative lease lifetime.
Every extension remains finite and continuous ownership requires continuous
renewal. M3.3 creates no background heartbeat.

## Atomic release

Release prunes expiry, then checks the exact lease ID across all requested
layers. If it was a complete active layered lease at operation start, it is
removed everywhere and `released=true`. Missing, expired, wrong, or previously
released ownership returns `released=false`.

If the same lease ID exists in only some requested layers, the operation removes
that exact caller-owned residual ID wherever present but returns
`released=false`. It never removes or reveals another member. Thus a first
complete release returns true and repeated release returns false.

## Expiry, cleanup, and restart

Owner failure stops renewal; the next atomic operation prunes the expired lease
and makes capacity available without a worker. After any operation leaving
active members, each key receives:

```text
key_ttl_ms = 2 * concurrency_lease_duration_ms
```

An empty sorted set may be explicitly deleted; Redis also removes the key after
its final member is removed. No cleanup worker is introduced.

Semaphore state is ephemeral. After Redis restart leases may be absent; old
lease IDs no longer prove ownership, new acquire sees available state, and old
renew/release return false. State is not reconstructed or written back from
process memory. Static Lua scripts may use `SCRIPT LOAD`/`EVALSHA`, with one
bounded `NOSCRIPT` reload and one retry.

## Validation, results, and failure boundary

Resolved policies require UUID identifiers, a valid frozen scope enum, positive
PostgreSQL-range integer `max_concurrency`, one tenant, and no duplicate
canonical scope. Invalid policies fail before Redis execution.

Immutable internal results are:

- acquire: `acquired: bool`, `lease_id: UUID | null`
- renew: `renewed: bool`
- release: `released: bool`

Redis unavailability produces a typed concurrency dependency failure. M3.3
never fails open, grants a local lease, maintains process-local correctness
state, or applies `degraded_factor`; M3.8 owns degraded behavior.

Telemetry is limited to bounded operation, outcome, and enum `scope_type`.
Tenant, scope, policy, and lease UUIDs, Redis keys, paths, IPs, credentials,
URLs, arguments, and exception text are forbidden as metric labels or logs.

## Impact

No table, column, constraint, migration, endpoint, HTTP enforcement, heartbeat
orchestration, Lua script, or semaphore runtime is added by this reconciliation.
PostgreSQL remains policy authority; lease duration is process configuration.
