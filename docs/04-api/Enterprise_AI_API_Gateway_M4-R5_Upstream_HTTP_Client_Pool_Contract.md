# M4-R5 — Normal API Upstream HTTP Client Pool Contract

Status: frozen, documentation-only M4 V1 reconciliation. **READY FOR M4 HTTP CLIENT POOL IMPLEMENTATION.** This does not declare the full M4 proxy ready or implement it.

## 1. Authority and scope

The M4 Normal API Engine Execution Pack V1.1 requires a long-lived shared `httpx.AsyncClient` with deployment-configured, bounded connection-pool limits. The HLD V1.1 Corrected calls for separate bounded HTTP pools by upstream/provider class. For M4 V1, the Normal API upstream transport is one such class: it owns one shared client/pool per running application process, distinct from any future LLM provider transport. This is not a per-tenant, per-service, or per-request client. The Implementation Plan V1.1 requires async HTTPX forwarding. The current `Settings` model uses unprefixed snake-case fields mapped case-insensitively to environment variables, and `app.main.lifespan` owns and closes process-local resources. No existing Normal API upstream client or pool settings are present. Compose currently starts PostgreSQL and Redis only; deployment supplies application settings through the existing environment/`.env` settings surface, not through a new Compose service.

This addendum narrows only client ownership/lifecycle and pool configuration. It does not change M4-R1 routing, M4-R2 circuit classification, M4-R3 header policy, M4-R4 Admin/OpenAPI behavior, the M3 protection system, or any tenant configuration.

## 2. Ownership and lifecycle

Each running application lifespan in an OS process constructs exactly one Normal API upstream `httpx.AsyncClient` after startup configuration validation and retains it for that lifespan. All Normal API requests in that process use that same client object. Shutdown closes it exactly once, including cleanup after a later startup/lifespan failure. A test creating multiple independent application lifespans in one process may create one client for each lifespan; no global module singleton crosses those lifespans. The client and its pool are process-local and are never shared across OS processes. Ordinary upstream connection/timeout failures do not cause application-level client recreation: HTTPX manages individual connections within the surviving pool. Construction must not probe every configured upstream or require an upstream to be online at startup.

## 3. Deployment settings and validation

| `Settings` field | External environment name | Default | Inclusive valid range | Zero | Null |
| --- | --- | ---: | ---: | --- | --- |
| `normal_api_upstream_max_connections` | `NORMAL_API_UPSTREAM_MAX_CONNECTIONS` | 100 | 1–512 | invalid | invalid |
| `normal_api_upstream_max_keepalive_connections` | `NORMAL_API_UPSTREAM_MAX_KEEPALIVE_CONNECTIONS` | 20 | 1–512 | invalid | invalid |

Both fields are optional startup-only deployment settings with deterministic defaults; absence uses the stated default. Accept integer values or canonical base-10 integer strings (as supplied by environment variables); reject booleans, fractional values, malformed or empty strings, null, and other non-integer values. `normal_api_upstream_max_keepalive_connections <= normal_api_upstream_max_connections` is mandatory. Invalid, out-of-range, or cross-field-invalid configuration fails startup before serving requests; it is never clamped or silently replaced with a default. The default pair is valid. The 512 ceiling is a per-process resource guard against accidental socket/memory over-allocation, not a throughput promise or business admission limit.

Use the current Pydantic settings/environment mechanism; do not add aliases, tenant rows, a settings API, or dynamic reload. A change to either setting requires process restart. These values are not PostgreSQL tenant configuration and do not participate in M3-R3A invalidation or reconciliation. There is no new M3-R3A consumer.

## 4. HTTPX mapping and transport boundaries

Pass the validated values to `httpx.Limits(max_connections=..., max_keepalive_connections=...)` and pass that `Limits` instance to the shared `httpx.AsyncClient`. `max_connections` bounds simultaneous connections in this client pool; `max_keepalive_connections` bounds idle reusable connections retained in that pool, not active requests. These limits apply to the client regardless of negotiated HTTP version; they do not enable HTTP/2. The repository currently resolves HTTPX 0.28.1, whose `AsyncClient` defaults are 100/20 and whose `Limits` default `keepalive_expiry` is 5 seconds. M4-R5 intentionally adds no keepalive-expiry setting: omit that argument and use the installed, lock-resolved HTTPX default. A future HTTPX upgrade that changes this default needs compatibility review; the expiry is not a new frozen numerical M4 setting.

Pool acquisition timeout is **DEFERRED TO A LATER M4 TIMEOUT CONTRACT**; the existing per-service `pool_timeout_seconds` is not a connection-count limit and this document does not change its request-time behavior. HTTP/2 enablement, redirect-follow behavior, TLS trust/custom CA configuration, and system/deployment proxy use are outside this pool contract. In particular, HTTPX's `trust_env` behavior must be explicitly reconciled with the outbound/SSRF contract before proxy runtime implementation; M4-R5 does not authorize an implicit environment proxy path. The M4 pack already says upstream redirects are not automatically followed, but this document does not redefine that rule.

## 5. Protection, failures, and process count

M3 rate and concurrency controls are business/runtime admission decisions. The HTTPX pool is a process-level transport resource bound. Neither limit is derived from the other, and pool exhaustion is not itself a policy rejection. With N application processes, each owns its own configured pool, so a deployment-wide theoretical connection count scales with N. There is no cross-process pool coordinator.

Invalid pool configuration is a startup/configuration failure, not an M4 request-time upstream error, upstream circuit failure, or M3 degraded-protection event. The settings contain no secrets; safe startup diagnostics may report validated numeric limits under M3-R13 logging rules, but must never include upstream credentials or destination-specific secret material. M4-R5 requires no new pool-count metric; broader observability belongs to M7.

## 6. Implementation and acceptance tests required later

Test default 100/20; valid explicit settings; both inclusive minimum and maximum boundaries; below-minimum, above-maximum, non-integer, empty/null, and keepalive-greater-than-max startup rejection. Verify that the exact validated values reach `httpx.Limits`, a single client object is reused by multiple Normal API requests in one lifespan, no per-request client is constructed, and shutdown closes the client. Verify separate lifespans/processes do not share one client. Test that an ordinary upstream transport failure leaves the shared client alive.

Acceptance should use a local synthetic HTTP server, not an internet API, to observe actual reuse: two sequential requests to the same origin should be handled over the same reusable HTTP/1.1 connection when the server keeps it open and the requests fall within the installed library's keepalive expiry. This demonstrates reuse, not a guarantee that every request always uses the same socket. Also verify that bounded limits and pool waiting do not bypass M3 admission controls. Proxy/SSRF and timeout acceptance remain separate gates.

## 7. Compatibility and supersession

M3 multi-process behavior remains process-local; M3 concurrency remains independent. M4-R1 URL routing, M4-R2 circuit outcomes, M4-R3 header filtering, and M4-R4 Admin/OpenAPI have no changes. Future M6 retry/streaming can reuse the long-lived transport without changing these pool limits; M7 may add safe pool observability. No database schema, Alembic migration, OpenAPI, or tenant config-version change is required.

M4-R5 supersedes only the M4 pack's unspecified *Normal API client ownership and pool-setting details*: one client per application lifespan/process, the two setting names/defaults/bounds and cross-field rule, startup validation, and startup-only deployment source. It does not supersede request timeouts, retry, redirects, streaming, body limits, cancellation, egress/proxy policy, or TLS trust. Those remaining areas must be reconciled under their own contracts before full M4 proxy implementation.
