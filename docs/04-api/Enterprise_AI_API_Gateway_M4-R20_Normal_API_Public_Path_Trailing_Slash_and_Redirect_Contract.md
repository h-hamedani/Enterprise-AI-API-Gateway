# M4-R20 — Normal API Public Path Trailing-Slash and Redirect Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract defines public Normal API trailing-slash identity and slash-redirect behavior. It does not implement the runtime or change M4-R1 path normalization.

## 1. Authority and scope

M4-R1 owns public-path normalization at the ASGI boundary, service-prefix extraction, normalized remaining-path matching and upstream path construction. It requires preserving meaningful internal segments and a trailing slash in the incoming remaining path. M4-R19 owns authenticated, tenant/service/current-snapshot-scoped path and method resolution, including 404/405 and `Allow`. M4-R7A owns admission order; M4-R18 owns the coherent process-local service/route generation; M4-R14 owns raw query forwarding; M4-R15 owns request-body preflight and forwarding. M4-R3's no-follow upstream-redirect rule is separate.

M4 V1 does not canonicalize slash variants and does not redirect solely to add or remove a trailing slash. This contract leaves R1's normalization and any root-path equivalence unchanged. It does not decide raw `scope["path"]` versus `scope["raw_path"]`, percent-encoded slash handling, dot segments, Unicode normalization, duplicate-slash normalization, or CORS.

## 2. Trailing slash is preserved, not repaired

After M4-R1 normalization, use the resulting remaining path exactly for route matching. Where that normalized representation distinguishes `/orders` from `/orders/`, they are distinct paths. The Gateway MUST NOT remove a trailing slash to find a route and MUST NOT append one to find a route. It MUST preserve the normalized representation; it does not add a second normalization rule here.

No slash-only redirect is generated, with any status, including 301, 302, 303, 307, or 308. The Gateway MUST NOT synthesize a slash-correction `Location` header. This applies to GET and to non-idempotent methods (POST, PUT, PATCH, DELETE); runtime behavior must not rely on a method-preserving redirect to repair a target URI.

## 3. Authentication, resolution, and result

No slash-variant decision may preempt canonical request-ID handling, `gw_` authentication, tenant binding, a usable M4-R18 current-state snapshot, or ACTIVE service resolution. A framework redirect MUST NOT disclose service or route existence, configured spelling, or canonical form before those checks.

After those prerequisites, resolve only the exact M4-R1 normalized remaining path. Apply M4-R19 to that exact variant:

- No ACTIVE executable path match: 404 `resource_not_found`, without `Allow`.
- One or more ACTIVE path matches but no route for the requested method: 405 `invalid_request`, with only the deterministic `Allow` for that exact variant as defined by M4-R19.
- One exact path-and-method match: continue normal M4 processing.
- Multiple exact path-and-method matches: preserve M4-R1's fail-closed ambiguity behavior.

Do not search the opposite slash variant to turn a 404 into a 405 or to construct an `Allow` header. For example, if only `GET /orders` exists, then `GET /orders` may execute; `GET /orders/` is not redirected to it and returns 404 if no route matches `/orders/`. `POST /orders/` is also 404 in that example, not 405 based on the `/orders` route. If `/orders/` has an ACTIVE GET route but no POST route, POST to `/orders/` follows R19 and returns 405 with methods from `/orders/` only.

## 4. Service-prefix forms and query/body

For `/api/{service}` versus `/api/{service}/`, do not add behavior beyond M4-R1's existing prefix extraction and normalized remaining-path semantics. Preserve any distinction R1 produces; preserve any root equivalence R1 already defines. The framework MUST NOT redirect between these public forms before Gateway authentication and R1 resolution.

Slash identity applies only to the path. Query bytes remain separate and unchanged under M4-R14: `/orders?x=1` and `/orders/?x=1` retain their respective path variants, with the same raw query sequence. Determining a slash mismatch does not require reading or consuming a request body; retain M4-R7A/M4-R15 ordering.

## 5. Framework and redirect boundaries

FastAPI/Starlette's `redirect_slashes=True` default is not authoritative for the public Normal API endpoint. The eventual ASGI/FastAPI boundary MUST be configured or structured so that framework slash auto-redirect cannot preempt Gateway-controlled request-ID/authentication, tenant binding, M4-R18 snapshot checks, and M4-R1/R19 resolution. This contract freezes the observable behavior, not the implementation mechanism.

A slash/path mismatch is resolved during service/route resolution, before rate admission, concurrency acquisition, body read/preflight, circuit admission, or upstream I/O. It consumes no rate token, acquires no concurrency handle, performs no circuit operation, and performs no upstream I/O. Preserve existing request-ID and authentication behavior.

The Gateway itself emits no slash-canonicalization `Location`. An upstream application's 3xx response after a valid proxied request remains governed by M4-R3/M4-R16 response-forwarding rules; the Gateway does not follow that redirect. These are separate cases.

## 6. Compatibility

R20 adds no error code: it uses R19's 404 `resource_not_found` and 405 `invalid_request`. It adds no OpenAPI operation, response, schema, or change; the frozen OpenAPI describes the dynamic proxy surface and does not promise a slash-correction redirect. It adds no database column, migration, service/route field, tenant setting, feature flag, or M3-R3A consumer. Slash policy is fixed for M4 V1, not tenant-configurable.

## 7. Required later acceptance evidence

Use the actual public ASGI/FastAPI boundary. Prove: exact `/orders` and `/orders/` routes can independently execute when configured and permitted by M4-R1 overlap rules; with only one variant configured, the other returns 404 without 301/302/303/307/308 or a synthesized `Location`; POST/PUT/PATCH/DELETE are not redirected; exact-path method mismatch follows R19 and its `Allow` excludes opposite-variant methods; unsupported HEAD/OPTIONS/TRACE/CONNECT never become executable through a slash redirect; invalid credentials receive the auth result rather than a framework redirect; unknown/disabled service and dirty/pending R18 state do not disclose route spelling; no rate token, concurrency lease, body consumption, circuit admission, or upstream I/O occurs on a slash mismatch; query bytes remain separate; and Starlette auto-redirect cannot preempt Gateway semantics. Also verify service-root forms under the existing R1 rule without changing that rule.
