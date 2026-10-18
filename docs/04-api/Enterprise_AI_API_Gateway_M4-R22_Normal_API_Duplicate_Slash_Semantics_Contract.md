# M4-R22 — Normal API Duplicate-Slash Semantics Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract defines duplicate literal slash handling for Normal API public paths and the minimum route-configuration compatibility consequence. It does not implement the runtime or modify Admin validation.

## 1. Authority and scope

M4-R1 owns one-time strict UTF-8 path processing, encoded-separator and dot-segment rejection, Service-prefix stripping, Route matching, and Model-A prefix append. M4-R19 owns path/method 404, 405, and `Allow`; M4-R20 owns trailing-slash identity and prohibits slash-correction redirects; M4-R21 owns raw-path ingress evidence and the single canonical execution path. R22 narrows only R1's previously unspecified treatment of empty segments caused by consecutive literal `/` characters. It does not reopen R1 or add a second normalization pass.

## 2. Public ingress decision: reject consecutive literal slashes

M4 V1 rejects a public Normal API path containing two or more consecutive literal `/` characters. It does not collapse repeated slashes, remove empty segments, redirect to a single-slash form, search a single-slash equivalent, or preserve an empty segment as routable input.

Examples rejected as HTTP 400 `invalid_request` when they reach the Gateway include:

- `/api/svc/a//b`
- `/api/svc/a///b`
- `/api/svc/a//`
- `/api/svc//a`
- `/api//x`
- `/api///x`

This is an M4 V1 Gateway safety rule, not a claim that HTTP itself forbids duplicate slashes. A request target rejected by the HTTP parser before ASGI invocation remains outside the Gateway error-envelope boundary.

## 3. Trailing slash remains distinct

A single final slash is not a duplicate-slash violation. `/api/svc/a` and `/api/svc/a/` retain the distinct effective-path behavior established by M4-R20; no redirect or canonicalization is introduced. `/api/svc/a//` is invalid because it contains consecutive slash characters. Internal duplicate-slash rejection and the trailing-slash identity rule are separate.

The one slash delimiter at each boundary in `/api/{service}/{remaining_path}` is valid. R22 rejects only consecutive literal slashes that create an empty path segment; it does not reinterpret an empty segment as routing input.

## 4. Service prefix and remaining path

Duplicate-slash validation is part of the one raw-path-to-canonical-path validation pass required by M4-R21. It occurs before Service lookup. `/api//x` must not trigger lookup for an empty Service slug; `/api/foo//x` must not be normalized to `/api/foo/x` or select that Service/route.

For a duplicate-slash remaining path such as `/api/svc/a//b`, reject ingress before constructing R19's path-match set. Do not match `/a/b`, do not match a stored `/a//b`, do not produce 404/405 or `Allow` from either representation, and do not forward `/a//b` upstream.

## 5. R1/R21 canonicalization and encoded separators

Use the existing R21 pipeline exactly once:

`raw_path` → one R1/R22 validation and canonicalization pass → one canonical execution path.

Detect literal repeated slash as part of establishing that path. Do not first collapse and then canonicalize. Do not use decoded `scope["path"]` to hide duplicate-slash evidence in `raw_path`.

R22 concerns literal repeated slash structure only. `%2F` and `%2f` remain the encoded-separator violation already governed by R1; reject them under R1 rather than decoding them and classifying the result as an R22 duplicate slash. R1's dot-segment, strict UTF-8, forbidden separator, base-path escape, and Model-A rules remain unchanged.

## 6. Error and admission behavior

A duplicate-slash client path received by the Gateway is invalid syntax and returns the existing HTTP 400 `invalid_request`. Do not use 404, 405, 301, 302, 303, 307, 308, or 500 for an ordinary client-supplied duplicate-slash violation. Do not echo the offending raw path in the error response.

Validation belongs to M4-R7A Service/Route resolution, after canonical request-ID handling, Gateway authentication, and tenant binding, and before Service/Route execution using the invalid form, rate admission, concurrency acquisition, body read, circuit admission, or upstream I/O. A rejection consumes no rate token, acquires no concurrency handle, reads no body, performs no circuit operation, and performs no upstream I/O. Server-level request-target rejection before ASGI remains outside this ordering.

## 7. Route-pattern and Admin compatibility boundary

A stored `Route.path_pattern` containing consecutive slashes can never authorize duplicate-slash ingress under R22. It is not matched for an invalid request; it cannot cause collapse to another pattern, alter R19 404/405/`Allow`, or enable upstream execution. Existing rows are not rewritten, normalized, or deleted automatically.

The current Admin `RouteCreate` validation requires a leading slash, and `RoutePatch` has the same leading-slash shape when supplied; the physical database also checks only that `path_pattern` starts with `/` and enforces method/path uniqueness. The frozen static OpenAPI `RouteCreate` schema currently states a non-empty string for `path_pattern`, and `RoutePatch` states a string; it does not encode a no-consecutive-slash constraint. Therefore R22 does not silently claim Admin rejection of `//` patterns or change that published contract. Tightening new/updated Admin route-pattern validation consistently requires a separate Admin/OpenAPI reconciliation. Until then, a persisted such pattern remains non-executable by the public runtime; no migration is introduced.

## 8. Connector, upstream, and security boundary

No accepted R22 ingress path contains consecutive literal slashes. The outbound connector is not authorized to receive a duplicate-slash canonical remaining path from accepted public ingress. It must still preserve the accepted canonical path under R21; HTTPX normalization is not a substitute for ingress validation.

The safety rationale is that servers, proxies, frameworks, and upstream applications may interpret or normalize repeated slashes differently. Routing a request as `/a//b` while an upstream interprets it as `/a/b` could make route authorization differ from the effective upstream target. M4 V1 removes that ambiguity by rejecting before routing; it does not depend on upstream behavior. R11 continues to protect scheme, authority, host, port, and validated peer. Duplicate-slash rejection is not an SSRF mechanism and does not add general URL-authority parsing.

Do not log unbounded raw paths or echo them in the public 400 envelope. Bounded reason/category metadata may identify duplicate-slash rejection under existing telemetry governance.

## 9. Compatibility and required later evidence

R22 adds no public error code or schema: existing 400 `invalid_request` applies. It adds no Normal API operation, OpenAPI change, database migration, Service/Route field, tenant setting, feature flag, configurable policy, or M3-R3A consumer. Duplicate-slash behavior is fixed M4 V1 runtime semantics.

Later ASGI integration acceptance must prove: ordinary `/api/svc/a/b` resolves normally; a single final slash remains R20-distinct; `/api/svc/a//b`, `/a///b`, `/a//`, `/api/svc//a`, `/api//x`, and `/api///x` return 400 without collapse, redirect, or `Location`; empty Service segments are never looked up; invalid input produces no R19 result, `Allow`, rate token, concurrency handle, body read, circuit operation, or upstream I/O; `%2F` remains an R1 rejection; dot segments remain R1 rejections; actual Uvicorn `raw_path` preserves duplicate slash for Gateway validation; connector behavior is not relied on to reject ingress; stored `//` route patterns cannot authorize requests and are never rewritten; and offending path bytes are neither logged unbounded nor returned.
