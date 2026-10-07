# M4-R19 — Normal API Public Endpoint Method and Routing Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This reconciliation resolves method mismatch semantics for the public Normal API endpoint. It does not implement the endpoint or change route matching.

## 1. Authority and scope

M4-R1 continues to own path normalization, path-pattern matching, path joining, and ambiguous same-request route selection. M4-R19 only defines the distinction between a missing executable path and a path whose active routes do not support the requested method, plus the safe `Allow` response. The M4 execution pack's public proxy method set is `GET`, `POST`, `PUT`, `PATCH`, and `DELETE`; its existing public errors include 404 `resource_not_found` and 405 `invalid_request`. M4-R16's unsupported HEAD and CONNECT decisions remain in force. M4-R7A and M4-R18 continue to own admission ordering and the coherent current-state snapshot.

Framework defaults are subordinate to these contracts. This contract does not freeze trailing-slash redirects, raw-path adapter behavior beyond M4-R1, or CORS policy.

## 2. Resolution model

After canonical request-ID handling, successful `gw_` authentication and tenant binding, a usable M4-R18 snapshot, and ACTIVE service resolution, form the tenant/service/snapshot-scoped path-match set `P`: all currently executable ACTIVE routes in that resolved service and captured coherent generation whose M4-R1 path-pattern semantics match the normalized remaining public path, considering path only for this 404/405 distinction. Do not inspect routes from another tenant or service, disabled/deleted routes, stale generations, or routes absent from the captured snapshot.

If exactly one route in `P` matches the requested method, continue with M4-R1 route selection. If multiple routes match both path and method, preserve M4-R1's existing ambiguous-match fail-closed behavior; M4-R19 does not choose a winner. Multiple path matches for different methods do not make the method-independent set ambiguous: `P` is a set of executable route records, and its method inventory is the deterministic union specified below.

## 3. 404 and 405

- If the service is unknown/inaccessible, preserve 404 `resource_not_found`; do not emit `Allow`.
- If `P` is empty, return 404 `resource_not_found` with no `Allow` header. A path represented only by disabled/deleted routes is empty for this purpose.
- If `P` is nonempty but no route in `P` supports the requested method, return 405 `invalid_request` with the `Allow` header defined in Section 4.
- If exactly one route in `P` supports the requested method, continue normally. M4-R1 ambiguity handling applies if more than one supports it.

Use the existing canonical safe Gateway error envelope and request ID. Do not expose route IDs, patterns, tenant IDs, backend URLs, configuration versions, or route inventory in the error body.

## 4. `Allow` header and disclosure boundary

For a 405, `Allow` is the de-duplicated set of public M4 V1 proxy methods represented by ACTIVE executable routes in `P`, serialized in this fixed order: `GET, POST, PUT, PATCH, DELETE`, omitting absent methods. Duplicate routes with the same method do not duplicate the value. Do not include methods from another tenant/service, disabled/deleted routes, internal/admin routes, framework-added methods, or unsupported methods.

This method-availability disclosure is permitted only after authentication and only for the resolved service in the usable tenant-scoped M4-R18 snapshot. It is method metadata, not route inventory, and appears only in the header, not the error body. An unusable/dirty/pending M4-R18 state fails closed under its existing safe behavior before route inventory is disclosed.

## 5. Unsupported methods and framework interaction

`HEAD`, `CONNECT`, `TRACE`, and `OPTIONS` are not proxied as Normal API V1 routes under this contract. For any such method that reaches this authenticated Normal API resolution boundary, an empty `P` yields 404 without `Allow`; nonempty `P` yields the same 405 `invalid_request` and `Allow` rules as any other unsupported method. HEAD is never synthesized from GET and is never added to `Allow`; CONNECT never creates a tunnel; TRACE is never reflected. OPTIONS is not proxied unless a separate authoritative contract explicitly adds it. This does not authorize framework-generated OPTIONS to bypass authentication; any independently authoritative CORS/preflight layer remains separately governed.

FastAPI/Starlette automatic HEAD-from-GET, generic method matching, redirect, and 405 behavior is not authoritative where it conflicts with this contract. A framework-generated response may be reused only if it preserves request-ID/auth/tenant ordering, computes `Allow` from the captured tenant/service snapshot, and exactly implements these outcomes. The Gateway must control the method inventory; static router registration cannot substitute for it.

## 6. Admission ordering and side effects

Method/path existence is determined only after request ID, gateway authentication/tenant binding, usable current-state snapshot, and ACTIVE service resolution. An unauthenticated caller cannot learn route/method existence from 404/405 differences or `Allow`, and tenant identity is not selected by the request path, query, or arbitrary header.

Service/route resolution 404 or 405 precedes rate admission, concurrency acquisition, request-body preflight/read, circuit admission, and upstream I/O. Such requests consume no rate token, acquire no concurrency handle, create no circuit event, and perform no upstream I/O. Preserve existing request-ID and authentication behavior.

## 7. Compatibility

No public error code or error-envelope schema is added: 404 `resource_not_found` and 405 `invalid_request` already exist in the frozen vocabulary. No OpenAPI change is required for this reconciliation. No database field, migration, runtime setting, or M3-R3A consumer is added; the decision reads only the coherent M4-R18 service/route snapshot. M4-R1 matching and ambiguity semantics are unchanged.

## 8. Required later acceptance evidence

Exercise the actual public ASGI/FastAPI boundary, not only a matcher unit. Prove: supported GET/POST select normally; existing path with absent method yields 405; deterministic `Allow` order and de-duplication; disabled/deleted routes are excluded; unknown path/service yields 404 without `Allow`; GET-only plus HEAD gives 405 and no synthesized HEAD; CONNECT never proxies; TRACE never proxies; OPTIONS never proxies absent a separate authoritative contract; authentication precedes method disclosure; cross-tenant/service methods never appear; 404/405 precede rate, concurrency, body read, circuit, and upstream activity; request ID and safe error body are preserved; no route details leak; M4-R1 same-method ambiguity remains fail-closed; dirty/pending current state fails under M4-R18; and framework-generated method behavior cannot bypass Gateway auth or snapshot-scoped `Allow`.
