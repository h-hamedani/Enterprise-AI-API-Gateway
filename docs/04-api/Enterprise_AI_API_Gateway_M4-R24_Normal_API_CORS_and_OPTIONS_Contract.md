# M4-R24 — Normal API CORS and OPTIONS Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract closes the public Normal API CORS decision reserved by M4-R19. It introduces no runtime implementation, Admin change, OpenAPI change, test, or migration.

## 1. Authority, scope, and precedence

[M4-R19](Enterprise_AI_API_Gateway_M4-R19_Normal_API_Public_Endpoint_Method_and_Routing_Contract.md) requires authenticated tenant/service/snapshot-scoped path resolution and defines unsupported-method 404/405/`Allow` behavior. It excludes OPTIONS proxying and reserves any separate CORS/preflight mechanism for an authoritative contract. R24 closes that reservation for M4 V1: there is no separate public Normal API CORS/preflight mechanism. It does not supersede R19's method inventory, authentication, disclosure, or resolution rules.

[M4-R3](Enterprise_AI_API_Gateway_M4-R3_Normal_API_Header_Policy_Contract.md) continues to control request filtering and the fixed response-header allowlist. [M4-R7A](Enterprise_AI_API_Gateway_M4-R7_Normal_API_Protection_Admission_Ordering_Contract.md) controls admission ordering; [M4-R18](Enterprise_AI_API_Gateway_M4-R18_Normal_API_Current_State_Consumer_Contract.md) controls the usable coherent service/route snapshot. [M4-R20](Enterprise_AI_API_Gateway_M4-R20_Normal_API_Public_Path_Trailing_Slash_and_Redirect_Contract.md), [M4-R21](Enterprise_AI_API_Gateway_M4-R21_Normal_API_Raw_Path_and_Canonical_Path_Representation_Contract.md), [M4-R22](Enterprise_AI_API_Gateway_M4-R22_Normal_API_Duplicate_Slash_Semantics_Contract.md), and [M4-R23](Enterprise_AI_API_Gateway_M4-R23_Normal_API_Unicode_Path_Canonicalization_Contract.md) retain their path rules. Existing security, error-envelope, and canonical request-ID contracts remain authoritative.

The [API Specification V1.2.2 Final](Enterprise_AI_API_Gateway_API_Specification_V1.2.2_Final.docx), section 6.2, makes HEAD/OPTIONS conditional rather than assumed; its Normal API error mapping already specifies 405 for a method not allowed by the resolved route. The [M4 execution pack](../05-implementation/Enterprise_AI_API_Gateway_M4_Normal_API_Engine_Execution_Pack_V1.1.docx) and the current Admin method type, physical database CHECK, and [OpenAPI](Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml) restrict executable Normal API route methods to GET, POST, PUT, PATCH, and DELETE. R24 adds no optional OPTIONS configuration or operation.

[M8](../05-implementation/Enterprise_AI_API_Gateway_M8_Admin_UI_Demo_Flow_Execution_Pack_V1.1.docx) and [M9](../05-implementation/Enterprise_AI_API_Gateway_M9_Hardening_Release_Candidate_Execution_Pack_V1.1.docx) govern Admin UI/session browser security, including same-origin defaults and explicit split-origin allowlists. Those rules do not grant CORS support to public Normal API routes. R24 does not change their scope or protections.

## 2. Public Normal API decision

M4 V1 public Normal API **does not provide browser CORS support**. This is an explicit application contract, independent of whether CORS middleware happens to be installed. Public Normal API traffic is intended primarily for authenticated server-to-server clients.

There is no public origin allowlist, credentialed browser-access mode, tenant/Service/Route CORS configuration, or automatic framework preflight handling in M4 V1. An otherwise valid request is not rejected merely because it includes an `Origin` header; it follows ordinary Gateway authentication, authorization, path validation, admission, and forwarding rules.

## 3. Three OPTIONS request categories

An OPTIONS request with `Origin` and `Access-Control-Request-Method`, optionally also `Access-Control-Request-Headers`, is preflight-shaped. It receives **no CORS exception**. The requested-method/header declarations are not a Gateway credential, do not authenticate the request, and do not replace ordinary resolution. It follows the same Gateway authentication and R19 method/path behavior as ordinary OPTIONS without those fields. Their presence does not cause an upstream attempt or a successful preflight response.

OPTIONS is not an executable persisted Normal API Route and is never proxied in M4 V1. The Admin method enum and database method CHECK continue to exclude it. Do not synthesize OPTIONS from an existing GET/POST or other Route, add a hidden OPTIONS handler, or allow a framework response to bypass authentication/resolution. Existing HEAD, TRACE, and CONNECT exclusions remain unchanged.

For requests that successfully authenticate and reach R19 resolution with valid path evidence and usable current state:

- Unknown/inaccessible Service or empty executable path-match set `P`: HTTP 404 `resource_not_found`, without `Allow`.
- Nonempty `P`: HTTP 405 `invalid_request`, because no executable OPTIONS Route exists. `Allow` contains only the distinct methods of executable ACTIVE Routes in that authenticated tenant/Service/path/snapshot set, serialized in R19's fixed order: GET, POST, PUT, PATCH, DELETE, omitting absent methods.

Authentication failures retain the existing authentication error contract; OPTIONS does not create a success or disclosure bypass. Other earlier path, current-state, or configuration failures retain their own frozen results. Do not search a different path, slash spelling, tenant, Service, or generation to construct `P` or `Allow`.

## 4. Response headers and R3 compatibility

The Gateway MUST NOT synthesize public Normal API CORS response headers, including `Access-Control-Allow-Origin`, `Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`, `Access-Control-Allow-Credentials`, `Access-Control-Expose-Headers`, or `Access-Control-Max-Age`. No additional CORS-specific `Vary: Origin` behavior or preflight cache policy is established.

R3 section 4 permits only its fixed upstream response allowlist: Content-Type, Content-Encoding, Content-Language, Content-Disposition, Cache-Control, Expires, ETag, Last-Modified, Vary, Location, Retry-After, Accept-Ranges, Content-Range, and Allow, subject to its syntax and hop-by-hop filtering rules. Upstream CORS response headers are absent from that list and therefore stripped under the already-frozen policy. A Route cannot expand the response allowlist. R24 does not redefine that policy.

Upstream `Vary`, including a valid `Vary: Origin`, remains eligible under R3; forwarding it does not create a CORS grant or authorize synthesizing one. Ordinary `Allow` remains distinct from `Access-Control-Allow-Methods`. Request `Origin` forwarding/filtering remains governed by R3's effective request allowlist and precedence; R24 neither automatically forwards nor newly forbids it. Query bytes retain R14 semantics and do not create a CORS identity or bypass.

## 5. Application and deployment ownership

The Gateway application owns the decision not to generate public Normal API CORS responses. Starlette/FastAPI `CORSMiddleware` or an equivalent preflight responder MUST NOT be mounted over those public routes. Later Admin UI/session CORS must remain scoped so it cannot intercept or grant cross-origin access to the public Normal API.

Under the M4 V1 deployment profile, an external ingress/reverse proxy MUST NOT introduce public Normal API CORS behavior without a separately approved contract or deployment-policy change. Operators must keep that profile consistent with this contract. The Gateway application cannot technically prevent independently managed external intermediaries from adding headers or answering requests; their behavior is distinct from the application's response semantics and cannot be claimed as application-provided M4 CORS support.

## 6. Authentication, correlation, and disclosure

There is no unauthenticated CORS preflight success path. Preflight-shaped requests preserve existing canonical request-ID, Gateway authentication, tenant binding, path validation, and Service/Route/current-state resolution requirements in their already-frozen order. Do not add an early response based on Origin, advertised headers, or advertised methods.

Canonical `X-Request-ID` remains the Gateway-generated UUID v7 on every Gateway-generated Normal API response where transport permits. A client-supplied value never replaces it. Errors retain the existing safe Gateway envelope and canonical request correlation. No raw credential, header value, route inventory, or internal destination is added to diagnostics or public error metadata.

No pre-authentication response may reveal tenant/Service/Route existence or derive a method/header inventory from their configuration. R19's authenticated `Allow` disclosure remains limited to its captured, usable tenant-scoped snapshot. A dirty/pending/unusable R18 state retains its existing fail-closed behavior; OPTIONS cannot substitute a stale inventory or a new database fallback.

## 7. Path validation, ordering, and side effects

Preflight-shaped OPTIONS passes through the same existing path boundary as ordinary Normal API requests: R1 separator/dot-segment validation, R21 raw-path authority and single canonical execution path, R22 consecutive-literal-slash rejection, and R23 NFC-only acceptance. R20's exact trailing-slash identity remains unchanged; no slash-correction redirect is introduced. No alternate CORS path normalization or decoded-scope fallback exists.

Preserve R7A/R19 ordering. OPTIONS rejected at authenticated method/path resolution consumes no Normal API rate token, acquires no concurrency handle, reads no request body, performs no circuit operation or completion, and performs no upstream I/O. The presence or absence of preflight headers does not change this accounting. Authentication and path-validation failures retain their respective earlier ordering and errors; no preflight-specific accounting, body-forwarding path, or protection bypass is added.

## 8. Errors and implementation boundary

Use the existing authentication failure contract for unauthenticated requests, R19's HTTP 404 `resource_not_found` for no accessible executable path, and HTTP 405 `invalid_request` plus deterministic `Allow` for a matched path without OPTIONS support. Earlier R20–R23/path/current-state failures retain their frozen status and envelope. Do not add CORS-specific error codes, alternate preflight statuses, framework-native bare preflight bodies, or successful preflight responses.

The current generic application HTTP-exception handler maps an unrecognized status such as framework 405 to 400. The standalone Control Plane CORS test demonstrates that generic behavior with no CORS grant. Neither overrides R19's public Normal API HTTP 405 contract; later runtime acceptance must prove the explicit R19 result and its preserved `Allow`, rather than rely on generic framework method handling. R24 changes no application helper or existing test.

## 9. Security limits and compatibility

CORS is a browser-origin access mechanism, not server-side authentication. Its absence does not prevent server-to-server use. Browser clients cannot rely on cross-origin JavaScript access to M4 V1 public Normal API responses. M4 V1 does not support exposing long-lived `gw_` credentials to browsers; same-origin or reverse-proxied browser architectures require independent design review before being claimed supported. Admin UI/session access remains independently governed by M8/M9.

R24 adds no database field, migration, tenant/Service/Route CORS setting, deployment CORS switch, method enum value, M3-R3A consumer, public error code, response envelope, or Normal API OPTIONS OpenAPI operation. OpenAPI's dynamic proxy extension already lists only the five executable methods. The API specification and R19 already define the required method-mismatch status and error vocabulary; no OpenAPI error-contract gap or schema change is introduced. Admin UI CORS and session policy remain unchanged.

## 10. Required later acceptance evidence

Actual application/ASGI integration must prove:

1. Authenticated GET without Origin retains ordinary behavior.
2. Authenticated GET with Origin follows ordinary Gateway processing.
3. Gateway-generated GET responses contain no synthesized CORS grant.
4. Unauthenticated preflight-shaped OPTIONS takes the existing authentication failure path.
5. Authenticated preflight-shaped OPTIONS to an executable Route path produces R19 HTTP 405 `invalid_request` and the correct deterministic `Allow`.
6. The corresponding request to an unknown Route path produces R19 HTTP 404 without `Allow`.
7. Ordinary OPTIONS without Origin/Access-Control-Request-Method uses the same R19 behavior.
8. Persisted OPTIONS creation remains prohibited by the existing Admin and database rules.
9. No preflight-shaped OPTIONS request reaches upstream.
10. No preflight-specific rate/concurrency admission occurs; ordinary R19 rejection consumes no rate token and acquires no concurrency handle.
11. R19 rejection reads no request body.
12. Such rejection performs no circuit operation or completion.
13. Gateway responses retain canonical `X-Request-ID` and the existing error envelope.
14. No framework CORS responder or bare OPTIONS response bypasses Gateway handling.
15. Unauthenticated requests disclose no tenant/Route existence or method inventory.
16. R20 trailing-slash identity remains unchanged.
17. R21 raw-path authority and one canonical path remain unchanged.
18. R22 duplicate-slash rejection remains unchanged.
19. R23 non-NFC rejection remains unchanged.
20. OPTIONS handling creates no unexpected 307/308 redirect.
21. Upstream CORS headers are stripped under R3, while eligible Vary/Allow and ordinary Origin request filtering retain their existing rules.
22. Admin UI/session CORS remains independently scoped, with no public-route interception or regression when implemented.
23. No OpenAPI, schema, migration, method-enum, or M3-R3A drift is introduced.

These are requirements for later implementation evidence, not claims that runtime acceptance was performed by this documentation change.

## 11. Deferred scope

Future opt-in browser CORS requires a separately approved contract defining origin policy, credential model, preflight authentication, route-disclosure protection, allowed/exposed headers, cache behavior, request-ID correlation, admission ordering, middleware ownership, deployment profile, and acceptance tests. R24 defines no future defaults, speculative fields, or configuration for that capability.
