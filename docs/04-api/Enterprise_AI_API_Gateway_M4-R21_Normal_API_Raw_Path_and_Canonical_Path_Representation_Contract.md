# M4-R21 — Normal API Raw-Path and Canonical Path Representation Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract selects the ingress evidence and execution-path representation for Normal API routing and forwarding. It does not implement a path adapter or change M4-R1's canonicalization rules.

## 1. Authority and scope

M4-R1 already owns public Service-prefix stripping, path normalization, path-pattern matching, and prefix-append upstream construction. It requires one strict UTF-8 percent-decoding/canonical-encoding pass, rejects malformed escapes, forbidden encoded or literal slash/backslash separators within a segment, dot segments (including encoded forms), and any path that could escape the configured base path or change authority; it forbids double decoding and preserves the trailing slash. M4-R19 uses the exact normalized effective path for path/method resolution. M4-R20 preserves that path's trailing slash without redirecting. M4-R14 separately owns raw query bytes. This contract chooses the input source and makes representation consistency explicit; it does not define a second normalization algorithm or widen R1's accepted/rejected character set.

## 2. Ingress evidence and canonical execution path

For a Normal API request, `scope["raw_path"]` is the authoritative **ingress evidence** for interpreting the encoded path. It is not itself the execution path. The Gateway performs exactly one M4-R1 validation, percent-decoding, and canonical-encoding pass on that evidence to produce one canonical Normal API path.

That single canonical path is the sole semantic representation for:

- Service-prefix interpretation and Service selection input;
- remaining-path derivation;
- Route path-pattern matching;
- the M4-R19 path-match set and its 404/405/`Allow` result;
- the M4-R20 trailing-slash identity; and
- M4-R1 prefix-append upstream path construction.

No independent decoded-path interpretation may influence one of those decisions.

## 3. `scope["path"]` and missing raw path

Decoded `scope["path"]` is not an authority for security-sensitive M4 path interpretation and MUST NOT override the canonical path derived from `raw_path`. Do not reconstruct raw evidence by percent-encoding `scope["path"]`; the original distinction is unrecoverable after decoding. The decoded field may be used only where framework integration or bounded diagnostics require it, and must not select a Service or Route or determine the forwarded path.

A usable `scope["raw_path"]` is required at the Normal API runtime boundary. If it is absent, `None`, unavailable, or otherwise cannot provide the encoded-path evidence needed to apply R1, do not fall back to `scope["path"]`. Fail closed with HTTP 500 `gateway_internal_error`; do not perform Service/Route execution. This is a local boundary/invariant failure, not a claim that the client sent an invalid path.

If both fields are present but have a relationship that the implementation cannot trust or reconcile with the pinned ASGI server behavior, treat this as a local invariant failure: return the same safe 500 `gateway_internal_error` before execution. Never prefer the decoded field or expose raw path bytes in the response.

## 4. Client-invalid path versus local representation failure

When raw-path evidence is present and the client path violates an existing M4-R1 rule, reject it as HTTP 400 `invalid_request`. This includes R1's existing malformed/unsafe percent encoding and forbidden encoded or literal segment-separator rules. `%2F` and `%2f` have the same security outcome: neither is allowed to become a segment boundary through `scope["path"]` decoding. An encoded separator in the Service component is rejected before Service lookup and cannot change the authenticated tenant's Service-prefix structure.

Keep these cases distinct:

1. The HTTP server/parser rejects the request target before creating an ASGI request: this is ingress rejection, not a Gateway response, and R21 does not claim Gateway controls it.
2. A request reaches the Gateway with raw-path evidence but violates R1: return the existing 400 `invalid_request`.
3. A request reaches the Gateway without usable raw-path evidence, or with an untrustworthy internal scope relationship: return the local 500 `gateway_internal_error`.

The existing public error vocabulary supports 400 `invalid_request` and 500 `gateway_internal_error`; no error code is added.

## 5. Exactly one canonical interpretation

Apply R1's one-time percent processing to `raw_path`, then use its canonical output. Never decode that output again for Route matching, URL construction, or transport. For example, any accepted `%252F` input must not be decoded twice into a structural slash. Serialization may encode the established canonical path for the connector, but may not semantically reinterpret it.

The original percent-escape spelling need not be preserved for path bytes. Path encoding follows M4-R1 canonical encoding; equivalent escape spellings may converge where R1 requires. This differs from M4-R14's byte-exact raw query guarantee.

## 6. Route and upstream representation consistency

Persisted `Route.path_pattern` remains in the normalized representation required by M4-R1. First derive the one R1 canonical request path from raw ingress evidence, then compare that result to the stored route pattern. Do not match using `scope["path"]` and later forward `raw_path`, or match a canonical raw-derived path and independently rebuild a different semantic path from `scope["path"]`.

The selected Route and the upstream request target must have identical path semantics: M4-R1 Model A appends the same canonical remaining path used for routing. The outbound connector may serialize that canonical path but must not decode escapes again, introduce separators, remove or collapse meaningful segments contrary to R1, change R20 trailing-slash identity, reinterpret path bytes as authority/scheme/query/fragment, or otherwise send a different path than the one used for routing.

Ordinary high-level HTTPX URL construction may normalize decoded string paths. The implementation must use an outbound construction path that preserves the established canonical path semantics. If a valid R1 canonical path cannot be represented faithfully by the selected connector, fail locally before upstream I/O with 500 `gateway_internal_error`; do not silently alter the target and do not relabel a connector limitation as client 400.

## 7. Query, authority, security, and ordering

`scope["raw_path"]` is path evidence only. `scope["query_string"]` remains M4-R14's separate opaque query representation; never append query bytes to raw path before R1 processing or derive query data from the canonical path. Encoded path data such as `%3F` or `%23` remains path data unless R1 explicitly rejects it; URL construction must not turn it into a query or fragment delimiter.

M4-R11 remains authoritative for destination security. Remaining path data cannot replace scheme, authority, hostname, port, or validated peer. Strings resembling `//host/path` or an absolute URL remain path data subject to R1's existing checks and cannot select a new authority.

Application-level raw-path evidence validation and the M4-R1 canonicalization required to identify the Service and Route occur in M4-R7A's Service/Route-resolution phase, after canonical request-ID handling, Gateway authentication, and tenant binding, and before rate admission, concurrency, body guard/read, circuit admission, or upstream I/O.

A failure at this ingress/resolution boundary — including missing or untrustworthy raw-path evidence or a client path that violates M4-R1 — consumes no rate token, acquires no concurrency handle, reads no body, performs no circuit operation, and performs no upstream I/O. A server rejection before ASGI naturally occurs outside this Gateway ordering.

After a valid canonical path and Route have been resolved, faithful outbound path construction and connector representability are deterministic local preparation under M4-R7A stage 6. If the valid canonical path cannot be represented without semantic mutation, return 500 `gateway_internal_error`. At that point successful rate admission is not refunded, any acquired concurrency handle receives exactly one provenance-aware release attempt, the bounded body guard has already completed, and neither circuit admission nor upstream I/O occurs.
Do not log unbounded raw path bytes or echo suspicious encoded input in public errors. Diagnostics remain bounded and follow existing telemetry/redaction contracts.

## 8. Preserved and deferred path semantics

R21 does not create new acceptance rules for path content. M4-R1's current rules remain controlling, including its rejection of literal/encoded dot segments and forbidden separators. R21 does not add policy for any issue R1 does not settle, including duplicate-slash acceptance/collapse, backslash treatment beyond R1's stated separator prohibition, or Unicode NFC/NFD normalization. Strict UTF-8 requirements remain as stated in R1; no additional Unicode normalization is introduced. CORS remains outside scope.

## 9. Compatibility and required later evidence

No new status, error schema, OpenAPI operation, database field, migration, Service/Route option, runtime setting, tenant setting, or M3-R3A consumer is added. Raw-path support is a required Normal API transport capability, not tenant configuration. Existing 400/500 public error families cover invalid client input and local inability to safely interpret/represent a path.

Later implementation acceptance must exercise actual Uvicorn/FastAPI/Starlette ingress and actual outbound HTTPX/connector request-target bytes. Prove: raw path is used as ingress evidence; decoded `scope["path"]` cannot override it; `%2F`/`%2f` are rejected by R1; encoded separators cannot alter Service selection; exactly one decode pass occurs; the same canonical path drives Service selection, Route matching, R19 404/405/`Allow`, R20 slash identity, and upstream serialization; `%252F` is not double-decoded; query remains separate; path cannot change authority; connector normalization cannot silently alter a valid canonical target; missing raw path and inconsistent scope fail with local 500 and no fallback; R1-invalid paths return 400; server-pre-ASGI rejection is distinguished.

For missing/unusable raw_path, inconsistent scope evidence, and R1-invalid ingress paths, prove no rate admission, concurrency acquisition, body read, circuit operation, or upstream I/O.

For a valid resolved canonical path that the outbound connector cannot represent faithfully, prove that rate admission has already occurred with no refund, an acquired concurrency handle is released exactly once, the body guard has completed, and there is no circuit admission or upstream I/O.

Also prove no unbounded raw-path logging or raw-path echo in public errors.
