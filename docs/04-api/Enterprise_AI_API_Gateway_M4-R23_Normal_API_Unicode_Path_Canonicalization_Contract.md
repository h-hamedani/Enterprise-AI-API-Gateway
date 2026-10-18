# M4-R23 — Normal API Unicode Path Canonicalization Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract defines Unicode canonical-equivalence handling for the public Normal API path. It narrows the set of paths accepted by M4-R1 without changing the single-path model established by M4-R21. It does not implement the runtime or modify Admin validation, OpenAPI, or persistence.

## 1. Authority and scope

M4-R1 owns strict UTF-8 percent-decoding/canonical-encoding, path validation, Service-prefix stripping, exact Route matching, and Model-A prefix append. M4-R19 owns authenticated path/method results (404, 405, and `Allow`); M4-R20 owns trailing-slash identity; M4-R21 makes `scope["raw_path"]` the ingress evidence for one R1 canonicalization pipeline and one canonical execution path; M4-R22 rejects consecutive literal slashes. R1's literal and encoded slash/backslash rejection is unchanged.

R21 explicitly left NFC/NFD policy unsettled. R23 closes only that gap and takes precedence over that earlier deferral: M4 V1 accepts decoded Unicode path content only when it is already in Unicode Normalization Form C (NFC). All other R1/R19–R22 semantics remain in force.

This is a Gateway M4 V1 path-identity safety rule. It is not a claim that HTTP universally requires NFC and does not address visual similarity or broader Unicode equivalence.

## 2. NFC conformance: reject, do not rewrite

After R1 has validated and strictly decoded the path's UTF-8 percent-encoded content, the Gateway MUST verify NFC conformance of the decoded Unicode path. For decoded path string `p`, the check is:

```python
unicodedata.normalize("NFC", p) == p
```

If the check passes, path processing may continue. If it fails, reject the client path; do not transform it to NFC, search for an equivalent NFC path, or use a normalized value for Service or Route selection.

For example, a precomposed NFC character such as `é` may proceed. A canonically equivalent decomposed sequence such as `e` followed by a combining acute accent is rejected when it is not NFC. The Gateway does not make these two spellings separately routable identities and does not match one spelling while forwarding the other.

This rejection policy avoids both silent path rewriting (which would obscure what the caller sent and diverge from Route identity) and independently routable NFC/NFD forms that downstream servers, frameworks, applications, filesystems, or business layers might treat as the same resource. NFC-only acceptance is the M4 V1 rule; no broader claim about HTTP is made.

## 3. One canonicalization pipeline

Preserve R21's one authoritative representation. Conceptually, process the raw path as follows:

```text
scope["raw_path"]
→ R1 percent-escape validation
→ exactly one percent-decoding operation
→ strict UTF-8 decoding
→ R1 segment/path validation
→ R22 consecutive-literal-slash validation
→ R23 NFC conformance check (assertion only; no conversion)
→ R1 canonical outbound encoding
→ one canonical execution path
```

The NFC check is part of establishing that one path; it is not a second normalization or routing pass. Do not transform a non-NFC value and rerun route normalization. Do not use decoded `scope["path"]` as an alternate source or Unicode-normalized path.

R1 remains authoritative for percent syntax, invalid UTF-8, separators, dot segments, base-path escape, and outbound encoding. Invalid or undecodable UTF-8 remains an R1-invalid client path; never invoke Unicode normalization on undecodable input. Percent-escape case or spelling is not a Unicode route identity. Equivalent valid UTF-8 percent-encoding spellings that decode to the same NFC Unicode scalar sequence use the same canonical execution path under R1. This does not alter M4-R14's separate raw-query preservation rule.

Uvicorn/h11 may reject an unescaped non-ASCII request-target before ASGI. R23 does not claim that such wire bytes reach the Gateway. Where an ingress stack can safely represent equivalent input, Gateway identity is based on the decoded Unicode value established from R21's raw-path evidence, not on the spelling of its UTF-8 octets.

## 4. No compatibility or presentation normalization

R23 uses only NFC conformance. It MUST NOT apply NFD, NFKC, NFKD, compatibility folding, width folding, case folding, accent stripping, homoglyph substitution, or transliteration. Compatibility-equivalent characters remain distinct unless another frozen contract says otherwise. NFC does not make `A` and `a` equivalent, and R1's exact static-segment comparison remains case-sensitive. No locale-aware comparison is introduced.

R23 does not establish policy for homoglyphs, confusables, mixed-script identifiers, IDN hostnames, language-specific equivalence, or other Unicode security concerns.

## 5. Service and Route identity

Apply NFC conformance to the entire decoded public path before Service-prefix interpretation. The current Service slug schema permits only lowercase ASCII letters, digits, and hyphens, beginning with a lowercase letter or digit; that stricter existing rule is preserved, and R23 does not permit Unicode Service slugs. A non-NFC public path is rejected before any Service lookup and cannot select an NFC, NFD, or alternative Service.

Apply the same NFC check to the remaining path used for Route matching. A non-NFC static path segment is rejected before Route lookup. R1's exact static-segment comparison remains authoritative; Unicode normalization is not performed during matching.

The current Admin `RouteCreate`/`RoutePatch` schema accepts a leading-slash string and does not enforce NFC. The current static OpenAPI likewise represents `path_pattern` as a string without a Unicode-normalization constraint. The persistence model stores `path_pattern` as supplied; it does not normalize the value. R23 does not claim that Admin rejects non-NFC input, does not tighten Admin/OpenAPI validation, and does not rewrite or migrate stored values. Any future requirement that new or updated Route patterns be NFC needs a separate Admin/OpenAPI reconciliation.

Do not rely on database collation or uniqueness to implement canonical equivalence. R23 defines public runtime identity at the application contract boundary; it does not assert that a SQL collation normalizes NFC/NFD.

## 6. Legacy stored Route patterns

Do not normalize, rewrite, or mutate existing `Route.path_pattern` values. A legacy non-NFC pattern cannot authorize non-NFC public ingress because that ingress is rejected before R19 constructs its path-match set. The Gateway MUST NOT normalize an NFC request into a legacy stored spelling, perform an equivalence search, or let a non-NFC legacy pattern globally poison an otherwise valid Service absent another frozen configuration rule.

If only a legacy non-NFC pattern exists, an NFC request that does not exactly match an executable NFC pattern proceeds through ordinary R19 semantics for the valid canonical path (including the applicable 404/405 distinction). Do not fabricate an NFC replacement Route. Existing stored values remain unchanged.

## 7. R19–R22 interaction and error behavior

Construct R19's tenant/service/snapshot-scoped path-match set only after the request has usable R21 raw-path evidence and has passed R1 validation, R22 duplicate-slash validation, and R23 NFC conformance. A non-NFC ingress path is rejected before R19; it does not produce a 404, 405, or `Allow` based on either the supplied spelling or a canonically equivalent spelling.

R23 does not change R20 trailing-slash identity, add or remove a slash, or create a redirect. It does not collapse slash structure; R22's duplicate-literal-slash rejection remains unchanged. The one accepted canonical path used for Route matching remains the path semantics used for R1 prefix-append construction and R21 outbound serialization. Never match on NFC and forward an original non-NFC form, or match on one interpretation and send another.

A valid UTF-8 path that fails the NFC conformance check is invalid M4 V1 client path syntax and returns the existing HTTP 400 `invalid_request` envelope. Do not return 404, 405, a redirect, or 500 for an ordinary received non-NFC violation. A server rejection before ASGI remains outside Gateway response control. If a valid canonical NFC path cannot be faithfully represented by the outbound connector, R21 Stage 6 remains authoritative: HTTP 500 `gateway_internal_error`, without silently normalizing or changing the path.

## 8. Admission ordering and side effects

NFC conformance is required to establish canonical Service/Route identity and is part of M4-R7A Service/Route resolution (stage 2), after canonical request-ID handling, Gateway authentication, and tenant binding. A non-NFC rejection occurs before Service/Route execution using that form, rate admission, concurrency acquisition, request-body read, circuit admission, and upstream I/O. It consumes no rate token, acquires no concurrency handle, reads no body, performs no circuit operation, and performs no upstream I/O.

Connector representability remains R21 Stage 6, with its distinct previously frozen side effects; R23 does not alter them.

## 9. Compatibility and required later evidence

R23 adds no public error code, response envelope, dynamic Normal API operation, database column, migration, Service/Route field, tenant setting, or M3-R3A consumer. The public rejection uses the existing 400 `invalid_request`. No static/generated OpenAPI change is expected for this runtime rule. Admin validation remains as currently contracted; if NFC validation is later required for new or updated Route patterns, reconcile that separately before changing Admin schemas or published OpenAPI.

Later implementation acceptance must use the actual Uvicorn/FastAPI/Starlette ingress and actual outbound connector. It must prove at least:

- ASCII path behavior is unchanged;
- an NFC character succeeds in direct client URL input where the client serializes it as UTF-8 percent encoding, and the equivalent UTF-8 percent-encoded spelling yields the same canonical execution path;
- a valid UTF-8 NFD path is rejected with 400 before Service/Route lookup and before rate, concurrency, body, circuit, or upstream side effects;
- NFC and NFD `Route.path_pattern` values are not treated as canonically equivalent, legacy non-NFC patterns are not rewritten or made executable by normalization, and valid NFC requests receive ordinary R19 outcomes when no exact executable NFC Route matches;
- R19 404/405/`Allow`, R20 trailing-slash identity, and R22 duplicate-slash rejection remain unchanged;
- the exact accepted canonical NFC path semantics are used for outbound serialization, with R21 Stage-6 behavior on connector-only representability failure;
- Uvicorn pre-ASGI rejection of raw non-ASCII request-target bytes is distinguished from Gateway validation; and
- there is exactly one percent-decoding pass and no hidden NFC conversion or second normalization pass.

ADMIN VALIDATION RECONCILIATION REQUIRED. The current Admin contract permits Route.path_pattern values without NFC validation, while M4-R23 makes non-NFC public path identities non-executable. A separate Admin/OpenAPI reconciliation must determine validation for newly created or updated Route patterns. R23 does not change that Admin contract, rewrite existing rows, or block freezing the public runtime rule.

Required later acceptance evidence must also prove that:

a rejected non-NFC/raw path is not echoed in the public error envelope; and

raw or decoded path content is not logged without the existing bounded telemetry/redaction controls.

No runtime code, tests, OpenAPI, schema, migration, or Admin validation is changed by this reconciliation.
