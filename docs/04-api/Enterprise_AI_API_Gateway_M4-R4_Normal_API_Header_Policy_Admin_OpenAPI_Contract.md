# M4-R4 — Normal API Header Policy Admin API / OpenAPI Contract

Status: frozen API/OpenAPI reconciliation for M4-R3. Documentation only; no Admin API, runtime, test, model, migration, or OpenAPI implementation is made here.

## 1. Authority and current behavior

[M4-R3](Enterprise_AI_API_Gateway_M4-R3_Normal_API_Header_Policy_Contract.md) is the runtime authority: route-only `header_policy`, NULL safe default, one-key non-null shape, case-insensitive names, mandatory strip rules, and no route-configured literal headers. [M2.6](Enterprise_AI_API_Gateway_M2.6_Normal_API_Registry_Contract_Reconciliation.md) made the field nullable and representable but expressly left its keys and runtime meaning open. The [API Specification V1.2.2 Final](Enterprise_AI_API_Gateway_API_Specification_V1.2.2_Final.docx) and [OpenAPI 3.1 V1.2 Final](Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml) retain the existing route Admin operations; this addendum narrows only their `header_policy` property. It does not alter M4-R1 routing or M4-R2 circuit outcomes.

Today `RouteCreate.header_policy` and `RoutePatch.header_policy` are `dict | None`; create omission defaults to None, and PATCH uses `model_dump(exclude_unset=True)`, so omission preserves the stored value while explicit null clears it. The registry persists the supplied dictionary without semantic validation and returns the stored value. OpenAPI `RouteCreate`/`RoutePatch` declare object-or-null with `additionalProperties: true`; `Route` responses inherit the broad representation. `{}`, unknown keys, wrong nested value types, duplicate/mixed-case names, and `{"mode":"metadata-only"}` are therefore presently accepted as dictionaries, although they do not have valid M4-R3 runtime meaning. An existing registry integration fixture uses that last object. The nullable JSONB column remains sufficient.

## 2. Exact external shape and normalization

`header_policy` is either `null` or exactly:

```json
{"request_allowlist":["accept","content-type","user-agent"]}
```

The non-null object has exactly one required property, `request_allowlist`. Its value is an array of strings, including an empty array. No other property, static header value, secret reference, denylist, response policy, or rewrite instruction is accepted. `{}` and `{"request_allowlist":null}` are invalid. Each name must be a non-empty ASCII HTTP field-name token (`!#$%&'*+-.^_`|~` and ASCII letters/digits); leading/trailing whitespace, colon, controls, Unicode, or an empty string are invalid. Input names may use any ASCII letter case, but are lowercased before storage and returned in lowercase. Preserve input list order after canonicalization; ordering has no forwarding precedence. Reject duplicates after lowercase comparison rather than silently deduplicating (`["Accept","accept"]` is invalid).

An empty `request_allowlist` is valid and **replaces** the M4-R3 default with no caller application headers. It does not suppress Gateway-generated `X-Request-ID` or managed upstream credentials. Non-null configured lists always replace, never extend, the default.

Reject any name in M4-R3's immutable caller-strip set: `authorization`, `proxy-authorization`, `proxy-authenticate`, `host`, `content-length`, `cookie`, `forwarded`, every `x-forwarded-*`, `x-request-id`, every `x-gateway-*`, `connection`, `keep-alive`, `te`, `trailer`, `transfer-encoding`, and `upgrade`. These checks are case-insensitive after canonicalization. A `Connection` header can nominate an otherwise valid application name dynamically; Admin validation cannot predict that request-specific nomination, and runtime must still strip the nominated field. M4-R3 **does permit explicitly allowlisted sensitive custom names** such as `x-api-key` or `x-auth-token` when not otherwise mandatory-denied; this addendum does not broaden the forbidden set to ban them. If a name equals the active managed STATIC_HEADER credential name, M4-R3's later injection still discards the caller value. This rule is explicit, not a silently ignored policy.

## 3. Create, PATCH, response, and error semantics

| Operation/input | Contract result |
| --- | --- |
| CREATE omits `header_policy` | Persist SQL NULL; response includes `header_policy: null`; M4-R3 safe default applies. |
| CREATE sends explicit `null` | Same stored/resulting NULL as create omission. |
| CREATE sends valid object | Validate, lowercase names, persist canonical object, return that object. |
| PATCH omits `header_policy` | Leave stored policy unchanged; no implicit reset. |
| PATCH sends explicit `null` | Persist SQL NULL; return `header_policy: null`. |
| PATCH sends valid object | Replace entire prior policy, not merge keys/lists; persist/return canonical object. |

All route resource responses include `header_policy`; omission is not a response form. A valid non-null policy is returned with the exact one-key shape and lowercase names. Normal API route creation/patching retains its existing audit/config-version/invalidation transaction behavior; this contract adds no new operation or idempotency rule.

Malformed new input is rejected before mutation with the existing Admin `400 invalid_request` envelope: safe `message` “Invalid request.”, `type`/`code` `invalid_request`, canonical `request_id`, and `X-Request-ID`. The existing validation handler derives `param` from the failing request location when available; neither this addendum nor OpenAPI freezes a per-array-index message or a new validation-details framework. Unknown keys, wrong types, forbidden names, duplicates, invalid tokens, and `{}` are all malformed input. A rejected mutation writes no route change, audit success, config-version increment, or invalidation event.

## 4. Legacy persisted data and validation boundaries

Validate all **new create and PATCH candidate writes** at the Admin boundary. A PATCH that omits `header_policy` must also verify that the resulting full route policy remains M4-R3-valid; if the existing row contains an invalid legacy object, it fails closed without mutation. A PATCH that explicitly replaces that object with a valid object or NULL may repair the row transactionally. No stored policy is silently normalized, dropped, or rewritten merely because it was read.

Admin GET/LIST of a row with an invalid legacy non-null policy cannot claim a strict `Route` response schema or return the invalid object as valid. Return a safe `500 gateway_internal_error` for that read, without echoing the policy contents or raw exception. M4 runtime also fails closed before upstream I/O under M4-R3. Operators must inventory such rows and repair them through an audited explicit route PATCH (when route ID is known) or a separately authorized maintenance procedure before enabling M4 traffic. The existing `{"mode":"metadata-only"}` test fixture proves accepted historical representation, **not** that a deployed database contains that value. This task makes no data change or migration.

NULL remains compatible. `{}` and arbitrary objects are incompatible until explicitly repaired. An already stored one-key list with valid names can be canonicalized only by an explicit write; pre-existing mixed-case or duplicate forms are not silently converted on read. This conservative boundary keeps Admin responses, runtime decisions, and authoritative stored state aligned.

## 5. OpenAPI/API change required later

In the frozen OpenAPI 3.1 source, narrow `RouteCreate.header_policy`, `RoutePatch.header_policy`, and the inherited/explicit `Route` response property consistently. The intended logical schema is:

```yaml
header_policy:
  type: [object, 'null']
  required: [request_allowlist] # applies when the instance is an object
  additionalProperties: false
  properties:
    request_allowlist:
      type: array
      items:
        type: string
        minLength: 1
        pattern: "^[!#$%&'*+.^_`|~0-9A-Za-z-]+$"
      uniqueItems: true
```

The item pattern is descriptive of HTTP token syntax; implementation must use an equivalent correct token validator. JSON Schema `uniqueItems` is case-sensitive and cannot by itself express case-insensitive uniqueness, mandatory-deny names/prefixes, or canonical lowercase output. The API validator must enforce those additional M4-R3/R4 rules, and the OpenAPI description must state them. The schema permits NULL but not `{}`; create may omit the outer optional field, while PATCH omission leaves the stored value unchanged. The public API Specification needs only a narrow addendum/cross-reference for this route-policy shape and validation; no unrelated endpoint or response semantics change. **READY FOR API/OPENAPI IMPLEMENTATION** of this reconciliation, not ready for M4 proxy runtime until it is applied.

The existing nullable `normal_api_routes.header_policy` JSONB remains unchanged: no table, constraint, enum, or Alembic migration is required. The route remains the single config resource; M3-R3A route invalidation and authoritative current-state reconciliation apply to policy changes, with no separate consumer.

## 6. Security, tests, and narrow supersession

No policy can enable forwarding the Gateway `gw_` Authorization, caller Host, hop-by-hop/framing fields, forged forwarding-chain/correlation fields, or a caller override of managed upstream credentials. `header_policy` contains names only, never literal values or secret material. Rejection/legacy-failure logs and audit metadata must not include sensitive header values, credentials, raw policy values, or uncontrolled exception text; M3-R13 remains authoritative.

Future API/OpenAPI tests must cover create omitted/null/valid/empty/`{}`/unknown key/wrong type/forbidden name/invalid token/case-fold duplicate; mixed-case input stored and returned lowercase; PATCH omitted/null/valid replacement; a legacy invalid row on GET/LIST and unrelated PATCH; explicit repair PATCH; exact OpenAPI null-or-object/required-key/no-additional-properties schema; and no audit/config-version/invalidation on invalid writes. Current tests that accept `{"mode":"metadata-only"}` must be revised in that future implementation task, not here.

Likely later implementation files are `app/schemas/control_plane.py`, `app/control_plane/normal_api_registry.py`, `app/api/control_plane/normal_api_registry.py` only if handler mapping needs adjustment, `docs/04-api/Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml` plus its validation artifact, relevant API addendum, and Normal API registry unit/integration/OpenAPI contract tests. M4-R4 supersedes only the previously unconstrained Admin representation/validation of route `header_policy` in M2.6 and the affected OpenAPI properties. It does not redefine M4-R3 forwarding, M4-R1 paths, M4-R2 circuit classification, M3 protection, or unrelated API behavior.
