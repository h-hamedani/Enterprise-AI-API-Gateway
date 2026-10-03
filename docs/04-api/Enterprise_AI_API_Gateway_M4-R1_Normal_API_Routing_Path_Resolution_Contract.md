# M4-R1 — Normal API Routing Path Resolution Contract

Status: frozen M4 V1 contract reconciliation. This document defines path resolution only; it does not implement the Normal API Engine.

## 1. Authority and narrowly scoped precedence

This decision reconciles the explicit prefix-strip path mapping in the [M4 Normal API Engine Execution Pack V1.1](../05-implementation/Enterprise_AI_API_Gateway_M4_Normal_API_Engine_Execution_Pack_V1.1.docx) with the required `upstream_path_template` representation in the [M2.6 Normal API Registry Contract Reconciliation](Enterprise_AI_API_Gateway_M2.6_Normal_API_Registry_Contract_Reconciliation.md). It is consistent with the public `/api/{service}/{remaining_path}` form and forwarding rules in the [API Specification V1.2.2 Final](Enterprise_AI_API_Gateway_API_Specification_V1.2.2_Final.docx) and [OpenAPI 3.1 V1.2 Final](Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml), and with the separate route matcher and service resolver in the [HLD V1.1 Corrected](../02-architecture/Enterprise_AI_API_Gateway_HLD_V1.1_Corrected.docx). The [ERD V1.2 Final](../03-data-model/Enterprise_AI_API_Gateway_ERD_Logical_Data_Model_V1.2_Final.docx), [Physical DB / Alembic Spec V1.2 Final](../03-data-model/Enterprise_AI_API_Gateway_Physical_DB_Alembic_Spec_V1.2_Final.docx), current `normal_api_services` / `normal_api_routes` models, and registry tests establish the stored fields and their integrity, not an executable rewrite language.

This contract supersedes only an interpretation that the stored route `upstream_path_template` overrides the M4 pack's prefix-append rule. It supplies the runtime meaning that M2.6 expressly left undefined. It does not supersede M2.6's HTTP representation, validation, persistence, route identity, or administration rules, nor unrelated M4 behavior.

## 2. Conflict and decision

The M4 pack says to strip `/api/{service}` and append the remaining request path to the configured service `base_url`, without an arbitrary rewrite engine. M2.6 makes `upstream_path_template` required on route creation and present in route responses, but says its reconciliation defines representation only and no new templating language. The non-null model column and existing registry fixture `/orders/{id}` → `/v1/orders/{id}` make the ambiguity observable: for `base_url=http://upstream:8080`, a request for `/api/orders/orders/123` could otherwise target either `http://upstream:8080/orders/123` or `http://upstream:8080/v1/orders/123`.

**Frozen choice: Model A — prefix append.** M4 V1 selects a service and route, then constructs the upstream path from the incoming remaining path, not from `upstream_path_template`. The latter remains required, persisted, and returned by the Admin API for schema/API compatibility and a possible later explicitly versioned capability. It is not used to match routes, rendered, used as a fallback, or permitted to alter the M4 V1 destination. Stored values, including `/v1/orders/{id}`, have no effect on M4 V1 destination construction. Activating them later requires a new contract; no implicit activation or dead-schema removal occurs here.

## 3. Three distinct operations

1. Authenticate the Gateway key and derive its tenant. Resolve one ACTIVE service by `(tenant_id, slug)`; the service UUID and `base_url` remain internal configuration.
2. Match the request's normalized *remaining public path* and HTTP method against ACTIVE `route.path_pattern` rows belonging to that service. Static segments are exact; named pattern parameters match one segment. The existing Admin overlap gate must make the result deterministic. Multiple matches fail closed as a configuration error. `upstream_path_template` does not participate in matching. No route ID, route name, or caller-selected upstream URL is a public routing input.
3. Independently construct the upstream destination from the resolved service `base_url` and the normalized remaining incoming path. Captured route parameters are for matching only; they are not substituted into `upstream_path_template`.

## 4. URL and query construction

The administrator-configured `base_url` supplies the scheme, authority, port, and optional base path. It must be a validated `http` or `https` URL with no userinfo, query, or fragment. The caller cannot supply or replace any of those components. Treat its path as a directory prefix: remove its trailing slash for joining, take exactly one leading slash from the remaining request path, and insert exactly one separator. Preserve meaningful internal path segments and a trailing slash in the incoming remaining path. An empty/root remaining path maps to the base path with one trailing slash. Do not use URL-resolution operations in which a leading `/`, `//`, `?`, or absolute URL can replace the configured authority or discard the configured base path. A leading `//` in the remaining path is invalid, not an authority.

The query string is separate from route matching and path joining. Forward its original encoded sequence, including duplicate keys and ordering where the HTTP stack permits; never use it as a template variable. The base URL itself supplies no query or fragment. Do not append a fragment.

Normalize and validate the public path once at the ASGI boundary before matching and forwarding; never double-decode it. Use a strict UTF-8 percent-decoding/canonical-encoding policy, reject malformed escapes, encoded or literal slash/backslash within a segment, dot segments (`.`/`..`, including encoded forms), and any form that could escape the configured base path or change authority. Encode each accepted segment when constructing the outbound path; do not treat percent-encoded text as a second instruction. For example, an accepted `%20` in a segment remains a space encoded as `%20` in the destination. These checks are independent of the shared M4 outbound destination/SSRF validator, which remains mandatory immediately before connect.

No general-purpose path-template interpreter, regex rewrite, scheme/authority override, arbitrary caller-supplied absolute URL, open-proxy mode, or secret interpolation is introduced. Upstream redirects remain non-followed under the M4 contract.

## 5. Normative examples

| Case | Configuration and request | M4 V1 destination |
| --- | --- | --- |
| Stored template differs | `base_url=http://upstream:8080`; route `/orders/{id}`; stored `upstream_path_template=/v1/orders/{id}`; request `/api/orders/orders/123` | `http://upstream:8080/orders/123` — **not** `/v1/orders/123` |
| Base trailing slash | `base_url=https://backend.internal/v2/`; request `/api/billing/customers/42` | `https://backend.internal/v2/customers/42` |
| Query multiplicity | `base_url=https://backend.internal/v2`; request `/api/billing/customers?tag=a&tag=b` | `https://backend.internal/v2/customers?tag=a&tag=b` |
| Encoded segment | `base_url=https://backend.internal/v2`; request `/api/billing/files/a%20b` | `https://backend.internal/v2/files/a%20b` |
| Path escape | `base_url=https://backend.internal/v2`; request `/api/billing/../admin` or `/api/billing/%2e%2e/admin` | Reject before upstream I/O; never escape `/v2` |

The examples assume an ACTIVE matching route and valid authorization. They illustrate destination construction, not a bypass of those prerequisites.

## 6. Compatibility and implementation consequences

No schema migration or OpenAPI change is required: `upstream_path_template` remains non-null on create and stored/returned exactly as M2.6 requires. Existing M2 Admin API and fixture values remain valid but do not change the M4 V1 destination. M3 Normal circuit identity remains tenant + route + upstream target; this decision changes neither identity nor circuit state.

M4 implementation will need a tenant-scoped service/route resolver, a prefix-append URL builder with the validation above, tests proving stored templates cannot redirect requests, query/encoding/traversal and base-path join tests, and documentation linking this contract. Route matching, URL construction, and outbound destination validation must be separate testable steps. No runtime code, test, schema, migration, or OpenAPI modification is made by this reconciliation.
