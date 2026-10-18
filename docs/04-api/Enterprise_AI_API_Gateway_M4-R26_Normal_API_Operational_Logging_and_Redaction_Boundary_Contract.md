# M4-R26 — Normal API Operational Logging and Redaction Boundary Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract defines the supported Normal API operational logging profile, request-event ownership, and redaction boundary. It implements no logger, middleware, test, deployment configuration, OpenAPI change, or migration.

## 1. Authority and narrow reconciliation

[M3-R13](Enterprise_AI_API_Gateway_M3-R13_Observability_Security_Boundary_Contract.md), sections 2–3, requires bounded diagnostics safe for a general operational destination, permits canonical request-ID correlation, and assumes no protected identifier-bearing sink. The [M7 execution pack](../05-implementation/Enterprise_AI_API_Gateway_M7_Observability_Retention_Operations_Execution_Pack_V1.2.docx), sections 5–6, defines structured logging fields and bounded metrics. Its conditional permission for identifiers in protected logs does not establish such a sink. R26 creates none and therefore grants none of those conditional identifier permissions. M3-R13's permission for a canonical UUID v7 request ID is compatible with M7's request-scoped correlation requirement; no request-ID conflict was found.

[M4-R14](Enterprise_AI_API_Gateway_M4-R14_Normal_API_Query_String_Forwarding_Contract.md) and [M4-R21](Enterprise_AI_API_Gateway_M4-R21_Normal_API_Raw_Path_and_Canonical_Path_Representation_Contract.md) require raw query/path evidence for execution, not logging. [M4-R17](Enterprise_AI_API_Gateway_M4-R17_Normal_API_Response_Streaming_Commit_Contract.md) distinguishes upstream completion, downstream commitment, and actual delivery termination. [M4-R18](Enterprise_AI_API_Gateway_M4-R18_Normal_API_Current_State_Consumer_Contract.md) owns immutable execution identity; [M4-R25](Enterprise_AI_API_Gateway_M4-R25_Normal_API_Request_Record_Coverage_Contract.md) owns terminal-only PostgreSQL request telemetry. [M4-R7A](Enterprise_AI_API_Gateway_M4-R7_Normal_API_Protection_Admission_Ordering_Contract.md) and [M4-R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md) retain admission, cancellation, and cleanup precedence.

R26 supplies the application/server/dependency/deployment logging boundary left unspecified by those documents. It does not weaken confidentiality, add a protected sink, select a logging backend, or change request execution, metrics, or durable audit contracts.

## 2. Supported deployment profile

M4 V1 uses **APPLICATION_OWNED_STRUCTURED_LOGGING**. This is a conceptual supported deployment profile, not a persisted enum, environment setting, database field, tenant option, or API property.

The Gateway application owns the primary request-scoped Normal API operational outcome event. ASGI servers and dependencies must not independently publish unsafe request targets, client identities, credentials, or transport content. Application exception sanitization alone does not establish compliance: the same minimization boundary must cover enabled server/client/dependency loggers and deployment ingress.

## 3. Primary event coverage and ownership

For each Normal API request reaching the Gateway application and reaching an observable terminal execution/delivery boundary, the application MUST attempt one bounded structured primary outcome event. Coverage includes successful execution, authentication/tenant-binding rejection, invalid ingress path, Service/Route rejection, R19 404/405, protection rejection, upstream failure, completed delivery, and observed cancellation/interruption. Events excluded from R25 durable NORMAL rows remain covered by this operational rule where their terminal signal is observed.

One request-local logging owner coordinates the primary event. Competing terminal signals and nested error/cleanup callbacks must not emit conflicting or duplicate primary outcomes. There is at most one primary emission attempt for an observed terminal request lifecycle. Additional bounded security/system diagnostics permitted by existing contracts may describe a separate condition, but do not override or re-emit that primary outcome. No new persistent event ID is introduced.

This is operational telemetry, not a PostgreSQL record, administrative audit event, or exactly-once durable delivery guarantee. Abrupt process termination or logging failure can prevent emission; do not claim crash-durable coverage or successful log delivery merely because an emission was attempted. Requests rejected by an HTTP parser before application invocation are outside application-event coverage and remain governed by the server/deployment minimization requirements.

## 4. Structured fields and correlation

Use M7's existing field meanings with a closed, safe application vocabulary:

| Field | M4 Normal API rule |
| --- | --- |
| `timestamp` | UTC RFC3339 event timestamp. |
| `level` | Bounded standard severity. |
| `event` | Stable machine-readable name from a finite application-defined vocabulary; never derived from a path, exception, or resource identifier. |
| `request_id` | The established server-generated canonical UUID v7, when available. |
| `plane` | M7's `DATA_NORMAL` classification. |
| result/status | Bounded actual terminal outcome; observed HTTP status where applicable does not by itself assert successful delivery. |
| `error_code` | Existing normalized error/outcome category where applicable; no raw exception or new public error. |
| `duration_ms` | Numeric actual request duration where measurable, following the existing terminal lifecycle and excluding later logging work. |

Do not introduce arbitrary metadata, a trace ID, a new correlation system, or resource-bearing event names. Do not fabricate an HTTP status where cancellation yields no response. An already committed HTTP 200 may coexist with an interrupted-delivery outcome and must not be reported as fully delivered success. Omit unavailable optional observations rather than infer them.

When established, the same canonical request ID correlates the primary event, the Gateway response where transport permits, R25's record where eligible and persisted, and existing authorized request correlation such as the R3 upstream header. A client-supplied correlation value never replaces it or automatically becomes a log field. If execution fails before canonical request-ID establishment, do not retrospectively fabricate one for this event or invent Route attribution. No universal distributed tracing requirement is added.

## 5. Identifier and content minimization

General runtime logs MUST NOT contain tenant, Service, Route, API-key, credential, Admin, provider, model, lease, probe, generation, or arbitrary resource identities; raw caller/client addresses; Redis keys or HMAC client-identity digests; raw request targets; or upstream URLs. Hashing an identifier to retain identifying logs is not a permitted workaround. Canonical request ID is the explicitly authorized correlation exception and remains prohibited as a metric label.

Never emit `gw_`/`adm_` plaintext, managed upstream/provider secrets, decrypted material, encrypted credential blobs, Authorization/Cookie values, sensitive headers, secret-bearing configuration, request/response bodies, raw query bytes/text, raw-path evidence, or uncontrolled decoded paths. Do not emit internal destinations, database SQL parameters, Redis payloads, or raw database/Redis/HTTPX exception messages or traces. Truncating an unsafe path, query, credential, or exception does not make it safe. Use finite categories and bounded safe diagnostic codes instead of `str(exc)`, `repr(exc)`, arbitrary metadata, or transport payloads.

Raw values needed internally for R14/R21 execution are not authorized diagnostic fields. Public errors retain their existing safe envelope; an internal log destination does not grant broader disclosure under this profile. Existing M3 worker/reconciliation diagnostics remain bounded; R26 does not establish a second logging authority for them.

## 6. ASGI server logging

The inspected lock uses Uvicorn **0.52.4**. Its ordinary access log defaults on and emits client address plus path/query/status through `uvicorn.access`; both HTTP implementations use the request-target helper. Under this supported M4 profile, ordinary Uvicorn access logging MUST be disabled through the server's existing supported access-log configuration mechanism. A deployment retaining stock access logging is not compliant. The application-owned primary event supplies the supported request-outcome coverage.

This does not prohibit safe bounded startup/shutdown/server lifecycle diagnostics. However, disabling access logging alone is insufficient: Uvicorn's ASGI exception path can emit `exc_info` through its server/error logger. Enabled server error/exception reporting must suppress unsafe exception content or pass only sanitized bounded diagnostics. It must not re-expose raw exceptions already suppressed by an application error handler. Do not blanket-enable ASGI scope/message tracing or assume a safe HTTP error response implies a safe server traceback.

## 7. HTTP client and dependency boundary

The inspected lock uses HTTPX **0.28.1** and httpcore **1.0.9**. HTTPX's `httpx` INFO records include the request URL. httpcore's `httpcore.*` DEBUG trace path can format transport arguments, return values, and exceptions; trace callbacks are also a separate exposure boundary. Under this profile, unsafe INFO/DEBUG transport output MUST be suppressed or sanitized before emission. Merely relying on a current default log level is insufficient. Safe normalized application-level diagnostics remain permitted. An upgrade requires verification of these boundaries; current defaults are not a permanent confidentiality guarantee.

The inspected dependencies also include SQLAlchemy **2.0.52**, psycopg **3.3.5**, and redis-py **8.1.0**. SQLAlchemy engine/pool INFO/DEBUG/exception paths can expose SQL, parameters, and connection exceptions; disabling engine echo alone does not sanitize every logger. The current engine creation does not explicitly enable echo, but that observation is not deployment compliance evidence. The supported profile MUST NOT enable SQL echo, sensitive pool/client debugging, raw exception reporting, or connection-URI output. Apply the existing M3-R13 rules to PostgreSQL/Redis clients and background configuration workers; no SQL parameters, credentials, Redis keys, or raw dependency messages may escape through another logger or exception reporter at any severity.

The Gateway's logging/deployment configuration is one coordinated enforcement boundary. Its implementation may use supported logger configuration and safe filters/adapters, but must verify logger propagation and handlers rather than rely solely on application `try/except` blocks. This contract selects no new logging library, handler backend, or configuration field.

## 8. External ingress responsibility

The application cannot sanitize independently generated reverse-proxy, load-balancer, WAF, or ingress-controller records. An M4 V1 deployment may be declared logging-compliant only when those independently managed access/diagnostic logs follow the same minimization boundary or are disabled. Deployment acceptance must verify that condition and record responsibility for it. Do not infer compliance from application tests, introduce vendor-specific settings here, or claim central technical control over external infrastructure.

## 9. Terminal lifecycle, R25, and failure isolation

The primary event reflects the applicable winning R9/R17 terminal request outcome, including downstream delivery. Headers, downstream response-start, or a first byte alone are insufficient. Upstream completion may already have fixed a circuit result while downstream later aborts; record the delivery interruption without rewriting that circuit result. Valid zero-byte/body-forbidden responses retain R16/R17 completion rules. Logging never manufactures successful delivery or a new upstream failure.

Logging must preserve R7A/R9 cleanup and must not delay or suppress concurrency release or renewal-worker cleanup. Freeze the result from the existing request lifecycle; do not re-resolve mutable configuration for diagnostics. R18 snapshot identity remains authoritative wherever attribution is otherwise permitted, but does not authorize identifiers in these general logs.

R25 remains the sole durable NORMAL request-record coverage and terminal-only insertion contract. A pre-Route outcome log does not create a `requests` row. A Route-attributed primary event does not prove database insertion succeeded. The primary event need not wait for persistence success or change its result after the write: an R25 write failure emits a separate bounded critical persistence diagnostic after that failure is known, using the established canonical request ID when available, without raw database content. This does not duplicate the primary outcome or create an alternate client result.

Logging failure must not change a determined response, repeat upstream execution, send a second response, change circuit classification, alter rate accounting, prevent response cleanup, or block streaming on unbounded work. It cannot trigger an unbounded retry queue, recursive failure-reporting loop, or durable delivery claim. Existing telemetry-failure metrics/hooks retain their authority; R26 introduces no metric name, queue, exporter, or persistence dependency.

## 10. Preserved boundaries and compatibility

R26 governs operational log ownership/redaction only. M7 metric names and M3 cardinality restrictions remain unchanged. Metrics classification/emission questions, if found by a later readiness audit, require their own reconciliation rather than an implicit R26 extension. No metric or tracing architecture is selected here.

R25 PostgreSQL request telemetry, Control Plane `audit_logs` durability/transactions, independent security-audit obligations, and M8/M9 browser/session security remain separate. This contract adds no per-request administrative audit obligation, protected logging sink, persistent log-event table, public operation/error, database field/migration, tenant/Service/Route option, environment setting, or M3-R3A consumer. Later implementation must configure the existing server/logger mechanisms and prove the profile; this document does not claim that the current runtime already enforces it.

## 11. Required later acceptance evidence

Later implementation and deployment acceptance must prove:

1. Authentication rejection attempts one safe primary outcome event.
2. Tenant-binding rejection retains established correlation without invented tenant/Route identity.
3. R19 404/405 emits bounded outcomes without Route identity.
4. Route-attributed success correlates with R25 where persistence succeeds.
5. Rate/concurrency rejection produces safe bounded outcomes.
6. Timeout/transport failure retains normalized classification.
7. Downstream disconnect is not successful delivery.
8. Lease loss preserves the winning terminal result.
9. Post-commit stream failure is not completed delivery.
10. Valid zero-byte/body-forbidden responses follow R16/R17.
11. Raw query content never appears in application logs.
12. Raw-path evidence and unsafe decoded targets never appear in logs.
13. Gateway credentials never appear in logs.
14. Managed upstream secrets never appear in logs.
15. Request/response bodies never leak.
16. Internal upstream URLs never leak.
17. Raw dependency exceptions do not leak, including Uvicorn's ASGI error path.
18. General logs contain no prohibited tenant/Service/Route or other resource identities.
19. Stock Uvicorn access logging is disabled under the supported profile.
20. HTTPX/httpcore URL/transport logs and trace callbacks cannot bypass minimization when logger levels change.
21. SQLAlchemy/PostgreSQL/Redis sensitive debug/exception output is disabled or safely sanitized.
22. External ingress compliance is documented and verified at deployment acceptance.
23. Established canonical request-ID correlation is preserved; missing IDs are not retrospectively invented.
24. Primary outcome and persisted R25 record agree on the observed lifecycle where persistence succeeds.
25. R25 write failure emits a separate bounded critical diagnostic without changing the client outcome.
26. Controlled competing terminal signals cannot produce duplicate/conflicting primary outcomes.
27. Logging failure cannot change cleanup, response, circuit results, rate accounting, or execution order, or cause unbounded work.
28. No unapproved OpenAPI, schema, migration, protected-sink, metric, or M3-R3A drift occurs.

No tests are implemented or claimed as passed by R26. Metrics semantics, broader acceptance-test architecture, protected identifier-bearing logging, vendor integration, and crash-durable log delivery remain outside this contract.
