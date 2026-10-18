# M4-R27 — Normal API Metrics Semantics Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract defines the Normal API contribution to the existing metric catalog. It implements no instrumentation, tests, exporter, endpoint, configuration, schema, or migration.

## 1. Authority and scope

The [M7 execution pack](../05-implementation/Enterprise_AI_API_Gateway_M7_Observability_Retention_Operations_Execution_Pack_V1.2.docx), sections 6–7, lists the metric names below and restricts label cardinality. It does not define their Normal API counting populations, exact emission points, or complete label schemas. [M3-R13](Enterprise_AI_API_Gateway_M3-R13_Observability_Security_Boundary_Contract.md) defines existing protection/dependency event semantics and backend-neutral hooks without selecting a metrics backend. R27 reconciles the missing Normal API meanings; it does not rename metrics, replace M3 events, or reinterpret other workloads.

[M4-R2](Enterprise_AI_API_Gateway_M4-R2_Normal_API_Circuit_Outcome_Classification_Contract.md) owns circuit classification. [R7A](Enterprise_AI_API_Gateway_M4-R7_Normal_API_Protection_Admission_Ordering_Contract.md) owns admission; [R8](Enterprise_AI_API_Gateway_M4-R8_Normal_API_Concurrency_Lease_Renewal_Contract.md)/[R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md) own lease/cancellation lifetime; [R10](Enterprise_AI_API_Gateway_M4-R10_Normal_API_Upstream_Timeout_Policy_Contract.md) owns timeout clocks. [R16](Enterprise_AI_API_Gateway_M4-R16_Normal_API_Response_Forwarding_Contract.md)/[R17](Enterprise_AI_API_Gateway_M4-R17_Normal_API_Response_Streaming_Commit_Contract.md) distinguish upstream completion, downstream commitment, and delivery termination. [R18](Enterprise_AI_API_Gateway_M4-R18_Normal_API_Current_State_Consumer_Contract.md) owns immutable configuration identity; [R19](Enterprise_AI_API_Gateway_M4-R19_Normal_API_Public_Endpoint_Method_and_Routing_Contract.md) owns path/method rejection. [R25](Enterprise_AI_API_Gateway_M4-R25_Normal_API_Request_Record_Coverage_Contract.md) and [R26](Enterprise_AI_API_Gateway_M4-R26_Normal_API_Operational_Logging_and_Redaction_Boundary_Contract.md) remain independent persistence and operational logging contracts.

No stronger inspected authority gives a conflicting Normal API definition for these catalog entries. R27 narrows only their previously unspecified Normal API population/timing. Global catalog entries continue to aggregate contributions from relevant workloads; Admin, LLM, health, and worker boundaries are not redefined by this document.

## 2. Catalog and emission ownership

| Existing name | Semantic instrument and owner for M4 |
| --- | --- |
| `requests_total` | Counter; application request-lifecycle owner counts each Normal API request observed at application entry once, including pre-Route rejection. |
| `request_duration_seconds` | Nonnegative duration observations in seconds; that owner records the observed request interval once at terminal delivery/cancellation. |
| `errors_total` | Counter; that owner records at most one Normal API terminal request error as defined in section 4. |
| `normal_proxy_requests_total` | Counter; that owner records one request crossing R25 Route-attributed eligibility, including later local rejection. |
| `upstream_duration_seconds` | Nonnegative duration observations in seconds; request-scoped upstream executor records one actually dispatched connector attempt interval. |
| `upstream_errors_total` | Counter; that executor records at most one upstream error under section 6. |
| `rate_limit_reject_total` | Counter; one observation owner/adaptor records each authoritative logical M3 rate rejection once. |
| `concurrency_reject_total` | Counter; one observation owner/adaptor records each authoritative logical M3 concurrency rejection once. |
| `circuit_state` | Bounded observations of actual M3 circuit states; the existing circuit instrumentation owns these observations. |
| `circuit_transition_total` | Counter; existing circuit instrumentation records actual applied state transitions once. |

Durations are observations, not labels or last-value substitutes for the distribution. R27 chooses no histogram buckets, summary implementation, numeric encoding for circuit states, exporter, or storage backend; those are future adapter concerns. M3's existing hook observations remain authoritative for protection state. A facade, primitive, application wrapper, and exporter must not each count the same logical rejection or transition as separate events. Existing per-scope diagnostic observations are not additional request rejections.

No existing complete M4 outcome/status label schema was found. The supported baseline adds **no new labels** to these names. Outcome classification below determines whether an existing aggregate counter increments, not a new label dimension. Existing bounded M3 dimensions retain their prior meaning; R27 does not remove them or invent replacements. Exporting an additional Normal API label schema requires explicit reconciliation of its finite vocabulary and meanings.

## 3. Normal Proxy eligibility versus emission

`normal_proxy_requests_total` counts requests crossing R25's **ROUTE_ATTRIBUTED_ATTEMPT** boundary: successful Gateway authentication and tenant binding, usable coherent R18 state, and definitive executable Route selection during R7A Stage 2. Mere authentication or membership in R19's path-match set is insufficient.

Do not count authentication/tenant-binding failure, invalid ingress path, unknown/inaccessible Service, R19 404, R19 405 without an executable method match, or unusable current state preventing definitive Route selection. Do count each eligible request even if it later ends in rate/concurrency/body rejection, Stage-6 preparation failure, circuit denial, timeout, transport/HTTP outcome, disconnect, lease loss, cancellation, or interrupted streaming.

With the unlabelled baseline, increment once at eligibility. Do not wait for database persistence and do not increment again on upstream start/response, response-start, terminal delivery, cleanup, R25 insertion, or R26 logging. The counter's eventual total is the eligible population, not the number of upstream calls or successful responses.

Eligibility and outcome classification remain distinct. If a future approved representation adds terminal outcome/status labels to this same counter, its one final labelled contribution must instead wait for the winning terminal result; it cannot coexist with an additional admission increment. R27 does not authorize those labels now.

Use canonical request ID and existing request-local lifecycle ownership internally to prevent duplicate emissions. Do not use the ID as a label or create a persistent metrics idempotency key. Abrupt process death or a metrics backend failure can prevent sample delivery; exactly one intended contribution does not imply crash-durable or exactly-once exporter delivery.

## 4. Shared Gateway metrics: Normal API contribution only

For Normal API application-observed requests, `requests_total` increments once at the authoritative Gateway request-start boundary, independently of later authentication or Route selection. Parser rejection before application invocation contributes nothing through this application owner. R19 404/405 and invalid paths that reach this boundary are included without fabricated resource attribution. Other planes' contributions retain their own authority; do not apply R25's Route requirement to Admin/LLM traffic or introduce new routing/exposure rules to identify the population.

`request_duration_seconds` receives one observation when the observed request delivery/execution terminates under R9/R17. Successful response-start is not its endpoint. Authentication rejection, local rejection, interrupted delivery, and observed shutdown cancellation are terminal observations, not exclusions from the duration population. If abrupt termination prevents observation, do not fabricate a duration.

For the Normal API contribution, `errors_total` increments once if the winning terminal request outcome is a Gateway rejection/failure, timeout, cancellation, or interrupted/failed delivery, or a completed downstream HTTP response with status 400–599. Complete 2xx/3xx delivery contributes zero. A committed 200 followed by delivery interruption contributes one. A delivered upstream 401/403 or 501 contributes one even though R2 may classify that upstream response as circuit SUCCESS. Status plus a subsequent interruption still contributes at most one error for the request. No fake HTTP status is required to count an observed cancellation.

R25 write failure, R26 logging failure, and metric export failure are separate diagnostic conditions; they do not turn an otherwise successful request into an additional `errors_total` contribution. These Normal API rules do not define LLM SSE business outcomes, Admin success/error rules, or other workload denominators.

## 5. Actual upstream attempts and duration

An upstream attempt begins with actual dispatch into the request-scoped connector/send operation after circuit admission. Preparing an HTTPX Request object, selecting a Route, receiving a body, validating a destination, or claiming a probe is not dispatch. Capture a monotonic timestamp immediately before dispatch, consistent with R10, but contribute an upstream observation only if connector execution actually began. A pool-acquisition timeout inside that dispatched operation is an attempted connector operation even though it obtained no socket.

`upstream_duration_seconds` ends when the upstream operation reaches clean definitive completion/failure or is terminated under the winning R9/R10 cancellation/deadline. It includes required final framing/body consumption under R16/R17, not merely header arrival. Valid zero-byte/body-forbidden responses use their existing terminal rules. If an incomplete stream is cancelled, its duration ends at the request-scoped upstream operation's termination; no successful HTTP completion is invented.

Exclude rate/concurrency admission, body reception before send, Stage-6 preparation, later R25 insertion, R26 logging, and metric export. Backpressure occurring before upstream completion can extend the actually observed attempt interval because R17 deliberately couples reads and downstream sends. Do not invent a pure network-service time by subtracting guessed stalls. Once upstream completion is established, later downstream-only draining/delivery time is excluded from this upstream interval. No additional TTFB or downstream-duration metric name is introduced.

The request-level interval begins at the authoritative Gateway request start and ends at observed terminal delivery/cancellation, matching R25's request interval. It excludes later storage/log/export work. Duration measurements use monotonic elapsed time; request correlation timestamps retain their existing wall-clock meaning. Metrics do not create new timeout clocks or change R10 budgets.

## 6. Upstream error metric versus circuit classification

R27 gives the previously undefined `upstream_errors_total` an independent aggregate meaning: one error for an actual dispatched attempt that ends in a complete upstream HTTP 500–599 response, or a verified upstream DNS/TCP/connect/write/read/reset/protocol/TLS failure before clean upstream completion. Do not count merely from an error status header if the body is still incomplete; resolve the attempt's actual terminal outcome once. A 500 response that truncates contributes one transport error, not a status error plus a second error.

Complete upstream 4xx, including 401/403/408/429, contributes zero to this upstream-error counter; it can still contribute one request-level `errors_total`. Complete 501 and 505–599 contribute one upstream-error count but remain R2 circuit SUCCESS. TLS failure contributes one upstream-error count but remains R2 NO_COMPLETION under R13. These explicit distinctions do not call a different M3 completion method or redefine upstream health classification.

Do not increment `upstream_errors_total` for pre-dispatch authentication/resolution/protection/preparation/credential/destination rejection, OPEN/occupied-probe denial, local pool-acquisition timeout, the separate Gateway pre-response deadline, downstream-only failure, lease loss, shutdown, or other local cancellation. A deadline/cancellation winning before a definitive upstream result must not be retrospectively classified from a later error signal. Local programming/configuration faults remain local. R11 validation-time DNS failure is not an upstream transport attempt; DNS transport failure during an actually dispatched connector operation is.

R2's circuit failure set remains exactly 500/502/503/504 plus its defined transport failures. Its SUCCESS/FAILURE/NO_COMPLETION classifications, stale/unknown handle treatment, HALF_OPEN ownership, and at-most-once applicable completion calls remain unchanged. A later downstream failure cannot rewrite an already definitive upstream error contribution or circuit result.

## 7. Protection observations

`rate_limit_reject_total` increments once for an actual logical M3 rate rejection; `concurrency_reject_total` once for an actual logical concurrency rejection. R7A keeps those stages distinct: rate rejection prevents concurrency acquisition and does not count as a concurrency rejection. A dependency UNKNOWN/error, a local preparation rejection, lease loss, or OPEN circuit denial is not a capacity denial for either counter. Redis failure followed by an authorized local decision does not create two logical request decisions. Do not re-count from an HTTP 429 alone or from cleanup/logging callbacks.

`circuit_state` observes actual known M3 state; do not invent a state from UNKNOWN_HANDLE, absent result state, local failure, HTTP status, or global traffic mode. `circuit_transition_total` counts actual applied changes between states, not attempts, repeated observations of the same state, stale/unknown completions, or duplicate callbacks. Successful HALF_OPEN claims and their expiry/completion retain M3's exact state/ownership semantics. Circuit events may precede final downstream delivery; the request owner must not fabricate another transition for its own terminal metric. Backend provenance remains relevant internally but handles/keys/generation identities are not labels.

## 8. Cardinality, R18, isolation, and delivery limits

Preserve M3-R13/M7 restrictions: no labels carrying request ID, tenant/Service/Route UUID, API-key or credential identity, raw client IP or HMAC identity, URL, raw path/query, Redis key, arbitrary user identity/model string, unbounded error category, or exception text. Numeric durations are observations. Configuration-provided Route templates or Service names are not automatically bounded catalogs. A separately authorized finite classification must come from the captured immutable R18 snapshot, never a later mutable lookup or an inferred identity for pre-Route failures. R27 adds no such catalog or label.

Use existing backend-neutral hooks and a single contribution owner per logical observation. Do not add `/metrics`, a Prometheus exporter, tenant-visible metrics API, public diagnostic operation, or new telemetry dependency. Exporter mapping and deployment access/isolation require separate approval. A backend adapter must not change the frozen population or double-count an operation merely because multiple hooks describe it.

Instrumentation failure cannot modify a public response, send another response, repeat an upstream call, change circuit completion, consume protection admission, retain a lease, prevent renewal cleanup, fail R25 persistence, or block streaming/cleanup on unbounded work. No unbounded queues or dynamic label sets are permitted. Separate safe diagnostics follow R26 without recursive export loops. A failed emission may lose a sample; do not retry business execution or claim guaranteed exporter delivery.

R25 database records, R26 primary operational events, and R27 metrics have independent populations, timing, and delivery guarantees. Never derive request counts from inserted rows or assume logging success proves metric delivery. R25 write failure retains its existing bounded critical signal and does not add a request error, replay an eligibility increment, or change an upstream/protection counter. R27 introduces no metric name for that diagnostic.

## 9. Required later acceptance matrix

In the table, R/N/E are increments of `requests_total` / `normal_proxy_requests_total` / `errors_total`; U is the number of `upstream_duration_seconds` observations; X is the increment of `upstream_errors_total`. These are table abbreviations, not new instruments. Each observed terminal request also contributes one `request_duration_seconds` observation. Unless stated otherwise, rows assume valid Route attribution, one attempt, normal delivery where possible, and working instrumentation. No actual backend delivery is guaranteed during failure.

| Scenario | R/N/E | U/X | Additional required observation |
| --- | --- | --- | --- |
| 1. Eligible request, complete delivered 200 | 1/1/0 | 1/0 | N increments once at eligibility. |
| 2. Authentication failure | 1/0/1 | 0/0 | No fabricated tenant/Route. |
| 3. Tenant-binding failure | 1/0/1 | 0/0 | No eligibility contribution. |
| 4. Unknown/inaccessible Service | 1/0/1 | 0/0 | No guessed attribution. |
| 5. R19 404 | 1/0/1 | 0/0 | No upstream operation. |
| 6. R19 405 | 1/0/1 | 0/0 | Path-match set is not Route selection. |
| 7. Rate rejection | 1/1/1 | 0/0 | Rate rejection +1; concurrency rejection +0. |
| 8. Concurrency rejection | 1/1/1 | 0/0 | Rate rejection +0; concurrency rejection +1. |
| 9. Body-limit rejection | 1/1/1 | 0/0 | No circuit admission/completion. |
| 10. Stage-6 failure | 1/1/1 | 0/0 | Includes local credential/destination failure. |
| 11. OPEN/occupied-probe rejection | 1/1/1 | 0/0 | No invented circuit completion or transition. |
| 12. Complete delivered upstream 2xx | 1/1/0 | 1/0 | R2 classifies only at its definitive boundary. |
| 13. Complete delivered upstream 4xx | 1/1/1 | 1/0 | Include 401/403/408/429; circuit SUCCESS. |
| 14. Complete delivered upstream 5xx | 1/1/1 | 1/1 | 501/505–599 remain circuit SUCCESS; selected R2 set is FAILURE. |
| 15. Connect timeout after dispatch | 1/1/1 | 1/1 | R2 FAILURE. |
| 16. Pool timeout after dispatch | 1/1/1 | 1/0 | Local error; R2 NO_COMPLETION. |
| 17. Upstream read-idle timeout | 1/1/1 | 1/1 | No second downstream envelope after commit. |
| 18. Pre-response deadline after dispatch | 1/1/1 | 1/0 | R2 NO_COMPLETION; no retrospective failure. |
| 19. Downstream disconnect before upstream completes | 1/1/1 | 1/0 | During pre-send body receipt instead: U/X=0/0. |
| 20. Lease loss after eligibility before dispatch | 1/1/1 | 0/0 | If dispatched: U=1; cancellation alone still X=0. |
| 21. Shutdown after eligibility before dispatch | 1/1/1 | 0/0 | If an in-flight operation is observed terminating: U=1; no fabricated success. |
| 22. Successfully delivered streamed 200 | 1/1/0 | 1/0 | No terminal observation at response-start. |
| 23. Interrupted committed 200 stream | 1/1/1 | 1/1 for upstream fault; 1/0 for downstream-only interruption | Preserve the actual winning R9/R17 outcome. |
| 24. Valid empty/body-forbidden success | 1/1/0 | 1/0 | Apply R16/R17 framing/completion evidence. |
| 25. Upstream 200 completes, then downstream fails | 1/1/1 | 1/0 | Circuit SUCCESS is not rewritten; repeat with complete 500 and X=1/circuit FAILURE. |
| 26. Request-duration boundaries | As baseline 1 | As baseline 1 | Assert original start to delivery/cancellation; exclude storage/log/export. |
| 27. Upstream-duration boundaries | As baseline 1 | As baseline 1 | Exclude pre-send stages and post-upstream delivery tail; test bounded backpressure. |
| 28. Label restrictions | As baseline 1 | As baseline 1 | No prohibited labels, arbitrary templates, or dynamic resource classes. |
| 29. R25 insertion fails after delivered 200 | 1/1/0 | 1/0 | Separate critical diagnostic; no changed counters or execution. |
| 30. Metrics backend fails | Intended baseline 1 contributions | Intended baseline 1 observations | Actual delivery may be absent; no second business call, response, altered circuit result, or cleanup leak. |

Also test TLS failure (R/N/E=1/1/1, U/X=1/1, circuit NO_COMPLETION), duplicate terminal callbacks, actual applied versus stale/unknown circuit outcomes, unknown completion state, and protection events across issuing-backend provenance. For global entry counts when a crash prevents terminal observation, do not assert that completed-duration/error totals must equal entry totals. Verify no OpenAPI/schema/migration/M3-R3A drift. Use controlled clocks and synchronization for timing/races; implement no tests in this reconciliation.

## 10. Compatibility and deferred scope

There are no new metric names, public errors/operations, persisted fields, migrations, tenant/Service/Route settings, M3-R3A consumers, exporters, or endpoints. Existing M3 state/lease ownership, R2 circuit classifications, R17 streaming, R25 persistence, and R26 logging retain their meanings. This document defines only M4 Normal API metric contributions; unrelated workload definitions remain unchanged. External instrument mapping, buckets, deployment scrape isolation, and any future labels remain separately approved adapter/deployment work, not implicit additions to R27.
