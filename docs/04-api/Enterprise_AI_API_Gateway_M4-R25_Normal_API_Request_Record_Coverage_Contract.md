# M4-R25 — Normal API Request Record Coverage Contract

Status: **FROZEN M4 V1 CONTRACT — DOCUMENTATION ONLY.** This contract defines eligibility and a bounded terminal persistence lifecycle for Normal API `requests` telemetry. It changes no runtime code, tests, OpenAPI, physical schema, migration, or existing contract file.

## 1. Authority and narrow reconciliation

The [M4 execution pack](../05-implementation/Enterprise_AI_API_Gateway_M4_Normal_API_Engine_Execution_Pack_V1.1.docx), section 16, requires one `requests` row for every accepted/authenticated attempt according to the existing telemetry contract, including failures before upstream invocation where appropriate. It also requires tenant, NORMAL workload, API-key/Route attribution, start/end, status, latency, and a normalized error category. That wording does not define when authentication becomes an accepted execution attempt.

The [PRD V1.4](../01-product/Enterprise_AI_API_Gateway_PRD_V1.4.docx), sections 19–20, separates request logging from administrative audit; its request-ID requirement covers all requests but does not require a PostgreSQL NORMAL row for every authenticated rejection. The [M7 execution pack](../05-implementation/Enterprise_AI_API_Gateway_M7_Observability_Retention_Operations_Execution_Pack_V1.2.docx), section 8, preserves parent request telemetry for accepted/authenticated Data Plane requests according to the frozen contract. Neither source explicitly requires durable NORMAL records for authenticated pre-Route failures. The [HLD](../02-architecture/Enterprise_AI_API_Gateway_HLD_V1.1_Corrected.docx) makes PostgreSQL authoritative for persisted request metadata and requires an explicit, observable logging-loss policy; it does not require a request-start insert.

The [physical database specification](../03-data-model/Enterprise_AI_API_Gateway_Physical_DB_Alembic_Spec_V1.2_Final.docx), current `Request` model, and M1 migration `c7d8e9f0a1b2` require a non-null Route reference for NORMAL workload, tenant-aware Route attribution, and unique canonical `request_id`. These constraints are compatible with the policy below; they are not by themselves its product authority. R25 explicitly resolves the ambiguous M4/M7 coverage phrase using those documents together. It does not weaken an explicit all-authenticated-pre-Route persistence obligation; none was found.

[M4-R7A](Enterprise_AI_API_Gateway_M4-R7_Normal_API_Protection_Admission_Ordering_Contract.md) owns admission order. [M4-R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md) and [M4-R17](Enterprise_AI_API_Gateway_M4-R17_Normal_API_Response_Streaming_Commit_Contract.md) own execution, cancellation, and delivery boundaries. [M4-R18](Enterprise_AI_API_Gateway_M4-R18_Normal_API_Current_State_Consumer_Contract.md) owns coherent execution identity. R25 supplies request-record coverage and persistence rules without redefining those execution outcomes.

## 2. Eligibility: ROUTE_ATTRIBUTED_ATTEMPT

A Normal API request becomes eligible for durable `requests` telemetry only after Gateway authentication and tenant binding succeed, required Service/Route/current-state resolution succeeds, and one executable Route is definitively selected from the request's coherent R18 snapshot. This conceptual boundary is **ROUTE_ATTRIBUTED_ATTEMPT**. It is not a new database enum, field, lifecycle state, public value, or configuration option.

An authenticated request proves a caller identity; it does not prove an executable Route has been selected. A valid key requesting an unknown Service or path has no selected Route. An R19 method-mismatch path-match set may contain several Routes but no executable Route for that method. Neither authentication nor membership in that set establishes ROUTE_ATTRIBUTED_ATTEMPT.

Route attribution occurs within R7A Stage 2. It is independent of later rate admission, concurrency acquisition, body guarding, preparation, circuit admission, and upstream invocation. Do not reorder those stages to simplify recording. The record's Route must be the actual definitive execution selection, never an intermediate candidate.

## 3. Coverage before and after selection

Requests failing before definitive executable Route selection do not require a NORMAL `requests` row under M4 V1. This includes authentication/tenant-binding failure, missing or inaccessible Service, invalid path/canonical-path evidence, R19 404 or 405 without an executable method match, and unusable R18 state preventing reliable selection. No placeholder Route, arbitrary first path match, foreign-tenant Route, Unicode-equivalent substitute, nullable NORMAL Route exception, or fabricated identity is permitted.

Exclusion from this table does not prohibit separately authorized operational/security monitoring. It does not create a new durable security-event table or allow identifying unknown callers by a guessed tenant. Broader durable security-event coverage requires its own contract.

After ROUTE_ATTRIBUTED_ATTEMPT, every attempt has one logical request record obligation even if upstream is never invoked. Coverage includes rate/concurrency rejection, body-limit rejection, disconnect during bounded body reception, Stage-6 preparation/destination/credential failure at its frozen stage, circuit-open rejection, pre-response timeout, upstream transport failure, complete upstream HTTP responses including 401/403 and other 4xx/5xx, successful streaming completion, downstream disconnect, lease ownership loss, post-commit stream failure, and shutdown cancellation after eligibility. Persistence-failure limits in section 6 apply; eligibility is not a promise of storage success during database failure or process loss.

## 4. Terminal insertion and recorded values

No inspected authority specifies Normal API insertion timing. R25 narrowly selects **terminal-only insertion**: maintain bounded request-local attribution/outcome state after eligibility, then make one bounded PostgreSQL insertion attempt after the request's terminal delivery/execution outcome is established and its request-scoped cleanup has completed or made its required cleanup attempts. Do not insert a request-start row, hold a database transaction across upstream/downstream network execution, or make telemetry storage a new admission gate. This rule applies only to M4 NORMAL rows and does not change LLM parent/attempt persistence.

The insert carries final values rather than creating a start row requiring a later completion update. Capture the original request start time; do not substitute the eligibility or database insertion time. Capture terminal time from the existing R9/R17 request lifecycle. `completed_at` marks termination, including an interrupted request; its presence is not evidence of successful response delivery. `latency_ms` represents that request interval and excludes the later telemetry write. Preserve the existing nonnegative-latency and completion-after-start constraints.

Populate only existing schema fields with actual observed values. `status_code` is the actual committed downstream HTTP status or an existing selected Gateway error status; leave it null when cancellation/disconnection provides no real response status rather than inventing one. A previously committed 200 must not imply successful delivery when the stream later aborts: retain that status and the applicable sanitized interruption category in `error_code` where representable. Existing Gateway failures retain their existing error code. Cancellation/interruption categories follow R9/R17; they do not become new public errors or fabricated upstream 5xx outcomes. Do not overwrite an error/status decision that already won its applicable terminal gate.

Persist tenant, application/API-key attribution where available, and actual selected Route from the captured execution context. Do not add Service, upstream-status, credential, generation, trace, upstream-invoked, or arbitrary metadata columns. The pack's optional extra metadata is recorded only where an existing schema can truthfully represent it. A selected Route is not evidence of an upstream invocation.

## 5. Lifecycle-safe finalization

Exactly one request-local finalization obligation owns the record. Normal completion, timeout, disconnect, lease loss, shutdown, and cleanup callbacks share that obligation; they must not independently persist competing final records. The first applicable R9 request terminal decision wins. R17's distinction between upstream completion and downstream delivery remains controlling: a completed upstream response may already have determined its circuit result while downstream delivery subsequently fails. Request telemetry must reflect the actual delivery termination without rewriting that definitive upstream circuit result.

Response headers, first raw bytes, or downstream response-start alone do not finalize request telemetry. Wait for the existing successful delivery terminal boundary or its winning interruption/cancellation. Body-forbidden and valid zero-byte responses retain R16/R17 completion rules. Cancelled or interrupted delivery must not be recorded as successful streaming completion.

Use the existing unique canonical `requests.request_id` as the correlation/deduplication basis; do not introduce another idempotency key. Keep the existing application-generated UUID v7 row primary-key requirement. Database uniqueness prevents a second row, but does not establish the correct winner: the request-local outcome must be frozen before insertion. A duplicate request-ID conflict must not overwrite an existing final row, change its attribution, or rerun the business request; signal the anomaly safely. Do not use an upsert that replaces a terminal result with a later callback's result.

There is no hard-crash delivery guarantee: an abrupt process kill may prevent the terminal write entirely, and an interrupted database acknowledgement can leave commit success unknown. This is an explicit limitation of bounded request telemetry, not exactly-once execution or a durable outbox. Never replay the upstream request to recover a missing or uncertain telemetry record.

## 6. Persistence failure and bounded loss

M7 section 18 preserves an already-determined client result on telemetry insert failure, requires a critical metric/log signal, and prohibits an unbounded retry queue. Its separate audit-write-failure rule rolls back Control Plane mutation/config-version changes; that rule does not apply to proxy traffic telemetry.

Terminal-only insertion ensures the client outcome is already determined before this write is attempted. There is no pre-outcome request telemetry insert whose failure could choose a new fail-open/fail-closed admission policy. A failed or unavailable terminal write preserves that result, existing status/envelope, canonical request correlation, rate accounting, and circuit outcome. It cannot create another upstream attempt, second downstream response, circuit completion, or delayed/repeated concurrency release.

Make one bounded write attempt for the frozen request record; do not add a retry queue, background worker, shutdown drain backlog, or persistence-driven execution replay. Database unavailability, write failure, or shutdown interrupting that bounded attempt may leave the record absent; record a critical bounded persistence-failure diagnostic through existing observability hooks, with canonical request ID where available and no raw database exception. Do not claim guaranteed diagnostics or durable recording after abrupt process termination. Required protection cleanup precedes persistence, so storage failure cannot retain a lease or prevent request-scoped response/renewal cleanup. Cancellation handling must preserve the single finalization obligation without an unbounded cancellation shield or wait.

These are traffic-telemetry loss semantics. They neither relax Admin/security audit obligations nor change their transactional failure behavior.

## 7. R18 identity, privacy, and storage compatibility

Use the tenant and definitive Route identity captured from one coherent R18 generation. Do not re-resolve after an Admin update, silently switch generations, select an NFC-equivalent different Route, or consult mutable configuration to improve persistence success. An invalidated tenant certificate prevents subsequent captures under R18; it does not change the identity of an already captured in-flight request. No generation column or new M3-R3A consumer is introduced.

Persist no plaintext Gateway key, decrypted upstream credential, credential ciphertext, request/response body, cookie, raw authorization/header value, raw query, raw-path evidence, internal upstream URL, or unbounded exception text. R14/R21 transport evidence does not authorize storage. Use existing authorized columns and M3-R13 data-minimization rules; request/resource identifiers are not metric labels.

`requests` remains traffic telemetry; `audit_logs` remains administrative/security audit under its own authority. R25 requires no Admin-style audit event for every proxy request. Existing retention, access controls, tenant-aware foreign keys, NORMAL workload CHECK, request-ID uniqueness, and runtime/retention-role privilege boundaries remain unchanged. No table, column, nullable-Route exception, migration, public operation/error/field, tenant setting, OpenAPI change, or M3-R3A consumer is added.

## 8. Required later acceptance evidence

Later implementation must prove:

1. Unauthenticated requests create no NORMAL row.
2. Authenticated unknown-Service requests create no NORMAL row.
3. R19 404 creates no fabricated Route attribution.
4. R19 405 does not select an arbitrary path-match Route for recording.
5. Definitive executable Route selection establishes eligibility before protection admission.
6. Rate rejection is recorded against that Route.
7. Concurrency rejection is recorded.
8. Body-limit rejection and body-reception disconnect are recorded.
9. Stage-6 preparation, destination, and credential failures are recorded without an upstream attempt.
10. Circuit-open rejection is recorded.
11. Complete upstream 2xx is recorded accurately.
12. Upstream 401/403 is recorded without changing passthrough semantics.
13. Upstream 5xx is recorded accurately.
14. Transport failure is recorded.
15. Timeouts retain their frozen public and circuit outcomes.
16. Successful streaming and valid zero-content completion are recorded at the actual terminal boundary.
17. Downstream disconnect/post-commit interruption is distinguished from successful delivery.
18. Lease loss retains its actual terminal outcome and cleanup.
19. Shutdown cancellation uses the one finalization obligation and a bounded persistence attempt.
20. One canonical request ID cannot produce duplicate rows; duplicate-ID handling cannot overwrite the final result.
21. Controlled terminal-signal races preserve the winning request result and R17's separate definitive upstream circuit result.
22. Persistence failure cannot cause another upstream attempt.
23. Persistence failure cannot cause a second downstream response; database failure/unknown acknowledgement is safely signalled without unbounded retries.
24. Secrets, bodies, raw paths/queries, and uncontrolled exceptions do not leak.
25. Persisted tenant/Route match the immutable R18 execution identity during concurrent configuration changes.
26. R7A rate/concurrency/circuit order and R8/R9 cleanup remain unchanged; no database transaction spans network execution.
27. No migration, schema, OpenAPI, or M3-R3A drift occurs.

Use controlled synchronization for terminal races. This documentation change supplies no runtime test evidence and implements none of these tests. Durable security coverage for excluded requests, stronger crash-durable telemetry, queues/outboxes, or broader observability architecture remain separate future contract work; none is introduced here.
