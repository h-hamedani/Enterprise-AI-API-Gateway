# M4-R2 — Normal API Circuit Outcome Classification Contract

Status: frozen M4 V1 contract reconciliation. This document classifies Normal API upstream outcomes only. It adds no runtime implementation, retry policy, public API operation, or circuit state-machine change.

## 1. Authority and narrow precedence

The [M4 Execution Pack V1.1](../05-implementation/Enterprise_AI_API_Gateway_M4_Normal_API_Engine_Execution_Pack_V1.1.docx) requires connect timeout, connection failure, read timeout, and “selected upstream 5xx” to be circuit-eligible; it excludes client cancellation and ordinary upstream 4xx. The [PRD V1.4](../01-product/Enterprise_AI_API_Gateway_PRD_V1.4.docx), [HLD V1.1 Corrected](../02-architecture/Enterprise_AI_API_Gateway_HLD_V1.1_Corrected.docx), [API Specification V1.2.2 Final](Enterprise_AI_API_Gateway_API_Specification_V1.2.2_Final.docx), [OpenAPI 3.1 V1.2 Final](Enterprise_AI_API_Gateway_OpenAPI_3.1_V1.2_Final.yaml), and [Implementation Plan V1.1](../05-implementation/Enterprise_AI_API_Gateway_Implementation_Plan_V1.1.docx) establish the Normal API/circuit boundary but do not enumerate the selected statuses. The [M3-R4 circuit contract](Enterprise_AI_API_Gateway_M3-R4_Distributed_Circuit_State_Contract_Reconciliation.md) explicitly delegates raw-outcome classification to invocation integrations. Its [M3-R4A settings contract](Enterprise_AI_API_Gateway_M3-R4A_Circuit_Configuration_Source_Contract_Reconciliation.md), [M3-R4B token contract](Enterprise_AI_API_Gateway_M3-R4B_Circuit_Generation_Eligibility_Token_Contract_Reconciliation.md), [M3-R7 dependency boundary](Enterprise_AI_API_Gateway_M3-R7_Redis_Dependency_Error_Boundary_Contract.md), and [M3-R13 telemetry boundary](Enterprise_AI_API_Gateway_M3-R13_Observability_Security_Boundary_Contract.md) remain authoritative. [M4-R1](Enterprise_AI_API_Gateway_M4-R1_Normal_API_Routing_Path_Resolution_Contract.md) determines path construction. The [M6 pack](../05-implementation/Enterprise_AI_API_Gateway_M6_Streaming_Retry_Fallback_Execution_Pack_V1.1.docx) owns LLM retry/fallback and does not define a Normal API retry loop.

This addendum supersedes only the ambiguous M4 phrase “selected upstream 5xx according to configured/default classifier” for **Normal API V1** and supplies the raw-outcome-to-M3 mapping left open by M3-R4. It does not supersede M3 eligibility, generation/incarnation, failure-window, probe, recovery, or provenance semantics; M4-R1 routing; M6 retry/fallback; or HTTP error/response mapping.

## 2. Classifier configuration and exact default

**DEFAULT-ONLY IN M4 V1.** The current service, route, rate-policy, circuit settings, and deployment settings models contain no Normal API outcome-classifier field or endpoint. M3-R4A's deployment settings configure the circuit threshold/window/open/probe parameters, **not** which HTTP statuses count as failures. M4 V1 therefore has one fixed classifier with no per-tenant, service, route, or global override, no inheritance, no empty-list semantics, and no precedence rules. The M4 pack's word “configured” is not an authorization to invent hidden JSON, environment, or database configuration. Any future configurable classifier requires an explicit contract and appropriate schema/config/API/reconciliation work.

The exact selected upstream HTTP failure set is **`{500, 502, 503, 504}`**. A completed final upstream HTTP response with one of those statuses is `FAILURE`; every other valid final HTTP status in `200..599` is `SUCCESS` for **circuit health only**. In particular, `408` and `429` are `SUCCESS` for circuit accounting: each proves that the upstream produced a complete HTTP response, and neither is confused with a gateway-side timeout or automatically trips the circuit. `501` and `505..599` are excluded from the default failure set. This is not a claim that any such response is a successful business response: M4 continues to pass the upstream status/body through under the separate public response contract.

## 3. Completion vocabulary and timing

M4 uses three *orchestration classifications*, not new M3 states:

- `SUCCESS` calls existing M3 `record_success` exactly once for the issued, provenance-aware completion handle. In CLOSED it clears applicable failure history; for the current HALF_OPEN probe it closes the circuit if the M3 primitive applies it.
- `FAILURE` calls existing M3 `record_failure` exactly once. In CLOSED it counts toward the existing sliding-window threshold; for the current HALF_OPEN probe it reopens the circuit if M3 applies it.
- `NO_COMPLETION` makes **neither** call. M3 has no neutral completion operation. In CLOSED it neither adds nor clears failure history. A HALF_OPEN probe retains its existing ownership until its M3 probe lease expires or another frozen M3 transition supersedes it; M4 does not synthesize success/failure or release a probe through an invented API. This is not an M3 `STALE` or `UNKNOWN_HANDLE` result.

Classify one terminal outcome for each admitted real upstream invocation. Informational `100..199` responses, if surfaced by HTTPX, are not terminal and do not complete the circuit; wait for the final response. A `101 Switching Protocols`/upgrade is unsupported by the V1 Normal proxy and is `NO_COMPLETION`, not evidence of backend health. HTTP status codes outside the valid HTTP range are malformed protocol, not ordinary HTTP responses. A complete final upstream response remains the circuit outcome even if a later downstream delivery error occurs. If the downstream client cancels before a complete upstream outcome is known, use `NO_COMPLETION`.

No response body, content type, `Retry-After`, `Connection`, or provider-specific header participates in classification. There is no response-body semantic classifier. Circuit classification is independent of public HTTP status/error mapping and of retry eligibility: `FAILURE` does not imply a retry, and `SUCCESS` does not forbid any separately frozen retry policy. M4-R2 introduces **no** retry or fallback attempt.

## 4. Normative outcome matrix

“Success call” and “failure call” below mean the existing M3 methods with the same issued handle; M3 can still reject a stale/unusable handle without mutation. “Closes probe” means only if M3 applies success for the current, unexpired HALF_OPEN owner.

| Raw outcome | Class | M3 completion action | Counts toward CLOSED failure threshold? | Closes HALF_OPEN probe? | Boundary note |
| --- | --- | --- | --- | --- | --- |
| DNS resolution failure for the configured upstream | FAILURE | Failure call | Yes | No; current probe reopens | Upstream cannot be reached. |
| TCP connection failure/refused | FAILURE | Failure call | Yes | No; current probe reopens | Includes connection refusal. |
| Connect timeout | FAILURE | Failure call | Yes | No; current probe reopens | Gateway-side connect timeout, not HTTP 408. |
| TLS handshake or certificate-validation failure | NO_COMPLETION | No call | No | No | Potential trust/configuration fault; never weaken TLS verification. |
| Write timeout while sending to upstream | FAILURE | Failure call | Yes | No; current probe reopens | A classified upstream socket-write stall; not a downstream-client stall. |
| Read/response-idle timeout | FAILURE | Failure call | Yes | No; current probe reopens | Includes after partial upstream response if the upstream stalls. |
| Local HTTP connection-pool acquisition timeout | NO_COMPLETION | No call | No | No | No upstream connection was obtained. |
| Upstream protocol error/malformed HTTP response | FAILURE | Failure call | Yes | No; current probe reopens | Only a verified upstream protocol failure; not local parsing/programming defects. |
| Upstream connection reset/disconnect before complete response | FAILURE | Failure call | Yes | No; current probe reopens | Includes mid-response upstream transport termination. |
| `100..199` informational, except `101` | Pending | No call yet | No | No | Continue to a final response. |
| `101` upgrade | NO_COMPLETION | No call | No | No | Upgrade protocol is outside M4 V1. |
| `200..299` | SUCCESS | Success call | No; clears CLOSED history | Yes | HTTP success response. |
| `300..399` | SUCCESS | Success call | No; clears CLOSED history | Yes | Redirect is passed through, never auto-followed. |
| `400..407` | SUCCESS | Success call | No; clears CLOSED history | Yes | Application/client HTTP response. |
| `408` | SUCCESS | Success call | No; clears CLOSED history | Yes | Upstream-generated HTTP response, not transport timeout. |
| `409..428` | SUCCESS | Success call | No; clears CLOSED history | Yes | Application/client HTTP response. |
| `429` | SUCCESS | Success call | No; clears CLOSED history | Yes | Upstream rate response; no default circuit failure. |
| `430..499` | SUCCESS | Success call | No; clears CLOSED history | Yes | Application/client HTTP response. |
| `500` | FAILURE | Failure call | Yes | No; current probe reopens | Selected default 5xx. |
| `501` | SUCCESS | Success call | No; clears CLOSED history | Yes | Excluded from default failure set. |
| `502`, `503`, `504` (each) | FAILURE | Failure call | Yes | No; current probe reopens | Selected default 5xx. |
| `505..599` | SUCCESS | Success call | No; clears CLOSED history | Yes | Excluded from default failure set. |
| Invalid/non-HTTP status | FAILURE | Failure call | Yes | No; current probe reopens | Verified malformed upstream protocol. |
| Downstream client disconnect/cancellation before upstream terminal outcome | NO_COMPLETION | No call | No | No | Cancel upstream promptly; release concurrency separately. |
| Application/server cancellation before upstream terminal outcome | NO_COMPLETION | No call | No | No | Do not turn task cancellation into upstream failure. |
| Malformed route, URL, or credential configuration before upstream execution | NO_COMPLETION | No call | No | No | Internal/configuration fault. |
| Local coding/serialization exception before upstream outcome | NO_COMPLETION | No call | No | No | Never poison upstream health with gateway defects. |
| Redis/PostgreSQL dependency failure | NO_COMPLETION | No upstream call from classifier | No | No | M3 dependency/recovery and DB error paths are separate. |
| Gateway error after upstream execution but before a definitive outcome | NO_COMPLETION | No call | No | No | Do not fabricate an upstream result. |
| Gateway/downstream error after a definitive upstream outcome | Preserve prior class | Complete once from prior upstream outcome | Per prior outcome | Per prior outcome | Delivery failure does not rewrite observed upstream health. |
| M3 OPEN or occupied HALF_OPEN denies admission | Not classified | No call | No | No | Zero upstream I/O; no synthetic failure. |

An HTTPS trust failure may make the request fail publicly, but its circuit classification remains neutral. Internal M3 circuit dependency failures must follow the typed M3-R7/M3.9 degraded/recovery boundary; they are not HTTPX upstream failures and must never be passed to this classifier.

## 5. Acceptance examples and compatibility

For an eligible CLOSED request, `200`, `404`, `408`, and `429` each call `record_success`; `500`, `502`, `503`, and `504` each call `record_failure`; connect refused, connect timeout, and read timeout call `record_failure`. A TLS certificate failure and downstream cancellation before a terminal upstream outcome make no circuit completion call. For the corresponding current HALF_OPEN probe, the success examples close it, failure examples reopen it, and neutral examples leave ownership to M3's existing probe expiry. OPEN denial performs no upstream I/O and no classification. A normal CLOSED result arriving after its generation/incarnation is stale remains subject to M3's existing stale-result protection; M4 does not override it.

This mapping changes no M3 primitive method/result type, `CircuitConfig`, identity (`tenant + route + service backend`), Redis key, local-generation provenance, M3-R7 dependency classification, M3-R13 dimension bounds, M4-R1 route resolution, or M6 retry/fallback ownership. Current circuit tests exercise primitive state transitions rather than this not-yet-implemented invocation mapping; M4 must add classifier and integration tests without rewriting those tests. Current persistence has no outcome-classifier field, so **no migration** is required for this default-only rule. No M3-R3A required current-state consumer is added, because there is no classifier configuration. This internal classification adds no public OpenAPI field or operation and requires **no OpenAPI change**.

Record only bounded outcome/status-class/transport-category/circuit-classification metadata where the existing request telemetry permits it. Never log or metric-label response body, request body, Authorization, Gateway key, upstream credential, raw exception text, tenant/resource UUID, Redis key, or completion handle. Existing request/audit privacy rules and M3-R13 bounded telemetry remain authoritative.
