# M4-P1C-F4B — Integration Acceptance Boundary

Status: **FROZEN — OWNER APPROVED — BOUNDED F4B IMPLEMENTATION AUTHORIZED AFTER THE FREEZE COMMIT**

Reviewed baseline: `feature/m3-runtime`, `bb1c577c581afdf93eb31d5d4058b191f66df680`, plus the accepted uncommitted F4A/F6 source. The commit alone does not contain that accepted working-tree implementation. No existing tracked or untracked work is disposable.

## A. Authority and scope

This owner-approved boundary defines only integration of accepted guarded renewal into the internal M4 RequestLifecycle. Its normative requirements are frozen. Bounded F4B implementation may begin only after this document and approved R2 are committed together and the existing F4A source-acceptance prerequisite is verified, as recorded in R2 section 12. No broader implementation authority is granted.

Authorities retain their responsibilities:

- [R2](../04-api/Enterprise_AI_API_Gateway_M4-P1C-R2_Guarded_Renewal_Dispatch_Boundary_Contract.md), sections 2–6: guarded dispatch, outcomes, evidence and timestamps; sections 9–12: cleanup, acceptance and approval.
- [R1](../04-api/Enterprise_AI_API_Gateway_M4-P1C-R1_Server_Shutdown_Request_Cancellation_Deadline_Contract.md), sections 3–8: deadline ownership, irreversible cutoff, registration and residual accounting. Its deployment-approval history is not changed here.
- [R8](../04-api/Enterprise_AI_API_Gateway_M4-R8_Normal_API_Concurrency_Lease_Renewal_Contract.md), sections 2–5: worker lifetime, cadence, horizon, UNKNOWN and release.
- [R9](../04-api/Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md), sections 2–5: request-scoped cancellation, first-winner outcomes and cleanup.
- [M3-R3](../04-api/Enterprise_AI_API_Gateway_M3-R3_Leased_Concurrency_Semaphore_Contract_Reconciliation.md): atomic lease authority, expiry and legacy result semantics.
- [M3-R7](../04-api/Enterprise_AI_API_Gateway_M3-R7_Redis_Dependency_Error_Boundary_Contract.md): exact dependency classification and propagated cancellation.
- [M3-R10](../04-api/Enterprise_AI_API_Gateway_M3-R10_Protection_Handle_Provenance_Contract.md), sections 4–7: issuing backend/generation, retention and restart rules.

A conflict with a frozen invariant MUST stop implementation and be reported; it MUST NOT be resolved by inference or by weakening this boundary.

## B. F4A dependency baseline

F4A/F6 source acceptance is an input, not deployment acceptance. The [accepted implementation notes](Enterprise_AI_API_Gateway_M4_P1C_F4A_Guarded_Renewal_Notes.md) describe the additive guarded facade/backend, immutable outcome snapshots, task-owned operation evidence, dedicated runtime-owned bounded pool, guarded local mutation, managed disposal and writer/protocol-generation confirmation.

F4B MUST consume those accepted interfaces without reopening their design absent a demonstrated defect. redis-py remains pinned to 8.1.0. Existing F4A deterministic and disposable-Redis tests remain regression gates; their passes do not prove lifecycle integration.

## C. Explicit included capabilities

F4B includes the request-owned live dispatch guard, guarded facade adoption, exhaustive outcome interpretation, producing-attempt anchors, bounded uncertainty/evidence ownership, cancellation integration and existing release/cutoff orchestration. It includes deterministic lifecycle tests and disposable-Redis lifecycle acceptance.

## D. Explicit excluded capabilities

No public Normal API route, upstream HTTP executor, request/response streaming implementation, authentication change, Admin input change, OpenAPI change, schema/migration, new policy repository/current-state consumer, new metrics infrastructure or audit action is included. No new retry/fallback, TTL, G, cleanup duration, configuration override, leader, worker scheduler or cross-process handoff is authorized.

Redis lifecycle/disposal redesign, writer-generation ownership changes and shutdown-semantic changes are excluded. A demonstrated integration defect must be reported and minimally scoped before expanding work. Platform shutdown certification and guarded TLS enablement remain separate gates, not waived requirements for their respective release/deployment claims.

## E. RequestLifecycle integration responsibilities

The existing request owner MUST supply synchronous `monotonic()` and `permit(sampled_at)` behavior using its live state and the same clock domain as its anchor and LifecycleScope. The guard MUST be bounded, strictly boolean and free of I/O, await, task creation and lifecycle mutation. Execution eligibility and authoritative terminal observations MUST share the existing synchronous event-loop coordination boundary.

The request MUST start one supervised renewal worker after a real acquire and before the body guard. Denied/null acquisition creates no worker or release obligation. Recording a definitive upstream outcome MUST NOT end renewal while downstream delivery remains active. Terminal execution/cancellation or entry into terminal cleanup disables new renewal. Shutdown initiation alone retains the existing active-request allowance before the process cutoff.

## F. Guarded dispatch and outcome semantics

The lifecycle MUST call the provenance-aware guarded facade, not raw primitives or legacy renewal as a fallback. F4A retains the final writer/under-lock boundary. Preparation is not renewal dispatch evidence.

| Observation | Owner behavior |
| --- | --- |
| BACKEND_RESULT true | Publish a new anchor only under section G |
| BACKEND_RESULT false or NOT_OWNED | Existing R9 definitive-loss path; no backend substitution |
| UNKNOWN | Existing R8 bounded dependency path; preserve attempt certainty independently |
| SUPPRESSED | Re-evaluate live owner state and enforce existing boundaries; no fabricated false, anchor or extra retry |
| Unexplained SUPPRESSED | Existing internal contract-fault path; preserve any prior terminal winner |
| GUARD_ERROR / INTERNAL_ERROR | Existing safe internal-fault path; no fabricated backend loss or dependency classification |
| Propagated cancellation | Preserve cancellation and available operation evidence; never return fabricated normal success/false |

Typed input-validation failures and existing worker-internal exceptions retain their existing semantics. R9 alone governs response/circuit precedence. Local cancellation is not an upstream circuit failure. No second response or circuit completion is introduced.

## G. Timestamp and anchor publication rules

The initial acquire keeps its existing dispatch anchor. Normal renewal is due at anchor + T/3, one UNKNOWN retry at anchor + T/2 and the hard confidence horizon at anchor + 2T/3. Native monotonic precision is required; equality denies. At most one operation is in flight, with bounded NOSCRIPT recovery inside it.

Only authoritative true confirmation observed while eligible and strictly before the previous horizon may publish a new anchor. It MUST use the successful producing attempt's timestamp, including the retry timestamp after NOSCRIPT. Queuing, preparation, the initial NOSCRIPT timestamp and reply arrival are not substitutes. False, suppression, UNKNOWN, faults and late success never extend confidence or revive execution. G is not renewal grace.

## H. Cancellation and evidence ownership

The request MUST retain immutable outcome snapshots and task-owned operation evidence even without a normal return. Cancellation before send, during drain, during response wait and after NOSCRIPT MUST preserve their actual histories and established replies. A later invocation or successful result MUST NOT erase older uncertainty.

Completed history may be compacted into bounded truthful summaries. Unresolved evidence remains strongly owned under R1; runner return, task completion or cancellation alone is not resolution. The request MUST transfer residual tasks and obligations to the supervisor before relinquishing ownership. Late observations may update factual accounting but cannot revive execution, rewrite a terminal result or authorize retries. Backend execution truth and socket-closure truth remain distinct. No unbounded archive of completed operations is introduced.

## I. Release, shutdown and cutoff ordering

All terminal paths share existing cleanup ownership: stop/cancel renewal and owned operations; await actual termination only within the earliest absolute cutoff; when safely stopped and time remains, attempt one release using the original handle. An independently stalled executor finalizer cannot block an otherwise eligible release. Release MUST NOT race unfinished renewal.

No new cleanup operation, release attempt, waiter or later disposal stage may start at or after cutoff. Completed evidence may be inspected synchronously. Pending work and not-attempted/unconfirmed obligations remain retained; no new G, deadline reset, detached retry or pool resurrection is permitted. False/failed release cannot replace the selected response or authorize another release.

F4A/F6 writer/protocol-generation obligations remain independent of current connection references. A writer-less no-op or another generation's confirmation cannot certify closure. Unconfirmed resources remain in runtime/supervisor accounting. Late confirmation cannot convert a terminal failed runtime close into success. Ordinary unmanaged Redis behavior remains unchanged.

## J. Backend provenance requirements

Renewal and release MUST route only by the original process-local handle to its issuing Redis backend or local generation, including a retained retired generation. Current traffic mode is not routing authority. Unknown provenance retains existing zero-backend safe false semantics. No ownership is reconstructed after restart or merged/written back across backends.

## K. Deterministic acceptance matrix

Use controlled clocks, Events and explicit acknowledgements. Arbitrary sleeps cannot prove correctness. Wall-clock bounds serve only as deadlock guards. Teardown MUST unblock and join test-owned work.

| ID | Scenario | Required evidence |
| --- | --- | --- |
| K01 | Real acquire versus denied/null acquire | Worker starts before body only for real handle; no null release |
| K02 | Cadence at minimum/maximum supported TTL | T/3, T/2 and 2T/3 preserved; no rounding grace or overlapping operations |
| K03 | Multiple successful renewals / short request | Producing anchors advance correctly; short request performs no unnecessary renewal and one release |
| K04 | Task/script/pool/auth/health/reconnect delay | Actual integrated final guard denies at/after horizon; zero denied renewal writes; preparation traffic identified separately |
| K05 | Timely NOSCRIPT retry / denied retry | Fresh guard and retry timestamp; exact bounded histories, no fabricated retry |
| K06 | Timely send, late success / exact equality | No anchor advancement or execution revival |
| K07 | False / unknown provenance / missing generation | Existing definitive-loss handling and correct zero-call cases |
| K08 | UNKNOWN then true, repeated UNKNOWN or hanging call | One bounded business retry; existing horizon terminalizes; no provenance substitution |
| K09 | Suppression / unexplained suppression | Existing boundary enforced or internal fault; no false loss, extra retry or anchor |
| K10 | Guard/internal fault, ACL/protocol error | Correct internal versus dependency distinction and immutable evidence |
| K11 | Cancellation before send, after NOSCRIPT, during drain/reply | Propagated cancellation; exact phase/history and retained operation ownership |
| K12 | Prior uncertain send followed by denial/success/fault | Earlier uncertainty survives; bounded summaries do not fabricate resolution |
| K13 | Definitive upstream outcome with active downstream delivery | Renewal continues; later loss stops delivery without rewriting circuit classification |
| K14 | Terminal decision races and late callbacks | First winner, at-most-once circuit completion/release; no fabricated response |
| K15 | Stop/release ordering with stalled worker/finalizer | No release racing renewal; eligible release not held by independent executor finalizer |
| K16 | Request/process deadlines in both orders; exact cutoff | Earliest deadline, latched expiry, no new cleanup after cutoff or additional grace |
| K17 | Cutoff with renewal/release/circuit completion pending | Strong supervisor retention and truthful incomplete/not-attempted/unconfirmed accounting |
| K18 | Local lock delay and recovery cutover | No denied mutation; original generation retained; Redis handle never becomes local |
| K19 | F6 no-op, multiple writer/protocol generations, late confirmation | Existing closure guarantees and runtime/supervisor residual accounting remain passing |
| K20 | Cancellation before runner starts / shutdown admission race | Existing registration ownership and no unsupervised managed work preserved |
| K21 | Redaction and repeated lifecycle entry | No raw handles, credentials, Redis keys or exception text; no duplicate actions |

The full R2 matrix remains required across backend regression and integrated tests. Each row MUST identify its evidence level; an unchanged backend test cannot substitute for a claimed lifecycle integration assertion.

## L. Disposable Redis acceptance

Use the existing opt-in isolated fixture or an equivalently ownership-checked disposable fixture. Never use shared Gateway Redis for FLUSHDB, ACL changes, outages or destructive commands. Preserve exact container identity/label checks and verified cleanup. No real secrets or production endpoints are permitted.

| ID | Actual lifecycle scenario | Required observation |
| --- | --- | --- |
| L01 | Healthy long-lived synthetic request | Real acquire, multiple guarded renewals, competing admission denied while held; finish and one release permit subsequent admission |
| L02 | Gated real connection preparation | Expire live eligibility before writer boundary; zero renewal-command bytes for denied attempt, unchanged anchor |
| L03 | Controlled NOSCRIPT recovery | Real Redis replies plus integrated producing timestamp; delayed retry denied without fabricated result |
| L04 | Controlled post-send uncertainty | Retained task/attempt evidence, existing UNKNOWN horizon and no opaque retransmission; no falsely confirmed cleanup |
| L05 | Request/process cutoff with guarded work pending | Shared scope authority, no late dispatch/cleanup, retained residuals; eventual observation never revives execution |
| L06 | Regression of accepted F4A/F6 real sockets | Both managed pools retain actual socket-confirmation guarantees; fixture-only cleanup is not product recovery evidence |

Real Redis establishes backend/socket evidence only for exercised cases. Synthetic executor hooks are not production upstream transport or platform process-shutdown acceptance. Report Redis/client versions and separate real I/O from controlled fault injection.

## M. Regression and quality gates

Run focused F4B unit/integration tests first, accepted F4A/F6 tests, P1C lifecycle/supervisor/lifespan regressions, relevant M3 concurrency/provenance/dependency/local/recovery tests, safe multiprocess tests where available and the broad safe suite. Explicitly list destructive shared-service exclusions and opt-in skips; do not claim skipped acceptance passed.

Run Ruff, format check, compileall with external bytecode storage, tracked/untracked whitespace, UTF-8/no BOM/no NUL checks and Git status/index verification. Use isolated external pytest temporary directories. Record exact commands, counts, versions and environmental blockers. No migration or OpenAPI change is expected; no shared-database migration round trip is authorized.

## N. Independent source-review requirements

After implementation, review the actual tracked/untracked source and diff, not test counts alone. Verify exhaustive dispositions, live synchronous guard state, producing timestamps, cancellation evidence, bounded ownership, cutoff/release sequencing, provenance and unchanged F4A/F6 closure semantics. A blocker needs exact location, violated invariant and reproducible evidence/direct source proof. Any changed acceptance scope requires explicit approval.

## O. Completion and STOP conditions

These gates are distinct:

| Gate | Meaning / current package claim |
| --- | --- |
| F4A source acceptance | Accepted dependency baseline, not deployment certification |
| F4B source acceptance | Future independent review of integration; not yet achieved |
| F4B completion | Approved boundary implemented, required tests/quality gates passed and independent source acceptance recorded; not yet achieved |
| Platform shutdown certification | Separate R1 real-server/platform gate; not certified by this package |
| Guarded TLS acceptance | Separate R2 real-TLS gate; guarded TLS remains disabled pending acceptance |
| Overall M4 release acceptance | Separate milestone acceptance; not established by F4B completion |

The owner explicitly approved final R2 semantics and freeze, this F4B boundary and conditional authorization of bounded F4B implementation. The existing F4A source-acceptance record and matching working-file hashes were verified as recorded in R2 section 12. Implementation MUST NOT begin until the dedicated two-document freeze commit exists. That commit does not include or certify implementation changes.

STOP on a demonstrated frozen-contract contradiction, need to weaken closure evidence/provenance/earliest cutoff, need for new retry/fallback/configuration semantics, inability to retain obligations truthfully, or a demonstrated defect requiring expansion beyond this scope. Report the exact evidence rather than silently changing accepted behavior. Environmental inability to execute a mandatory acceptance case leaves that case pending, not passed.

### Requirement-level compatibility verification

| Requirement | Source authority | Preservation | Assessment |
| --- | --- | --- | --- |
| Atomic renewal / no resurrection | M3-R3, Atomic renewal | F/G/J leave Lua and backend time unchanged | COMPATIBLE |
| Exact availability errors / propagated cancellation | M3-R7, Installed classification and cancellation | F/H preserve errors independently of send certainty | COMPATIBLE |
| Issuing backend / retained generation / restart | M3-R10 sections 4–7 | J plus K07/K18 | COMPATIBLE |
| Worker active through downstream lifetime | R8 section 2; R1 section 3 | E and K13 distinguish outcome from terminalization | COMPATIBLE; explicit clarification |
| Cadence / producing anchor / hard horizon | R8 sections 3–4; R2 section 6 | G and K02/K05/K06/K08 | COMPATIBLE |
| Suppression without backend false | R2 section 5; R8 sections 4–5 | F and K09 enforce existing owner boundaries | COMPATIBLE; explicit clarification |
| First-winner circuit/client decision | R9 section 4 | F/H and K13/K14 preserve outcome | COMPATIBLE |
| Earliest deadline / irreversible expiry | R1 sections 3–5 | I and K16 preserve original authority, no new G | COMPATIBLE |
| Stop renewal before one release | R1 section 7; R8 section 5; R9 section 5 | I and K15/K17 | COMPATIBLE |
| Strong residual ownership / no false completion | R1 section 8; R2 sections 4–5 | H/I and K11/K12/K17 | COMPATIBLE |
| Writer-generation confirmation / retained resources | Accepted F4A notes, F6 paragraphs | I and K19/L06 preserve accepted behavior | COMPATIBLE; no backend redesign |
| Separate platform and TLS certification | R1 section 9; R2 section 10 | D/L/O keep gates outstanding | COMPATIBLE; acceptance boundary clarification |
| Approval before implementation | R2 section 12 | A/O record explicit owner approval and the commit prerequisite | OWNER APPROVED; freeze commit required before implementation |

No frozen-contract contradiction was identified in the independent approval-package review. The owner decision supplies explicit approval; the dedicated freeze commit remains the implementation-start gate. No numeric G or deployment certification is supplied here.
