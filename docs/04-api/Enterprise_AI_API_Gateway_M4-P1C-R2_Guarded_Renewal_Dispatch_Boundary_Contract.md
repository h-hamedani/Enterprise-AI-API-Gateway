# M4-P1C-R2 — Guarded Renewal Dispatch Boundary Contract

Status: **FROZEN — OWNER APPROVED — BOUNDED F4B IMPLEMENTATION AUTHORIZED AFTER THE FREEZE COMMIT**

Baseline: `feature/m3-runtime`, `36355d52b5c9118b181100caf850f0e4526e2a76`.

## 1. Purpose, authority and scope

M4-P1C-F4 reproduced a renewal that passed an early M4 eligibility check but reached Redis command dispatch after the ownership-confidence horizon. Queuing a coroutine leaves connection, script-loading and transport awaits ahead of its actual write. This additive companion defines guarded renewal dispatch and its evidence for future M4 integration.

The following authorities retain their existing responsibilities.

| Authority | Preserved responsibility |
| --- | --- |
| [M3-R3](Enterprise_AI_API_Gateway_M3-R3_Leased_Concurrency_Semaphore_Contract_Reconciliation.md) | Atomic layered renewal, exact lease identity, Redis `TIME`, expiry, immutable `RenewResult` and one bounded NOSCRIPT reload/retry |
| [M3-R7](Enterprise_AI_API_Gateway_M3-R7_Redis_Dependency_Error_Boundary_Contract.md) | Redis dependency error classification |
| [M3-R10](Enterprise_AI_API_Gateway_M3-R10_Protection_Handle_Provenance_Contract.md) | Original issuing backend, process ownership and issuing local generation |
| [M4-R8](Enterprise_AI_API_Gateway_M4-R8_Normal_API_Concurrency_Lease_Renewal_Contract.md) | Renewal cadence, dispatch anchors and confirmation-before-horizon requirement |
| [M4-R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md) | First terminal decision, cancellation, circuit classification and cleanup ordering |
| [M4-P1C-R1](Enterprise_AI_API_Gateway_M4-P1C-R1_Server_Shutdown_Request_Cancellation_Deadline_Contract.md) | Shared lifecycle duration, earliest absolute cutoff, irreversible expiry and residual obligations |

This contract qualifies the dispatch boundary of the additive guarded renewal path. It MUST preserve existing `renew(...) -> RenewResult` behavior and exceptions for legacy consumers. It changes no acquisition, ordinary release, circuit completion, rate limiting or unrelated Redis command path. Implementation authority is limited by the owner decision and section 12.

## 2. Ownership and eligibility

M4 MUST own the live terminal lifecycle state, effective request/process cleanup cutoff, current R8 ownership-confidence horizon, monotonic clock and interpretation of guarded outcomes. M3 MUST own provenance validation, backend selection, final guard invocation, execution mechanics, result interpretation and bounded NOSCRIPT handling. M3 MUST NOT infer M4 lifecycle policy from lease time or current traffic mode.

The caller MUST supply a synchronous guard and its process-local monotonic clock. The same clock domain MUST be used for the R8 anchor, confidence horizon and R1 effective cutoff. A decision MUST read live eligibility at invocation. A captured earlier boolean or queued-task timestamp is insufficient.

Permission requires all applicable conditions to hold simultaneously. Request execution has not terminalized, renewal remains enabled, R1 expiry has not latched, and the sampled time is strictly earlier than both the existing R8 horizon and every established applicable cutoff. Equality MUST deny. Process shutdown initiation alone MUST retain R1's existing allowance for active requests before their effective cutoff. Shutdown cancellation, latched expiry and an expired cutoff MUST deny.

For renewal eligibility, terminalization means terminal request execution/cancellation or entry into terminal cleanup. Recording a definitive upstream outcome does not by itself disable renewal while downstream delivery remains active. That outcome retains its existing R9 circuit-classification precedence. A later local cancellation may stop delivery without rewriting the recorded upstream outcome. Ordinary successful delivery activates cleanup only when delivery ends, under R1; it does not grant further renewal after terminalization.

The guard MUST perform only bounded synchronous eligibility checks, with no external I/O, await, task creation or lifecycle mutation. It MUST return exactly a Python `bool`. Awaitables, truthy objects and numeric substitutes MUST be rejected. Invalid/non-finite clock samples, clock faults and ordinary guard exceptions MUST fail closed as internal faults. `asyncio.CancelledError` MUST propagate under the cancellation rules in section 5. Diagnostics MUST use fixed safe categories and exclude raw exception contents.

## 3. Normative Redis dispatch boundary

Guarded Redis renewal dispatch is the final local command-send authorization boundary immediately before the connection's synchronous writer invocation queues the potentially lease-mutating `EVALSHA` command.

For each such attempt, M3 MUST perform this order.

1. Finish script preparation, pool checkout, connection acquisition/initialization, authentication, health checks, permitted reconnect preparation and command encoding.
2. In the execution segment that will invoke the writer, synchronously sample the M4-owned monotonic clock and invoke the caller guard with that sample.
3. Validate the strict decision. On denial or guard fault, queue zero renewal-command bytes for that attempt.
4. On permission, retain that sample as the attempt timestamp and invoke the synchronous writer without another await or scheduler handoff.
5. Await drain and response processing only after the write has begun.

If a socket-timeout wrapper schedules a send coroutine, steps 2–4 MUST occur inside that coroutine after any scheduling delay. Every reconnect or other preparation path leading to a new renewal write MUST traverse the boundary again. Entry into the facade, semaphore, script loader or public `evalsha()` MUST NOT be used as dispatch evidence.

The guarantee concerns local write initiation. It cannot guarantee when Redis receives or executes the command, and cannot undo an attempted write. Connection/authentication, PING and SCRIPT LOAD traffic are preparation traffic. Denial guarantees zero bytes for the denied renewal command, without claiming that preparation generated no traffic.

Terminal-state observation and the permission-to-write segment MUST share the process's synchronous coordination model. No lifecycle callback may introduce a handoff between authorization and write. Cross-thread lifecycle signals MUST enter that same coordination boundary before they become authoritative to this path.

## 4. Additive interface and evidence model

The conceptual signatures below define semantics. Production names and concrete immutable Python representations MAY differ.

```python
class RenewalDispatchGuard(Protocol):
    def monotonic(self) -> float: ...
    def permit(self, sampled_at: float) -> bool: ...

async def renew_guarded(
    policies, handle, *, guard: RenewalDispatchGuard
) -> GuardedRenewalOutcome: ...
```

The facade MUST receive the existing provenance-aware handle. Backend methods MAY receive its existing raw lease UUID after validation and routing. No new tenant/route identity, Redis key format, durable handle or version authority is introduced.

`GuardedRenewalOutcome` MUST be immutable and contain these conceptual fields.

| Field | Required meaning |
| --- | --- |
| `disposition` | One of `BACKEND_RESULT`, `NOT_OWNED`, `SUPPRESSED`, `UNKNOWN`, `GUARD_ERROR`, `INTERNAL_ERROR` |
| `result` | Existing `RenewResult` for `BACKEND_RESULT`; existing safe `RenewResult(False)` for `NOT_OWNED`; absent for all other dispositions |
| `dispatched_at` | Timestamp of the attempt producing `BACKEND_RESULT`; absent when no backend result was obtained |
| `attempts` | Bounded ordered immutable evidence for every permitted renewal write or local execution, including its monotonic timestamp and `RESULT`, `NOSCRIPT`, `RESOLVED_ERROR` or `UNRESOLVED` status |
| `failure_category` | Optional fixed sanitized category, with no exception text, command arguments or secrets |

There are at most two Redis EVALSHA attempts per guarded invocation under the existing NOSCRIPT allowance, and at most one local execution. Guard-denied candidates MUST NOT be recorded as dispatched attempts. For Redis, an attempt becomes `UNRESOLVED` immediately before entering the synchronous writer and remains so unless a definitive reply resolves it. A writer exception is conservatively uncertain unless zero-byte non-dispatch is positively established. Internal operation evidence MUST remain reachable by the owning task/supervisor while an operation is unresolved.

Each attempt MUST retain its original timestamp and issuing-backend association. `RESULT` MUST retain the authoritative `RenewResult`; `NOSCRIPT` MUST identify a definitive NOSCRIPT reply without a renewal result; `RESOLVED_ERROR` MUST identify a definitive non-NOSCRIPT Redis error reply with a fixed safe classification/category and no renewal result. `UNRESOLVED` MUST retain potentially attempted execution without a definitive result. A received malformed non-error reply remains a protocol fault with unavailable valid renewal truth; it MUST NOT become `RESULT` or a fabricated `RESOLVED_ERROR`.

History preservation applies to every disposition, propagated exception and cancellation. Finalization MUST preserve all preceding attempts in order, including their original timestamps and established results/statuses. Only additional permitted execution can append an attempt. Preparation, reload failure, denial and guard faults MUST NOT erase an earlier attempt or fabricate another. A definitive reply MAY resolve its own previously unresolved entry; later local events MUST NOT rewrite it. Attempt-history emptiness means that this invocation never entered a renewal writer or permitted local execution. Preparation traffic has no renewal-attempt entry.

`RESOLVED_ERROR` proves receipt and classification of an error reply for the recorded attempt. It MUST NOT imply successful renewal, `renewed=False`, missing ownership or general rollback/no-mutation. A script execution error may follow partial effects. ACL rejection and unsupported/invalid-command replies MUST retain M3-R7's non-dependency classification. Other definitive errors MUST use M3-R7's exact classification, including its explicitly allowed availability errors, rather than treating every Redis error as UNKNOWN. NOSCRIPT retains its separate status and existing recovery allowance. No error status authorizes a backend change or automatic retry.

`NOT_OWNED` covers existing no-execution paths such as invalid/unknown process provenance, absent issuing local generation or empty policy ownership. It MUST make zero backend calls and preserve M3-R10/R9's safe false interpretation. Its result MUST NOT be described as a Redis reply, and it has no dispatch timestamp. Existing policy/argument validation failures MUST retain their typed validation behavior and occur before backend execution.

## 5. Outcome interpretation and uncertainty

| Observation | Required interpretation |
| --- | --- |
| Valid authoritative result `1` or `0` | `BACKEND_RESULT` with the existing `RenewResult`, producing timestamp and attempt evidence |
| Guard denial before any permitted attempt | `SUPPRESSED`, absent result, empty attempts |
| Confirmed NOSCRIPT followed by denied retry | `SUPPRESSED`, absent result, one resolved NOSCRIPT attempt |
| Availability failure before any renewal write in the invocation | `UNKNOWN`, absent result, empty attempts and a safe preparation-failure category; R8 dependency handling applies although renewal is definitely unsent |
| Preparation/reload availability failure after a confirmed NOSCRIPT | `UNKNOWN`, absent result, preserve the earlier NOSCRIPT attempt/timestamp and record the later preparation-failure category; no retry attempt is fabricated |
| Availability failure after a write whose result remains unknown | `UNKNOWN`, absent result, preserve all prior attempts and the unresolved producing attempt |
| Definitive non-NOSCRIPT Redis error reply | Record `RESOLVED_ERROR`; choose `INTERNAL_ERROR` for M3-R7 non-dependency errors or `UNKNOWN` only for its explicitly classified availability errors; no renewal result |
| Guard raises or returns malformed data | `GUARD_ERROR`, absent result, no denied-attempt write; retain any earlier attempt evidence |
| Unexpected protocol/programming fault | `INTERNAL_ERROR`, absent result; preserve execution evidence and fail closed |

`SUPPRESSED` MUST NOT manufacture `renewed=False`, prove ownership loss or advance an anchor. `UNKNOWN` MUST retain R8's bounded dependency handling. Guard and internal faults MUST terminalize through the existing safe internal-fault path, without being reclassified as authoritative backend false or Redis availability failures. Definitive backend false and `NOT_OWNED` retain R9's loss semantics.

On `SUPPRESSED`, the M4 owner MUST re-evaluate its existing live terminal, renewal-enabled, confidence-horizon and lifecycle-cutoff state. It MUST preserve an already selected terminal decision and enforce any reached existing boundary without fabricating backend loss. Suppression MUST NOT authorize an additional retry, a new anchor or continued execution beyond an existing boundary. If suppression cannot be explained by the owner's eligibility state, it MUST follow the existing internal contract-fault path. This owner-side handling does not change the backend disposition or convert intentional terminal cleanup into a second fault.

Operation disposition and attempt certainty are independent. `UNKNOWN` identifies the applicable M3-R7/R8 dependency path; an `UNRESOLVED` attempt identifies uncertain execution. Thus reload failure after confirmed NOSCRIPT may be `UNKNOWN` with no unresolved mutation, and a definitive availability error reply may be `UNKNOWN` with `RESOLVED_ERROR` evidence. A guard/internal fault can retain older uncertainty without becoming an availability failure. Non-dependency authentication, ACL, command and configuration faults MUST remain internal/non-dependency failures. The guarded facade MUST preserve existing Redis-unreachable notification for a classified Redis dependency failure, with no local renewal substitution.

The following validation matrix is normative. `N`, `R`, `E` and `U` abbreviate attempt statuses `NOSCRIPT`, `RESULT`, `RESOLVED_ERROR` and `UNRESOLVED`. Bracketed sequences describe this invocation. A prefixed `N` is allowed only through the existing single NOSCRIPT recovery. Every nonempty entry retains its own timestamp. Earlier-invocation uncertainty is retained separately under bounded request/R1 accounting and MUST survive every row until actually resolved. Only `BACKEND_RESULT` has a producing `dispatched_at`; all other dispositions have that field absent, with timestamps available in their attempt evidence.

| Case | Disposition and attempt history | RenewResult | Uncertainty evidence | New R8 anchor |
| --- | --- | --- | --- | --- |
| Confirmed successful renewal | `BACKEND_RESULT`, `[R]` or `[N, R]` | Required true | Producing attempt resolved; retain any older outstanding uncertainty | Only if section 6 eligibility/confirmation rules hold |
| Confirmed renewed=False | `BACKEND_RESULT`, `[R]` or `[N, R]` | Required false | Producing attempt resolved | Never |
| Confirmed missing/stale ownership | Executed check uses preceding false row; no-execution provenance/generation/empty-policy path uses `NOT_OWNED`, `[]` | Required false; provenance false is no Redis reply | No new uncertain execution | Never |
| Suppression before any renewal send | `SUPPRESSED`, `[]` | Absent | No new uncertainty | Never |
| Confirmed NOSCRIPT then suppression | `SUPPRESSED`, `[N]` | Absent | NOSCRIPT resolved without script execution | Never |
| Confirmed NOSCRIPT then preparation failure | `UNKNOWN` for availability or `INTERNAL_ERROR` for non-dependency failure, `[N]` | Absent | Retain N; no second execution or uncertain lease mutation fabricated | Never |
| Confirmed NOSCRIPT then timely successful retry | `BACKEND_RESULT`, `[N, R]` | Required true | Both replies resolved | Only retry timestamp, subject to section 6 |
| Definitive non-NOSCRIPT Redis error | `INTERNAL_ERROR` for non-dependency or `UNKNOWN` for classified availability, `[E]` or `[N, E]` | Absent | Definitive error reply retained; no generic rollback claim | Never |
| Uncertain transport after send | `UNKNOWN` for classified availability, `[U]` or `[N, U]` | Absent | Required unresolved attempt evidence | Never |
| Uncertain send then denied later invocation | Earlier outcome/history stays retained; later `SUPPRESSED`, `[]` | Absent in later invocation | Earlier U MUST remain | Never |
| Guard fault before first send | `GUARD_ERROR`, `[]` | Absent | No new uncertainty; preserve any older U | Never |
| Guard fault after a prior resolved attempt | `GUARD_ERROR`, `[N]` within NOSCRIPT recovery | Absent | Preserve N and any older U | Never |
| Cancellation before a send | Before first send retain `[]`; during reload retain `[N]`; propagate cancellation in both cases | No fabricated result | No new uncertainty; preserve any older U | Never |
| Cancellation after attempted send | Propagate cancellation, retain `[U]` or `[N, U]` if reply unavailable; retain established R/N/E if already observed | Preserve an already observed result in its attempt record; no normal outcome required | Preserve available U; never downgrade known replies | Cancellation cannot authorize a new anchor |

For every normal outcome, `result` and `dispatched_at` MUST obey the field table. A `RESULT` in `BACKEND_RESULT` MUST be the final producing attempt, and its timestamp MUST equal `dispatched_at`. `NOT_OWNED` MUST have empty attempts. `SUPPRESSED` may contain only `[]` or `[N]` within one invocation. Other faults MUST preserve their actual history and cannot manufacture `RESULT`. Malformed replies or non-dependency local faults after a write MUST use `INTERNAL_ERROR` with unresolved execution evidence where valid renewal truth is unavailable. `RESOLVED_ERROR` MUST never carry `RenewResult`. Fixed error categories MUST distinguish preparation failure from an EVALSHA error reply. Validation failures remain typed exceptions as specified in section 4.

An earlier uncertain send MUST remain uncertain if a later guarded invocation is suppressed. M4 MUST retain unresolved attempt evidence until existing ownership/cleanup accounting resolves it. A later denial proves only that the denied attempt sent nothing. It MUST NOT erase uncertainty from another attempt or manufacture a completed renewal. Per-invocation evidence is bounded as defined above. Request accounting MUST use bounded summaries and R1-owned live obligations rather than accumulate an unbounded history of completed operations. No automatic retransmission after an uncertain write is authorized by this interface.

M4 MUST retain operation evidence independently of whether an invocation returns a normal outcome. Cancellation, a later suppressed invocation, a later successful renewal, task completion or runner return MUST NOT alone resolve an earlier uncertain attempt. Completed histories MAY be compacted into bounded truthful summaries; unresolved obligations MUST remain strongly owned under R1, including transfer to the supervisor before request ownership is relinquished. Compaction MUST NOT fabricate backend execution truth or authorize a new operation. Immutable published snapshots MUST remain unchanged; a definitive reply may resolve its own live operation record and produce a new snapshot under section 4. Backend execution uncertainty and physical socket-closure confirmation are separate obligations; evidence for one MUST NOT be substituted for the other.

Cancellation MUST propagate according to existing task ownership. It MUST NOT be converted to a normal false result, Redis UNKNOWN or guard/internal-error outcome, or swallowed as success. Cancellation before dispatch establishes no new uncertain send. After dispatch, its operation record MUST preserve available attempt timestamps, uncertainty and already observed replies/results. The record MUST distinguish preparation, drain and response-wait cancellation. A retained cancelled/unresolved operation remains part of R1 cleanup accounting even when no normal outcome is returned. The existing R9 first-winner terminal decision MUST remain authoritative. A later guard fault or cancellation MUST NOT overwrite an already recorded terminal decision or rewrite an already published valid anchor. An unexpected worker cancellation while still active retains R8's existing internal-fault handling at the request owner, without reclassifying the cancelled operation as a Redis failure.

## 6. Timing and timestamp provenance

R8's normal scheduling at `anchor + T/3`, one UNKNOWN retry at `anchor + T/2` and hard confidence horizon at `anchor + 2T/3` MUST remain unchanged. All comparisons use native monotonic precision. `now >= horizon` MUST deny without tolerance, rounding grace or extra retry. This companion adds no TTL or timeout value. Neither the R1 duration `G` nor any example deployment value, including `G=30`, grants renewal grace.

Only an authoritative `renewed=True` confirmed before the previously applicable confidence horizon, while the request remains eligible, MAY establish a new anchor. That anchor MUST be the dispatch timestamp of the attempt producing the successful result. Task creation, script-load entry, initial NOSCRIPT dispatch and response arrival MUST NOT substitute for that timestamp. A timely send with a late confirmation MUST NOT extend confidence or revive terminal execution. False results, suppression, UNKNOWN and faults never advance the anchor.

Only one renewal operation for a handle may be in flight. NOSCRIPT recovery remains inside that operation. R8's bounded business-level UNKNOWN retry remains separate and may begin only after the preceding operation has returned and live eligibility still permits it. This contract creates no extra business-level retry.

## 7. Redis adapter and NOSCRIPT

The approved architecture for the tested plaintext deployment is a renewal-only adapter with a dedicated bounded connection pool owned by the same Redis runtime lifecycle. It MUST use the issuing Redis destination, database and credentials explicitly and preserve existing policy/key construction. It MUST reuse compatible redis-py authentication, initialization, packing and response parsing. It MUST NOT fall back to another endpoint, database, credential set or backend. Pool ownership MUST include bounded acquisition/resource behavior and closure under the existing R1 lifecycle boundary, without a new grace duration.

The adapter MUST disable opaque automatic retransmission of renewal commands after uncertain sends. Connection preparation retry MAY retain existing applicable limits only while it cannot retransmit a renewal. Every eventual EVALSHA still requires fresh authorization. A connection with uncertain response alignment MUST be invalidated/disconnected before reuse. Clean suppression may return an aligned prepared connection to its pool.

One confirmed NOSCRIPT permits the existing bounded forced SCRIPT LOAD and one retry. The NOSCRIPT attempt MUST be marked resolved without lease renewal. After reload, the retry MUST receive a new timestamp and guard decision. Denial suppresses that retry with no fabricated result. A second NOSCRIPT or invalid reply MUST follow the existing protocol/internal-fault boundary rather than loop. An uncertain network write MUST NOT enter NOSCRIPT recovery without a definitive NOSCRIPT reply.

If reload connection acquisition or SCRIPT LOAD fails, the first NOSCRIPT entry MUST survive with its original timestamp, and no second EVALSHA entry may appear until a second writer is actually entered. M3-R7 determines whether that subsequent preparation failure is dependency UNKNOWN or internal/non-dependency. A second definitive NOSCRIPT yields `INTERNAL_ERROR` with `[NOSCRIPT, NOSCRIPT]`, absent renewal result and no third attempt. A definitive non-NOSCRIPT error during reload is preparation evidence, with no new renewal-attempt entry. A fully consumed definitive EVALSHA error reply MAY leave a connection reusable only when parser alignment and connection validity are established; otherwise it MUST be invalidated. Definitive error classification alone is insufficient proof of safe reuse.

This architecture MUST remain isolated from the existing unguarded client path. Shared global monkey-patching, interception of unrelated Redis commands and unbounded per-request pools are forbidden. Pool exhaustion MUST preserve bounded dependency/internal-fault handling without borrowing another backend or bypassing the guard. Exact resource capacity remains an implementation decision requiring boundedness evidence, with no new deployment default authorized here.

## 8. Local degraded backend

An additive guarded local method MUST route only to the handle's issuing generation, including a retired generation still valid under M3-R10. Existing local `renew()` behavior MUST remain unchanged.

Current `LocalConcurrencyStore.renew()` samples lease time before awaiting its lock, calls mutating `_clean()` inside the lock, and then checks/updates ownership. An outer M4 check cannot protect those operations across lock contention. The guarded method MUST therefore acquire the existing local lock before sampling/evaluating M4 eligibility, and MUST deny before calling `_clean()` or performing any other store mutation. On denial, owners, expiries, touched timestamps, entries and retention bookkeeping MUST remain unchanged.

On permission, the guarded method MUST sample the authoritative local lease clock under the lock and apply the existing cleanup, exact-owner/all-layer checks and finite expiry assignment synchronously without a further await. That clock determines local lease validity. The separate M4 monotonic sample determines eligibility and the dispatch anchor. This placement is specific to the new guarded method and does not rewrite legacy local timing. The permitted local execution yields the existing true/false `RenewResult` plus its M4 pre-execution timestamp.

The facade MUST retain existing issuing-generation completion/retention bookkeeping for confirmed local renewal. Suppression or a guard fault MUST NOT extend generation retention as though renewal succeeded. No local operation is converted into Redis ownership after recovery cutover.

## 9. Terminal cleanup and security

After terminalization or effective cutoff, new renewal dispatch MUST be denied even if preparation was already queued. R9's first recorded terminal decision remains authoritative. Suppression does not itself select a different public response, circuit outcome or retry policy. No fabricated circuit completion or second downstream response is allowed.

Cleanup MUST stop/cancel and await renewal and its owned operations before the one original-provenance release attempt, subject to R1's earliest irreversible cutoff. It MUST NOT race release against unfinished renewal. At cutoff, unresolved renewal tasks and release obligations MUST be strongly retained and reported under R1. Runner exit alone MUST NOT imply cleanup completion. A safely stopped suppressed renewal retains the original handle's release obligation when time remains. No denied/null acquisition creates an obligation, and no second release attempt is authorized.

F4B MUST preserve the accepted F4A/F6 managed Redis closure model. Each outstanding writer/protocol-generation obligation MUST retain its original confirmation evidence independently of the connection's current writer reference. Reconnection, protocol replacement, a writer-less disconnect or confirmation for another generation MUST NOT resolve an older obligation. Only successful normal completion with `None` of the captured original protocol confirmation future establishes closure under the accepted redis-py 8.1.0 integration. Missing, pending, cancelled, exceptional, unsupported or invalid-result confirmation MUST remain unconfirmed. Only confirmed obligations may be compacted.

Managed pool/runtime completion MUST check all retained writer obligations, including those for retired connections; the supervisor MUST retain unconfirmed Redis resources. Already-started establishment/disposal work remains owned until it actually terminates. Observing late confirmation MAY update factual accounting synchronously, but MUST NOT initiate cleanup, another waiter, retry or later disposal stage after cutoff, resurrect a pool, or convert a terminal failed runtime close into success. Waiting on existing confirmation before cutoff remains bounded by the original socket bound and R1 authority. No new duration is introduced. These requirements preserve accepted F4A/F6 behavior; they authorize no disposal redesign.

Guard evidence and diagnostics MUST exclude credentials, raw lease/probe identifiers, Redis keys, command payloads and raw exception contents. Guard decisions require no new tenant/service/route identifiers. Bounded existing categories MAY describe suppression, dependency uncertainty and internal faults. No new metric infrastructure or audit action is introduced.

## 10. Tested baseline, evidence and dependency gates

| Evidence level | Established scope |
| --- | --- |
| R2 reconciliation and R2A source assessment | Architectural/source feasibility for the final writer seam |
| R2B prototype | 23 passing synthetic RESP/TCP checks; overall HOLD for missing real Redis evidence |
| R2C real disposable Redis | 45 passing prototype checks using CPython 3.12.14, redis-py 8.1.0 and Redis 7.4.11; 65 relevant existing M3 regression tests passed |
| Production M3/M4 integration | Pending implementation, deterministic integration tests and independent source review |
| Deployment-platform acceptance | Pending acceptance on each supported deployment path |

The inspected Compose deployment uses plaintext Redis. R2C established real Redis feasibility under those tested conditions. Its temporary subclasses, injected failures and controlled clocks do not certify production lifecycle integration or every deployment platform.

The transport MUST retain redis-py 8.1.0 compatibility evidence. Writer access and associated connection internals are private dependencies. A library upgrade MUST revalidate initialization/authentication, health checks, command packing, actual synchronous writer placement, timed-send scheduling, connection retries, NOSCRIPT, cancellation, uncertain response disposal, pool ownership and closure. Installed and locked versions MUST match the accepted compatibility evidence. No private hook is promised as a stable public API.

Real TLS acceptance is mandatory before enabling guarded `rediss://` deployment. It MUST prove correct TLS connection-class selection, trust/certificate validation, authentication, guarded writer placement, command routing and cleanup. Plaintext evidence MUST NOT be advertised as generic TLS compatibility.

F4B integrated lifecycle acceptance, deployment-platform shutdown certification and guarded TLS acceptance are separate gates. F4B MUST satisfy the lifecycle acceptance boundary linked in section 12; backend-only/prototype evidence counts only for scenarios actually exercised. R1 real-server/platform shutdown acceptance remains a separately tracked release obligation and MUST NOT be represented as completed by F4B source acceptance. Guarded TLS MUST remain disabled until its separate real-TLS gate passes. F4B completion does not certify overall M4 release readiness.

## 11. Required deterministic acceptance matrix

Tests MUST use controlled monotonic clocks, explicit gates and observed write/mutation boundaries. Wall-clock timeouts MAY bound deadlocks. Arbitrary sleeps MUST NOT establish race correctness. Unblock and join test-owned tasks during teardown. Real Redis tests MUST use disposable resources, with no destructive operations against shared Gateway Redis.

| ID | Scenario | Required evidence |
| --- | --- | --- |
| 1 | M4 queued task delayed past horizon | Final guard denies; zero renewal bytes |
| 2 | Script-load delay past horizon | Preparation completes without authorizing late renewal |
| 3 | Pool-lock contention past horizon | Denial occurs after actual checkout wait |
| 4 | Authentication reply delayed | Fresh guard after initialization denies late write |
| 5 | Health-check reply delayed | PING traffic does not bypass the final guard |
| 6 | Connection acquisition delayed | Zero renewal writes after eligibility expires |
| 7 | Timed-send coroutine delayed | Guard executes inside the actual writer coroutine |
| 8 | Reconnect/retry preparation delayed | Fresh guard denies; no hidden retransmission |
| 9 | Initial NOSCRIPT and timely retry | Two decisions/timestamps; success uses retry timestamp |
| 10 | Initial NOSCRIPT and late retry | Only initial EVALSHA written; no false result |
| 11 | Exactly at horizon | Strict denial at equality |
| 12 | Timely dispatch, late confirmation | Preserve dispatch evidence; no anchor advancement/revival |
| 13 | Terminal cancellation during preparation | Live terminal state denies; first terminal decision retained |
| 14 | Process shutdown during preparation | Existing active-request allowance preserved before cutoff; shutdown cancellation/expiry denies |
| 15 | Request cleanup cutoff during preparation | Latched/effective cutoff denies without new grace |
| 16 | Guard exception or malformed decision | Zero denied-attempt writes; internal fault, no backend false |
| 17 | Post-write transport uncertainty | UNKNOWN with retained timestamp/evidence |
| 18 | Cancellation during drain | Unsafe connection invalidated; unresolved operation retained |
| 19 | Cancellation during response wait | No unread-response contamination or fabricated result |
| 20 | Uncertain send and later denial | No automatic retransmission; earlier uncertainty survives |
| 21 | Issuing backend/database | Original Redis identity or local generation only; zero cross-routing |
| 22 | Real authentication and DB isolation | Wrong password rejected; correct credentials and selected DB verified |
| 23 | Real Lua lease authority | Redis TIME expiry, all-layer atomicity, valid/stale/expired results |
| 24 | Local lock crosses horizon | Under-lock guard denies queued renewal |
| 25 | Local denial with expired entries | No cleanup/owner/touched/retention mutation; local lease clock still governs permitted execution |
| 26 | Legacy M3 callers | Existing result types, exceptions, validation and unguarded behavior preserved |
| 27 | Release after suppressed renewal | After renewal stops, one eligible release with original handle |
| 28 | Terminal races and late callbacks | No duplicate release/circuit completion or second response |
| 29 | Runtime shutdown/pool disposal | R1 cutoff respected; no orphan tasks/connections or falsely completed obligations |
| 30 | Dependency compatibility | Locked/installed redis-py evidence and all private-seam checks pass |
| 31 | Initial NOSCRIPT then SCRIPT LOAD failure | Preserve first N/timestamp, no second attempt/result/anchor; subsequent failure follows M3-R7 |
| 32 | Initial NOSCRIPT then reload connection failure | Preserve N and dependency preparation category; zero retry renewal writes |
| 33 | Definitive EVALSHA ACL error reply | E/timestamp retained, internal/non-dependency, no RenewResult or automatic retry |
| 34 | Definitive non-NOSCRIPT command error | E/timestamp/category retained; apply M3-R7, no false renewal or generic rollback claim |
| 35 | Prior uncertain send then denial or local fault | Earlier U survives later SUPPRESSED/GUARD_ERROR/INTERNAL_ERROR; no new unauthorized write or anchor |
| 36 | Cancellation precedence during preparation | Exercise cancellation before first send and after N; propagate it, retain history, preserve first terminal decision against guard faults |
| 37 | Cold and warm guarded pools | Real initialization and reuse both reach the final writer guard; delayed/denied paths retain exact history/result/timestamps |
| 38 | Reuse after a definitive error reply | Fully consume error, preserve E/category, then parse subsequent independent operation correctly; reuse only when alignment is established |
| 39 | Uncertain response invalidation | Dispose unsafe connection; no opaque retransmission, unread-reply reuse, lost U or fabricated result |

Production acceptance MUST cover the full matrix. Prototype passes MUST be attributed only to scenarios actually exercised. Production integrated lifecycle tests and independent source review remain mandatory even after real Redis feasibility passes.

## 12. Implementation boundary and review gate

The owner approved the six reviewed normative amendments and the [F4B Integration Acceptance Boundary](../05-implementation/Enterprise_AI_API_Gateway_M4_P1C_F4B_Integration_Acceptance_Boundary.md). The independent approval-package review passed with owner approval required. This freeze records that explicit owner decision; it is not inferred from source acceptance.

The F4A/F6 source-acceptance prerequisite was verified against this chat's independent acceptance record at HEAD `bb1c577c581afdf93eb31d5d4058b191f66df680`: `M4-P1C-F4A-F6 SOURCE REVIEW PASS — F4A SOURCE ACCEPTED / F4B READY`. All 32 recorded working-file hashes, including source, tests and eight access logs, still matched before this freeze. The accepted F4A implementation remains uncommitted and MUST be preserved; the HEAD alone is not its source artifact.

Bounded F4B lifecycle integration is authorized only after this contract and its approved acceptance boundary are committed together in the dedicated two-document freeze commit. Until that commit exists, implementation MUST NOT begin. This authorization consumes the accepted guarded backend and permits only the minimal backward-compatible request-lifecycle integration within the approved boundary. Existing legacy renewal APIs and frozen M3/M4 algorithms MUST remain unchanged.

No public Normal API exposure, upstream HTTP implementation, request/response streaming implementation, migration, Admin/OpenAPI change, new retry or fallback policy, lease authority/duration, deadline or cleanup duration is authorized. Existing approved R1 configuration authority remains separate; no value of G is selected or changed. A demonstrated need to expand scope MUST stop work for explicit change control.

The independent F4A source review and approval-package review satisfy the source/contract review prerequisite for this freeze. F4B implementation, deterministic and real-Redis acceptance, and independent F4B source review remain outstanding. Approval and this freeze do not certify platform shutdown, guarded TLS or overall M4 production readiness.

This outcome-model repair preserves M3-R3 atomic renewal and backend clock authority, M3-R7 exact dependency/error classification, M3-R10 original provenance, M4-R8 cadence/horizon/confirmation rules, M4-R9 first-winner precedence, and P1C-R1 absolute deadlines, cleanup ordering and residual ownership. No frozen authority is superseded.
