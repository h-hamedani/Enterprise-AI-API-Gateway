# M4-P1C-R1 — Server Shutdown and Request Cleanup Deadline Contract

Status: **CONTRACT DRAFT — APPROVED ARCHITECTURAL DIRECTION; NUMERIC DEPLOYMENT VALUE PENDING.** Documentation only. Baseline: `feature/m3-runtime`, `eadb46ccc145e30aafca0f8939d645238fdd0e16`.

## 1. Authority, scope and existing gap

[M4-R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md), section 5, requires cancellation-safe, bounded request cleanup and references the server's configured graceful boundary. It does not supply a finite duration or expose that boundary to request scopes. This complementary contract supplies that missing lifecycle authority; it does not edit or redefine R9's outcome precedence or public behavior.

At the baseline, `app/main.py` owns shared resources through application lifespan, and `scripts/run-dev.ps1` and `scripts/run-dev.sh` launch Uvicorn without a graceful timeout. Compose runs PostgreSQL and Redis, not the application server. Installed Uvicorn's `timeout_graceful_shutdown` defaults to `None`; no finite value may be inferred. Its request draining and subsequent application lifespan shutdown are distinct phases. Existing M3 worker stop paths cancel and await their tasks; their operation/reconciliation bounds do not establish a common process shutdown deadline.

[M4-R8](Enterprise_AI_API_Gateway_M4-R8_Normal_API_Concurrency_Lease_Renewal_Contract.md) supplies lease-confidence timing, and [M4-R10](Enterprise_AI_API_Gateway_M4-R10_Normal_API_Upstream_Timeout_Policy_Contract.md) supplies upstream timeouts. Neither supplies a lifecycle cleanup grace. Neither may be reused as one by inference. The uncommitted P1C Event-only interface is not sufficient deadline authority and is not certified compliant by this document.

## 2. One deployment configuration

One deployment-owned duration `G` in seconds MUST supply both request cleanup grace and process graceful-shutdown grace:

| Property | Contract |
| --- | --- |
| Settings field | `lifecycle_grace_timeout_seconds` |
| Environment name | `LIFECYCLE_GRACE_TIMEOUT_SECONDS` |
| Configuration owner | Deployment/operator, startup-only |
| Valid value | Explicit positive finite duration in seconds |
| Default | None; no numeric default is authorized |
| Requiredness | Mandatory before enabling the future managed Normal API execution path |
| Invalid input | Absent, zero, negative, non-finite or otherwise invalid values MUST fail startup validation for that path |

The existing Settings/environment mechanism is the configuration surface. A boolean is not a duration. This contract adds no special numeric string spelling requirement, tenant/service/route override, dynamic reload, clamping, or per-subsystem grace setting. Validation must also reject an unusable non-finite derived deadline rather than silently fall back to unbounded waiting. The actual positive finite deployment value of `G` remains an explicit approval item. Configuration and launcher changes are deferred; this document does not add a Settings field or environment file entry.

## 3. Supervisor ownership and deadline calculation

One **process-local Lifecycle Supervisor** owns the validated `G`, the monotonic time source, process shutdown state, request registrations and unfinished-work obligations. Each request has one request-local cleanup owner under that supervisor. This is lifecycle ownership, not a new distributed coordinator, version authority or M3 traffic-state store.

All timestamps MUST use the same process-local monotonic clock domain. Deadlines are absolute timestamps, not restartable relative waits:

```text
D_request = first_terminal_cleanup_activation_time + G
D_process = first_process_shutdown_activation_time + G
D_effective = earliest established applicable deadline
```

Each base deadline is assigned once. The effective deadline may tighten when another applicable earlier deadline becomes known, but MUST NOT move later. A request's first accepted terminal cancellation decision MUST synchronously establish cleanup activation before awaiting cleanup; a later finalizer cannot defer that activation. Ordinary successful execution MUST establish it on transition into terminal cleanup as well. A recorded definitive upstream outcome alone MUST NOT activate cleanup while downstream delivery remains active.

An active request without cleanup activation has no request cleanup lifetime timer. This is not an end-to-end request deadline. Once process shutdown begins, its process deadline applies to every registered active or cleaning-up scope. A later request cleanup activation cannot grant time beyond that process deadline. Repeated cancellation, repeated shutdown notification, callbacks, release attempts and finalizer stages MUST NOT restart either deadline.

## 4. Cancellation and finalization modes

| Mode | Execution authority and client/circuit semantics | Deadline activation |
| --- | --- | --- |
| Normal client disconnect | R9 terminates request-scoped work; no fabricated response or circuit outcome | Activate this request's cleanup, not global shutdown |
| Application-policy/request-task cancellation | Existing R9 terminal gate; no new public error or upstream attempt | Activate this request's cleanup, not global shutdown |
| Winning upstream timeout | R10 retains phase classification and pre-/post-commit result | Activate request cleanup when execution terminalizes |
| Lease loss or R8 confidence expiry | Stop protected execution immediately; R9 existing 500 boundary if applicable, otherwise terminate delivery | Activate request cleanup; `G` does not extend ownership confidence |
| Ordinary successful finalization | Delivery has ended; preserve selected upstream/client result | Activate request cleanup even without cancellation |
| Graceful process shutdown | Close admissions; existing requests may finish subject to all existing protections until process cutoff | Activate `D_process`; propagate it immediately |
| Forced process termination | Process may end without cooperative cleanup | No new grace window or cleanup guarantee |

R9 first-recorded outcome precedence remains authoritative. A later cause may stop remaining execution/delivery but MUST NOT rewrite an already definitive upstream result. Requests already cleaning up continue under the earlier applicable deadline, not a fresh shutdown window.

## 5. Expiry is irreversible and independently enforceable

The supervisor/scope deadline abstraction MUST provide permanently latched expiry. Expiry is established when the monotonic clock reaches the effective deadline or an authoritative expiry notification has been observed. A notification means cutoff expiry, not merely shutdown initiation. `Event.clear()` MUST NOT be interpreted as recovery from shutdown or undo an observed cutoff.

A notification Event may wake an observer but MUST NOT be the sole authority. With the Event permanently unset, an activated finite deadline MUST still end cleanup waiting. Once an expiry waiter completes, its observation MUST be retained; a completed waiter MUST NOT repeatedly wake an unfinished join without terminating that wait. Cancellation, clearing/replacing a notification, late callbacks and repeated cleanup entry MUST NOT reset latched expiry.

Every awaited cleanup join MUST be bounded by the same effective absolute deadline, including joins reached after earlier awaits. At or after expiry, no new awaited cleanup operation, release attempt or unbounded finalizer join may start. Already completed results may be inspected synchronously. Cancel unfinished work, account for it, and relinquish waiting without claiming that cancellation terminated it. No cancellation shield may turn this cutoff into indefinite waiting.

The bound governs cooperative waiting and permission to continue work, not forcible Python task termination. If the event loop itself is blocked, physical execution at the exact timestamp cannot be guaranteed; on resumption, deadline checks MUST precede additional protected work or cleanup waits. Hard process termination remains an external deployment responsibility.

## 6. Future Server Adapter / Lifecycle Supervisor interface

The minimum interface is conceptual; these names do not prescribe a production implementation:

| Operation/evidence | Required owner and behavior |
| --- | --- |
| Configure supervisor | Startup validates explicit finite `G` before managed execution is enabled |
| Register scope | Supervisor retains active request ownership and associates the common clock/deadline authority |
| `begin_cleanup()` | Scope establishes `D_request` once at first terminal cleanup activation; repeated calls are idempotent |
| `begin_shutdown()` | Server Adapter synchronously latches shutdown, closes new admissions and fixes `D_process` before awaiting drain |
| `deadline` / `expired` / `wait_expired()` | Expose earliest applicable deadline and irreversible expiry; waiting is deadline-driven, not dependent on eventual Event delivery |
| Propagate process cutoff | Supervisor tightens all active/cleaning scopes immediately without changing their terminal-result authority |
| Retain incomplete ownership | Before runner ownership is relinquished, supervisor accepts strong task references, original handles and unresolved obligations |
| Observe eventual completion | Supervisor consumes task results/exceptions and updates factual accounting without reviving execution or retrying release |

Scope registration and shutdown admission closure MUST share one atomic synchronization boundary. A scope racing shutdown is either registered and supervised before closure, with the process cutoff propagated to it, or rejected before managed execution. A scope MUST NOT begin managed work without registration. No particular lock primitive is prescribed.

Deregistration MUST occur only after all owned work is complete or residual tasks and obligations have been explicitly transferred to the supervisor for continued retention and observation. Runner return alone is not proof that ownership is finished; deregistration MUST NOT discard unfinished work or unresolved release obligations.

Shutdown initiation is not automatically request cancellation: existing active requests may finish within the process boundary, under R8/R9/R10. At process expiry, remaining execution is cancelled, expiry is latched, and scopes cannot obtain an additional `G` for cleanup. If no time remains to perform safe release, that is an explicitly unattempted obligation, not permission to race renewal.

The Server Adapter MUST integrate before request draining, not first discover shutdown in the application's lifespan `finally` block. Uvicorn request draining and application lifespan resource closure MUST consume the same process cutoff. Worker shutdown, shared-client closure and other awaited lifespan finalization MUST NOT each receive a new grace window. Existing M3 algorithms are unchanged; integration must supervise lifecycle waiting rather than infer a new deadline from worker internals.

Configuring only Uvicorn's timeout does **not** prove complete process shutdown compliance. Future wiring must show that server cancellation, registered scopes and application lifespan observe the same authorized cutoff. The shared client retains its R5 lifespan ownership and R9 cleanup ordering to the extent the process boundary permits. A forced stop may interrupt any phase; no extra drain timeout or promise of complete resource closure is introduced.

## 7. Cleanup sequence and release obligations

All terminal paths MUST share one cleanup owner and this sequence:

1. Stop/cancel active request execution and renewal; no new renewal starts after terminalization.
2. Await renewal and its owned operations only within the effective cutoff. Cancelling the worker alone is not evidence that an underlying renewal has stopped.
3. When renewal has safely stopped and time remains, make the one intended provenance-aware release attempt for the original real concurrency handle. An executor finalizer waiting independently MUST NOT hold that eligible release hostage.
4. Join remaining request finalizers, receive/upstream work and any already applicable circuit-completion work within the same cutoff.
5. At cutoff, retain and report unfinished tasks and unresolved obligations; do not claim successful cleanup.

Release MUST NOT race an unfinished renewal. Null/denied ownership creates no release obligation. False/failed release preserves existing M3 semantics and does not replace the chosen public result. There is no backend substitution, second acquisition, detached cleanup retry, release retry or new circuit completion primitive.

Existing dependency timeouts remain operation-specific limits; an earlier lifecycle cutoff caps waiting, without restarting the dependency timeout. If release cannot safely begin before cutoff, record **not attempted**. If it began but its result is unknown at cutoff, record **attempted, unresolved**, not released. An explicit false return is a confirmed false result, not a success. Preserve an actual confirmed result when available. M3 lease expiry remains the crash/forced-shutdown safety net, not the normal substitute for the required release attempt.

## 8. Incomplete-task ownership and observation

Python cannot forcibly terminate a coroutine that suppresses cancellation. A runner may relinquish waiting at cutoff only with truthful incomplete-cleanup reporting and explicit supervisor ownership of residual tasks. Strong references MUST survive request runner exit; task results/exceptions MUST be observed safely while the process remains alive. Keep original handle provenance and fixed task-role/obligation categories; no raw handles, secrets, Redis keys or uncontrolled exception text enters diagnostics.

The supervisor MUST distinguish unfinished execution, renewal, release and applicable circuit-completion work. It MUST retain whether release was not attempted, attempted/unresolved, confirmed false, confirmed failed or confirmed successful. Eventual completion may update factual accounting but MUST NOT change the selected response, fabricate a circuit outcome, authorize renewed work or cause another release attempt. Do not report that the cutoff completed all cleanup merely because residual tasks finish later.

This retention is not a scheduler or retry queue. It creates no new operations after cutoff. Forced process termination may prevent observation, release and diagnostics entirely; no durable-cleanup guarantee or cross-process handoff is claimed.

## 9. Required deterministic acceptance matrix

Use controlled monotonic clocks, event gates and explicit completion acknowledgements. Wall-clock waits may serve only as test deadlock guards, not arbitrary sleep-based correctness proof. Always unblock/join deliberately non-cooperative fixtures during test teardown.

| Scenario | Exact required observable outcome |
| --- | --- |
| Client disconnect without server shutdown | Request cleanup activates once; process admissions remain open; deadline ends waiting for a blocked finalizer |
| Application-policy cancellation | Only the selected scope terminalizes; unrelated requests retain their own deadlines and execution |
| Upstream timeout or lease-confidence loss | Existing R10/R8 decision and circuit classification remain intact; cleanup grace never permits resumed protected execution |
| Unset notification Event | Advancing to the absolute cutoff finishes runner waiting with incomplete child accounting; no duplicate release |
| Set / observe / clear expiry Event | Expiry remains true after clear and repeated entry; no new join/release starts and no completed-waiter wakeup loop occurs |
| Repeated cancellation/shutdown notifications | Captured base deadlines remain identical; effective deadline never increases; at most one release attempt |
| Successful delivery then stalled cleanup | Request cleanup deadline activates despite success; runner reports incomplete cleanup without rewriting delivered/committed outcome |
| Renewal operation pending at cutoff | Original operation remains accounted for; release is not raced and is recorded not attempted; late renewal cannot revive execution |
| Executor finalizer stalled | After renewal stops, eligible release occurs before finalizer acknowledgement; at cutoff unfinished executor remains strongly owned |
| Release pending at cutoff | Exactly one attempt; no false confirmed release; pending task/result remains owned and observable |
| Global shutdown during existing cleanup | Effective cutoff becomes the minimum of existing request/process deadlines, including both relative orderings; neither base resets |
| Shutdown during active execution | Admission closes synchronously; active request may finish before cutoff; at cutoff remaining work is cancelled with no extra cleanup window |
| Scope registration racing process shutdown | Exercise both atomic orderings: registration first yields a supervised scope receiving the process cutoff; closure first rejects the scope before managed work. No scope starts unregistered or escapes shutdown ownership |
| Cancellation before request runner's first execution step | Cancel before the runner body executes; no managed work starts unsupervised. The registering owner/supervisor accounts for the cancelled runner and any existing obligations without relying on its unentered cleanup block; deregistration requires completion or explicit residual transfer |
| Forced termination | No second grace window, no claimed guaranteed release/task termination/log persistence; lease safety net unchanged |
| Already definitive circuit result | Later cancellation/cutoff preserves original classification; no fabricated or duplicate completion; unresolved completion is reported |
| Downstream already committed | Commitment stays true even across a response-start acknowledgement race; remaining delivery stops without a second envelope |
| Tasks finish after runner exit | Supervisor retains references before exit, observes final results/exceptions, and never restarts execution or retries release |
| Server drain followed by lifespan cleanup | Both use the same `D_process`; resource/worker cleanup cannot start a second full grace period |

Real server acceptance MUST separately demonstrate admission closure, request draining, cancellation and lifespan coordination with the pinned server version. Fake clocks and ASGI-only tests do not prove process shutdown or network transport behavior. No such acceptance is claimed by this documentation change.

## 10. Frozen-contract compatibility

| Authority | Preserved boundary |
| --- | --- |
| [M4-R7A](Enterprise_AI_API_Gateway_M4-R7_Normal_API_Protection_Admission_Ordering_Contract.md) | Admission order, rate consumption, probe-safe preparation and one original-handle release obligation unchanged |
| [M4-R8](Enterprise_AI_API_Gateway_M4-R8_Normal_API_Concurrency_Lease_Renewal_Contract.md) | `T/3`, `T/2`, `2T/3`, confirmed dispatch anchors, UNKNOWN handling and provenance unchanged; cleanup grace never extends protected execution |
| [M4-R9](Enterprise_AI_API_Gateway_M4-R9_Normal_API_Cancellation_and_In_Flight_Abort_Contract.md) | First-winner outcome, request-scoped abort, safe public mapping and stop/await-before-release preserved; this complement supplies previously missing finite lifecycle authority |
| [M4-R10](Enterprise_AI_API_Gateway_M4-R10_Normal_API_Upstream_Timeout_Policy_Contract.md) | Upstream clocks, route precedence and timeout classification unchanged; cleanup cutoff is not a new upstream or total-request timeout |
| [M4-R17](Enterprise_AI_API_Gateway_M4-R17_Normal_API_Response_Streaming_Commit_Contract.md) | Factual commitment, bounded streaming and no second envelope preserved; upstream completion alone does not end downstream lifetime |
| [M4-R18](Enterprise_AI_API_Gateway_M4-R18_Normal_API_Current_State_Consumer_Contract.md) | Coherent immutable capture, certificates, tenant isolation and no request-time DB fallback unchanged; no new consumer/version authority |
| [M3 concurrency](Enterprise_AI_API_Gateway_M3-R3_Leased_Concurrency_Semaphore_Contract_Reconciliation.md) / [M3-R10](Enterprise_AI_API_Gateway_M3-R10_Protection_Handle_Provenance_Contract.md) | Atomic lease behavior, expiry/no-resurrection and issuing-backend routing unchanged |
| [M3-R11](Enterprise_AI_API_Gateway_M3-R11_Unknown_Circuit_Completion_Result_Contract.md) / [M3-R12](Enterprise_AI_API_Gateway_M3-R12_Local_Circuit_Completion_Lifetime_Contract.md) | APPLIED/STALE/UNKNOWN_HANDLE and completion lifetimes unchanged; cutoff is not evidence of upstream failure or invented circuit state |

The forced-cutoff exception is bounded waiting with explicit residual ownership under R9's graceful-lifecycle limitation, not permission to leave untracked renewal/upstream tasks or omit ordinary cooperative cleanup. No frozen document is modified. No public API/error, Admin/OpenAPI, schema/migration, retry/fallback, browser CORS, traffic algorithm, configuration snapshot or transport behavior changes here.

## 11. Approval and implementation boundaries

**A. Approved architectural direction:** one shared deployment duration, absolute monotonic deadlines, earliest applicable cutoff, irreversible expiry and process-local supervisor ownership. This document records that accepted direction as a complementary contract draft ready for source review.

**B. Pending deployment approval:** the actual positive finite value of `G`. No number is selected, defaulted or implied by Uvicorn, dependency timeouts, lease TTL or this draft. A deployment cannot claim compliance without an approved explicit value and validated wiring.

**C. Deferred implementation:** Settings validation, launcher/server integration, Lifecycle Supervisor, P1C-F2 deadline/latch repair, residual-task ownership integration and deterministic/real-server acceptance. These are separate implementation tasks. The existing four untracked P1C files are preserved unchanged; this draft does not resolve their source-review HOLD by assertion.
