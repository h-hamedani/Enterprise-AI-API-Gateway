# M4-P1C-F4B — Lifecycle Integration Acceptance Evidence

Implementation evidence; **independent source acceptance remains required**. This is not a frozen contract, platform shutdown certificate, guarded TLS acceptance or overall M4 release approval.

Baseline: `feature/m3-runtime`, local/remote `c59e7cab10c428eb41b67ed4083f0e7c57a795fa`, plus the preserved accepted F4A/F7 working tree. The pre-resumption manifest contains 33 paths (10 tracked modifications, 23 untracked files), including eight access logs. Existing F4A transport/pool/quarantine, facade, runtime, supervisor and F7 tests are protected dependencies. No contract, migration, public route, upstream executor or streaming transport changes are part of this task.


## F4B-F2 correction — cancellation phase after compaction

F1 independent source review found a K11 gap: the test retained the original operation while asserting its phase, but detached `RenewalEvidence` omitted that field. After collection, drain and response cancellation became indistinguishable. This section records implementation evidence only; independent source acceptance remains required.

### Failing before / passing after

The existing `test_k11_lifecycle_cancellation_preserves_actual_operation_history` now releases its original record reference, acknowledges scheduled callbacks, calls GC and verifies both record and invocation Task weak references are dead **before** inspecting detached evidence. The actual RequestLifecycle → recovery facade → semaphore → guarded transport path is exercised with controlled RESP connections and Event gates, not real sockets.

Command: `.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k11_lifecycle_cancellation_preserves_actual_operation_history --tb=short --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4bf2-red`.

Before production edits: **4 failed**, each with `AttributeError: 'RenewalEvidence' object has no attribute 'phase'`, after GC assertions passed. Variants: preparation, drain, response, and NOSCRIPT reload (canonical `preparation`). Same selection after repair (`gateway-f4bf2-green`): **4 passed**.

### Minimal evidence extension

- Detached immutable `RenewalEvidence.phase` captures only the existing canonical values `preparation`, `write`, `drain`, `response`, `local`; absent or unsupported source values become `None`, never an invented phase or raw text.
- Immutable `RenewalEvidenceSummary.cancellation_phase_counts` has six fixed entries: those five production values plus `None` for no established phase. It counts completed cancelled invocations, not ordinary UNKNOWN outcomes, successful operations or cancellation requests that have not terminated execution.
- Phase capture and counter publication occur in the existing synchronous completion-accounting segment before Task retirement. Existing per-Task idempotence prevents duplicate aggregation. No await, new Task, version authority, cleanup operation or deadline is added.
- Recent detail remains bounded to two entries. Phase counts preserve historical distinctions after detail eviction; they do not reclassify uncertain sends or manufacture backend results. Old snapshots remain unchanged. All evidence is detached: no Task, mutable operation, connection, raw handle, exception or secret reference is introduced.

### Executable mapping

| Requirement | Regression |
| --- | --- |
| K11 actual phases, pre-send and post-send cancellation, original record/Task GC, exact attempts, unchanged original-handle release | `test_k11_lifecycle_cancellation_preserves_actual_operation_history` (four variants) |
| Actual drain and response snapshots remain unequal after compaction | `test_f2_actual_drain_and_response_snapshots_are_distinguishable` |
| Mixed/repeated phase accounting, immutable old publications, eviction, later UNKNOWN/success/SUPPRESSED, idempotent callbacks, GC | `test_f2_phase_counters_survive_eviction_success_suppression_and_gc` (accounting-only controlled operation records, not a claim that a terminal request resumes execution) |
| Absent/unsupported phase is not fabricated or leaked | `test_f2_missing_or_unsupported_phase_never_fabricates_a_production_phase` |
| Cancellation before any operation exists counts absence once | `test_f2_cancellation_without_an_operation_counts_absence_once` |
| K12 1/8/64/256 cycles, two-detail bound, zero retained completed Tasks, fixed six-entry counter schema | `test_f1_repeated_cycles_retire_completed_tasks_and_bound_history` |
| K17 live/cancellation-pending Task and physical ownership unchanged | `test_f1_live_cancelled_work_and_physical_obligations_are_independent`, existing P1C supervisor tests, real L05/L06 |

The mixed-history accounting test completes preparation/drain/response/drain/response cancellations, then three UNKNOWN outcomes, a successful result and SUPPRESSED. Both recent details are then non-cancellation records, while phase counts remain exactly preparation=1, drain=2, response=2 and all other entries=0. Earlier uncertainty remains unresolved. All original record/Task weak references are collectable; repeated callbacks leave counters unchanged.

### Real Redis boundary

L04's repeated uncertainty/recovery variant does **not** exercise cancellation, so no cancellation phase is invented there. Existing L05 request/process cutoff variants do cancel an actual response-wait operation on a real socket. Both now assert detached `phase == "response"`, cancellation and exactly one response-phase count, alongside unchanged quarantine, original confirmation, residual ownership and no-release-after-cutoff assertions. No new normative L-case or fixture/backend behavior is introduced. Disposable Redis remains 7.4.11 with redis-py 8.1.0. Fixture-owned physical cleanup is separate from product cleanup.

### Operational debt and scope

Historical-only uncertainty can retain an entire residual scope even after all Tasks and physical resources finish. Cross-request residual accumulation remains existing **non-blocking operational debt**; F2 does not discard uncertainty, certify cleanup, change supervisor retention or introduce a TTL/archive/global cleanup policy. R1/R2 ownership, R8 cadence/anchors, R9 terminal precedence, accepted F4A/F7 and frozen contracts are unchanged. No P1D, public routing, migration, OpenAPI or deployment acceptance is included.

### Verification

- K11 failing-before: **4 failed**, then **4 passed** after the repair.
- F4B unit suite: **60 passed in 3.73s**.
- Focused F4B/F4A/F7/P1C/M3 selection: **511 passed in 15.39s**.
- Disposable Redis L01–L06 and accepted F4A/F7: **52 passed in 23.53s**, including real L05 phase assertions. Container teardown verified; no owned disposable containers remain.
- Default opt-in-disabled disposable check: **52 skipped in 2.73s**.
- Broad safe selection: **1,246 passed, 8 skipped, 3 warnings in 75.03s**. Shared migration round trips (`test_migrations.py`), destructive shared rate-limit Redis tests (`test_rate_limit_runtime.py`) and separately executed disposable suites were excluded. Eight skips remain explicit opt-in outage/chaos, multiprocess and platform-server cases, not reported acceptance passes.
- Ruff: PASS. Format: **21 changed Python files PASS**. Compileall with external `PYTHONPYCACHEPREFIX`: PASS. Strict UTF-8/no BOM/no NUL: **27 source/document/config files PASS**. Tracked and untracked-source whitespace: PASS; existing `.env.example` line-ending advisory only.
- Index remains empty. Only lifecycle, F4B unit/integration tests and this evidence document change; all 31 other working paths and eight access logs are byte-identical to the F2 baseline. No frozen contract, supervisor, backend, runtime, facade, launcher or unrelated test changes.

Commands use `.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider`, external `--basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4bf2-{red,green,unit,focused,redis,optout,broad}` and the same focused/broad file selections recorded below for F4B/F1. Redis flags are `RUN_M4_F4B_REDIS=1` and `RUN_M4_F4A_REDIS=1` only for the disposable run. Compilation uses `PYTHONPYCACHEPREFIX=C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4bf2-pycache`. Existing opt-in outage/platform/multiprocess gates remain separate. No staging, commit, push or independent source acceptance is claimed.

## F4B-F1 correction — bounded historical uncertainty

F1 starting inventory: 35 working paths (10 tracked modifications and 25 untracked files), empty index. SHA-256 comparison preserves all 31 non-F1 paths, including eight access logs; only lifecycle, its F4B unit/integration tests and this evidence document change.

The preceding F4B K12 claim was incomplete: independent review reproduced eight completed uncertain Tasks retained after eight UNKNOWN/success cycles. The earlier one-cycle tests proved preservation, not bounded retention. This section supersedes that boundedness claim; it does not claim independent source acceptance.

Failing-before command: `.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider "tests/unit/test_m4_p1c_f4b_lifecycle.py::test_f1_repeated_cycles_retire_completed_tasks_and_bound_history[8]" --tb=short --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4bf1-red2`.
Observed: **1 failed**, retained completed Tasks `[1,2,3,4,5,6,7,8]`.

### Representation and ownership

`RenewalEvidenceSummary` is an immutable, fixed-field aggregate with exact scalar invocation, uncertainty, UNKNOWN, cancellation, exception, disposition, failure-category and attempt-status counters; dispatch timing bounds; and original issuing backend/generation. Recent immutable invocation detail has an explicit representation bound of **two**, not a renewal, traffic, retry or lifetime limit. No summary links to a prior summary, Task, operation, connection, raw handle or exception. Previously obtained snapshots remain unchanged.

`_renewal_obligations` now owns only live invocation Tasks, including cancellation-suppressing work. Completed results/exceptions are observed, history captured once, and real deferred-resource records transferred independently before Task references are removed from lifecycle and scope role maps. The idempotence marker lives on the completed Task, not in a growing seen-set. Worker-local completed Task references are cleared. No weak-reference ownership replaces strong live ownership; weak references appear only in GC assertions.

`_renewal_resources` retains actual operation/resource obligations until the existing backend confirms closure. Execution completion is not closure proof. A detached historical snapshot's `cleanup_pending` is a fact at capture time, not a current physical-state query. Current resource counts are sampled separately. Compaction performs no cleanup I/O and cannot initiate work after cutoff.

Earlier unresolved attempt uncertainty remains true after retry, original-handle release and runner exit. Supervisor residual scope ownership and `cleanup_complete=False` are preserved even with zero live Tasks and zero physical obligations. UNKNOWN without an uncertain mutating attempt is counted without fabricating an uncertain send.

### K12 / deterministic mapping

| Evidence | Executable regression |
| --- | --- |
| Actual lifecycle → facade → guarded transport, 1/8/64/256 UNKNOWN/success cycles, active downstream delivery, immutable snapshots, anchors, exact release, Task GC | `test_f1_repeated_cycles_retire_completed_tasks_and_bound_history` |
| Strong live/cancellation-pending ownership, independent deferred connection confirmation, repeated callbacks, immutable physical-state snapshots | `test_f1_live_cancelled_work_and_physical_obligations_are_independent` |
| Observed exception, idempotent counters, no Task archive or raw exception evidence | `test_f1_exception_is_observed_once_without_archiving_task` and `test_f1_completed_record_truth_survives_exception_or_cancellation` |
| Earlier uncertainty followed by success and runner exit | `test_k12_uncertain_attempt_survives_later_success_and_runner_exit` |
| Earlier uncertainty followed by SUPPRESSED/GUARD_ERROR/INTERNAL_ERROR | `test_k12_prior_uncertainty_survives_later_denial_or_fault` |
| Cancellation/terminal boundaries and immutable attempts | K09/K11 and existing P1C shutdown/supervisor regressions |
| No new cleanup after cutoff, writer generations, truthful residuals | K16/K19, real L05/L06 and accepted F4A/F7 regressions |
| Bounded redacted diagnostics, logger failures and duplicate callbacks | K21 regressions |

| Cycles | Completed invocations summarized | Historical uncertain invocations | Retained completed invocation Tasks | Live Tasks / physical obligations | Recent details |
| --- | --- | --- | --- | --- | --- |
| 1 | 2 | 1 | 0 | 0 / 0 | 1 |
| 8 | 16 | 8 | 0 | 0 / 0 | 2 |
| 64 | 128 | 64 | 0 | 0 / 0 | 2 |
| 256 | 512 | 256 | 0 | 0 / 0 | 2 |

Every cycle samples current summary state and verifies the first immutable snapshot remains unchanged. Weak-reference/GC checks prove all completed invocation Tasks are collectable while downstream delivery remains active. This demonstrates bounded historical object retention, not a claim that genuine unresolved physical/live obligations consume constant memory.

### L04 real socket extension

`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l04_repeated_uncertain_sends_compact_completed_tasks` extends existing L04, not the frozen L-case list. It uses disposable Redis **7.4.11**, redis-py **8.1.0**, the production lifecycle/facade/guarded transport, and actual TCP transport abort immediately after each first mutating write. Eight UNKNOWN/success cycles produce 16 observed mutating sends, eight retained uncertainty facts, zero retained completed Tasks, at most two recent details, unchanged first snapshot, continued downstream delivery and one original-Redis-handle release. Whether each aborted first command executed is deliberately left unknown. Clock scheduling and downstream hooks are controlled; Redis I/O is real. No shared Gateway Redis or destructive shared FLUSHDB is used.

### F1 verification results

- Focused F4B/F4A/F7/P1C/M3 selection: **503 passed**.
- Disposable Redis L01–L06 plus F4A/F7: **52 passed** (16 F4B variants, 36 backend variants).
- Default opt-in-disabled check: **52 skipped**.
- Two final deterministic probes additionally verify completed record truth after exception/cancellation (`test_f1_completed_record_truth_survives_exception_or_cancellation`). They pass in the final broad safe suite. Automatic approval review timed out twice before a final rerun could launch. After explicit owner authorization, the final combined source run passed **106 tests in 24.33s**: 54 F4B unit tests plus all 52 disposable-Redis/F4A/F7 tests. No verification blocker remains.
- Final broad safe selection: **1,240 passed, 8 skipped, 3 warnings** in 73.89s. Shared migration round trips and destructive shared rate-limit Redis tests were excluded.
- Ruff: PASS. Format: **21 changed source/test files PASS**. Compileall: PASS with external `PYTHONPYCACHEPREFIX`. Strict UTF-8/no BOM/no NUL: **27 source/document/config files PASS**. Tracked and untracked-source whitespace: PASS. Index remains empty.
- Existing multiprocess tests were not repeated: F1 does not change the multiprocess worker, configuration/protection algorithms or process coordination; prior two-test evidence is not represented as a new F1 run.

Limitations: no platform shutdown, guarded TLS, public proxy, real upstream streaming, P1D or overall M4 deployment acceptance is claimed. Independent review remains required. No frozen contract, F4A/F7 backend, supervisor, facade/runtime, Lua, lease authority, cutoff, G, retry/fallback policy or public API is changed by F1.

## Implementation reconciliation

The partial lifecycle already provided the live guard, guarded invocation, producing timestamp, active downstream lifetime, suppression interpretation, immutable evidence and residual accounting. The original K16 remains unchanged. This resumption adds permanent context-isolation regressions and integrated acceptance variants. The only new production behavior is R8-required finite renewal-condition diagnostics: loss, first UNKNOWN, recovery, confidence expiry and internal worker fault. They use canonical request-ID correlation, exclude resource/secret/exception fields, cannot alter lifecycle truth on logger failure, and are separate from the future R26 primary outcome logger. No metric infrastructure or new audit action is added.

## K01–K21 exact frozen requirement mapping

The following scenario/evidence wording is copied from frozen Section K. Test names map to actual coordinator execution; backend-only regressions are supplementary, not substituted for lifecycle assertions. Test-owned RESP connections and gates provide deterministic transport evidence; Section L separately provides real sockets. All grouped variants below are required; none is intentionally omitted.

| ID | Frozen scenario | Frozen required evidence | Executable tests |
| --- | --- | --- | --- |
| K01 | Real acquire versus denied/null acquire | Worker starts before body only for real handle; no null release | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k01_worker_is_supervised_before_body_only_for_real_acquisition` |
| K02 | Cadence at minimum/maximum supported TTL | T/3, T/2 and 2T/3 preserved; no rounding grace or overlapping operations | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k02_native_fractional_unknown_schedule_at_both_ttl_bounds`<br>`tests/unit/test_m4_p1c_lifecycle.py::test_exact_renewal_schedule_and_loss_cancels_work` |
| K03 | Multiple successful renewals / short request | Producing anchors advance correctly; short request performs no unnecessary renewal and one release | `tests/unit/test_m4_p1c_lifecycle.py::test_multiple_renewals_continue_during_downstream_delivery`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k01_worker_is_supervised_before_body_only_for_real_acquisition`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l01_real_lifecycle_multiple_renewals_and_exact_original_release` |
| K04 | Task/script/pool/auth/health/reconnect delay | Actual integrated final guard denies at/after horizon; zero denied renewal writes; preparation traffic identified separately | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k04_integrated_preparation_delay_reaches_live_writer_guard`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l02_real_initialization_delay_denies_late_lifecycle_dispatch` |
| K05 | Timely NOSCRIPT retry / denied retry | Fresh guard and retry timestamp; exact bounded histories, no fabricated retry | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k05_producing_retry_timestamp_and_immutable_snapshot`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l03_real_noscript_retry_uses_producing_timestamp_or_is_denied` |
| K06 | Timely send, late success / exact equality | No anchor advancement or execution revival | `tests/unit/test_m4_p1c_lifecycle.py::test_delayed_true_renewal_cannot_extend_old_safety_horizon`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k04_k13_delayed_dispatch_guard_after_upstream_outcome` |
| K07 | False / unknown provenance / missing generation | Existing definitive-loss handling and correct zero-call cases | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k07_provenance_and_backend_false_through_real_facade` |
| K08 | UNKNOWN then true, repeated UNKNOWN or hanging call | One bounded business retry; existing horizon terminalizes; no provenance substitution | `tests/unit/test_m4_p1c_lifecycle.py::test_unknown_one_retry_and_original_safety_horizon`<br>`tests/unit/test_m4_p1c_lifecycle.py::test_hanging_renewal_cancelled_and_awaited_before_release`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k02_native_fractional_unknown_schedule_at_both_ttl_bounds` |
| K09 | Suppression / unexplained suppression | Existing boundary enforced or internal fault; no false loss, extra retry or anchor | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k09_expected_suppression_preserves_owner_boundary`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k09_unexplained_suppression_is_internal_not_backend_loss` |
| K10 | Guard/internal fault, ACL/protocol error | Correct internal versus dependency distinction and immutable evidence | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k10_actual_backend_error_classification_and_evidence`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k10_guarded_internal_dispositions_do_not_become_loss_or_unknown` |
| K11 | Cancellation before send, after NOSCRIPT, during drain/reply | Propagated cancellation; exact phase/history and retained operation ownership | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k11_lifecycle_cancellation_preserves_actual_operation_history` |
| K12 | Prior uncertain send followed by denial/success/fault | Earlier uncertainty survives; bounded summaries do not fabricate resolution | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k12_uncertain_attempt_survives_later_success_and_runner_exit`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k12_prior_uncertainty_survives_later_denial_or_fault` |
| K13 | Definitive upstream outcome with active downstream delivery | Renewal continues; later loss stops delivery without rewriting circuit classification | `tests/unit/test_m4_p1c_lifecycle.py::test_multiple_renewals_continue_during_downstream_delivery`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k04_k13_delayed_dispatch_guard_after_upstream_outcome`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l01_real_lifecycle_multiple_renewals_and_exact_original_release` |
| K14 | Terminal decision races and late callbacks | First winner, at-most-once circuit completion/release; no fabricated response | `tests/unit/test_m4_p1c_lifecycle.py::test_first_gate_wins_without_rewriting_circuit`<br>`tests/unit/test_m4_p1c_lifecycle.py::test_duplicate_outcome_and_delivery_callbacks_do_not_duplicate_cleanup` |
| K15 | Stop/release ordering with stalled worker/finalizer | No release racing renewal; eligible release not held by independent executor finalizer | `tests/unit/test_m4_p1c_lifecycle.py::test_hanging_renewal_cancelled_and_awaited_before_release`<br>`tests/unit/test_m4_p1c_lifecycle.py::test_f1_stalled_executor_finally_does_not_block_release_or_server_cutoff` |
| K16 | Request/process deadlines in both orders; exact cutoff | Earliest deadline, latched expiry, no new cleanup after cutoff or additional grace | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k16_actual_guarded_transport_does_not_start_disconnect_after_cutoff`<br>`tests/unit/test_m4_p1c_f2_supervisor.py::test_f2_earliest_deadline_and_notifications_do_not_extend`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l05_real_pending_response_at_effective_cutoff_stays_owned` |
| K17 | Cutoff with renewal/release/circuit completion pending | Strong supervisor retention and truthful incomplete/not-attempted/unconfirmed accounting | `tests/unit/test_m4_p1c_f2_supervisor.py::test_f2_absolute_cutoff_retains_dependency_without_overlapping_release`<br>`tests/unit/test_m4_p1c_f3_repairs.py::test_f3_unresolved_circuit_attempt_is_not_complete_or_forgotten`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l05_real_pending_response_at_effective_cutoff_stays_owned` |
| K18 | Local lock delay and recovery cutover | No denied mutation; original generation retained; Redis handle never becomes local | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k18_lifecycle_uses_retired_local_generation_after_cutover`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k18_redis_issued_lifecycle_never_substitutes_active_local_backend` |
| K19 | F6 no-op, multiple writer/protocol generations, late confirmation | Existing closure guarantees and runtime/supervisor residual accounting remain passing | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k16_actual_guarded_transport_does_not_start_disconnect_after_cutoff`<br>`tests/integration/test_m4_p1c_f4b_acceptance.py::test_l06_lifecycle_and_both_real_pool_confirmation_failures_remain_unclosed` |
| K20 | Cancellation before runner starts / shutdown admission race | Existing registration ownership and no unsupervised managed work preserved | `tests/unit/test_m4_p1c_f2_supervisor.py::test_f2_cancel_before_runner_first_step_has_registered_owner`<br>`tests/unit/test_m4_p1c_f2_supervisor.py::test_f2_registration_shutdown_linearization` |
| K21 | Redaction and repeated lifecycle entry | No raw handles, credentials, Redis keys or exception text; no duplicate actions | `tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k21_r8_diagnostics_are_bounded_correlated_and_not_duplicate`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k21_fault_redaction_and_repeated_terminal_callbacks`<br>`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_k21_diagnostic_failure_does_not_change_cleanup_or_selected_result` |

K04 has six deterministic preparation variants (task, script, pool, auth, health and reconnect) through the real facade/semaphore/guarded writer using controlled RESP fixtures. L02 adds seven real-Redis preparation variants. Some real preparation variants are cancelled before reaching the writer; they prove zero late sends, not a fabricated guard invocation. The deterministic non-cooperative preparation variants deliberately resume after terminalization and assert actual final-guard denial.

K05/L03 distinguish a timely retry's actual producing timestamp from the initial NOSCRIPT timestamp. In the late case, cancellation may prevent a normal outcome; retained NOSCRIPT history and zero retry sends remain the authoritative evidence. K06 retains the old horizon on late confirmation. K18 injects cutover membership in the fixture while exercising production facade/local-store routing, rather than claiming this certifies the M3 recovery barrier. K19 additionally reruns all accepted F6/F7 backend regressions for writer-generation details.

## Permanent context-isolation regression

`tests/unit/test_m4_p1c_f4b_lifecycle.py::test_context_isolation_concurrency_nesting_child_lifetime_and_cancellation` persists concurrent distinct deadlines, nested restoration, cancellation, child inheritance/continuation after parent exit and subsequent unmanaged context. The unchanged F7 `test_f7_deferred_scope_cannot_be_replaced_to_retry_cleanup` proves first-deferred connection authority under later scopes.

## L01–L06 real-Redis lifecycle acceptance

All cases use the approved uniquely named/labeled, identity-checked disposable fixture; Redis 7.4.11 and redis-py 8.1.0 are asserted. No shared FLUSHDB, Gateway restart or shared volume operation is used. The primary actions flow through RequestLifecycle → RecoveryConcurrencySemaphore → accepted guarded backend. Resolver/upstream/delivery hooks remain synthetic and are not production HTTP forwarding.

| ID | Test in `tests/integration/test_m4_p1c_f4b_acceptance.py` | Real I/O evidence | Clock/gate injection |
| --- | --- | --- | --- |
| L01 | `test_l01_real_lifecycle_multiple_renewals_and_exact_original_release` | Real acquire, three renewals, competing rejection beyond the original Redis TIME expiry, exact original release and subsequent admission | Real monotonic clock; observed scheduled-deadline acknowledgements. The original expiry is measured from Redis and rechecked against Redis TIME, with a derived deadline timer rather than arbitrary sleep |
| L02 | `test_l02_real_initialization_delay_denies_late_lifecycle_dispatch` (7 variants) | Real connection preparation with zero late renewal-command writes and unchanged anchor; original release | Controlled clock/gates for initialization, AUTH/HELLO reply, health PING, reconnect, checkout, script preparation and task dispatch |
| L03 | `test_l03_real_noscript_retry_uses_producing_timestamp_or_is_denied` (2 variants) | Disposable-server SCRIPT FLUSH, actual NOSCRIPT, timely retry result with its timestamp, or no late retry write with retained N | Controlled reload gate and clock; cancellation of deliberately delayed preparation is distinguished from a returned suppression outcome |
| L04 | `test_l04_real_uncertain_send_retained_across_one_bounded_retry` | Actual writer send followed by socket abort; U retained through one R8 retry and later true result; no falsely completed cleanup | Clock selects T/2 retry; injected socket abort creates real transport uncertainty without asserting whether the first command executed |
| L05 | `test_l05_real_pending_response_at_effective_cutoff_stays_owned` (2 variants) | Actual open socket and sent renewal; no post-cutoff disconnect, quarantine, retained original future/scope, no release attempt | Pending-response gate; existing request or process cutoff selected by controlled clock |
| L06 | `test_l06_lifecycle_and_both_real_pool_confirmation_failures_remain_unclosed` (2 variants) | Lifecycle renewal/release, real writer-close failure, no-op disconnect cannot erase confirmation, supervised runtime remains unclosed for guarded and legacy pools | Instance-local close failure on actual writer; existing finite socket wait bound. No synthetic success is manufactured |

Teardown unblocks and joins test tasks, then performs explicit fixture-owned physical socket cleanup and verifies disposable-container removal via the accepted identity-checking helper. Test-owned disposal is never represented as production retry or post-cutoff runtime recovery. Expected terminal runtime-close failures remain failures even after fixture cleanup.

## Verification record

Final command outcomes are recorded after execution. Destructive shared-service migration/FLUSHDB tests and unauthorized outages are excluded. Opt-in safety is checked separately. Independent review must inspect these sources and assertions; counts alone do not establish source acceptance.

## Final verification outcomes

- Focused F4B/F4A/F7/M3/P1C selection: **497 passed in 13.12s**.
- Separate F4B disposable Redis acceptance: **15 passed** across all six L cases (1 + 7 + 2 + 1 + 2 + 2 variants).
- Combined final F4B and accepted F4A/F7 disposable Redis suites: **51 passed in 21.23s**.
- Default opt-in-disabled check for both disposable suites: **51 skipped in 2.54s**, with no fixture allocation.
- Broad safe selection: **1232 passed, 8 skipped, 3 deprecation warnings in 69.06s**. Shared migration round trips, shared FLUSHDB tests and the separately executed disposable suites were excluded. Unauthorized outage flags remained disabled.
- Isolated opt-in multiprocess regressions: **2 passed, 1 deprecation warning in 44.63s**.
- Ruff, formatting, compileall with external bytecode storage, strict UTF-8/no BOM/no NUL and tracked/untracked whitespace: **PASS**. The index remains empty; no staging, commit or push occurred.
- All 30 protected baseline paths are byte-identical. Only lifecycle production source, F4B unit tests and lifecycle notes changed; the F4B real acceptance file and this evidence record are new. The entire original F4B unit-test prefix, including K16, matches its pre-resumption SHA-256 after accounting for its original final newline.
- No acceptance variants are pending within K01–K21 or L01–L06. Independent F4B source acceptance is still pending. Platform shutdown, guarded TLS and overall M4 acceptance remain separate.

### Commands

PowerShell, repository root; every pytest invocation used `-B -p no:cacheprovider` and an isolated external `--basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4b-...` directory.

```powershell
.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests/unit/test_m4_p1c_f4b_lifecycle.py tests/unit/test_m4_p1c_lifecycle.py tests/unit/test_m4_p1c_f2_supervisor.py tests/unit/test_m4_p1c_f3_repairs.py tests/unit/test_guarded_renewal.py tests/integration/test_m4_p1c_f2_lifespan.py tests/unit/test_concurrency.py tests/unit/test_redis_runtime.py tests/unit/test_m38_redis_error_boundary.py tests/unit/test_m38_local_degraded.py tests/unit/test_m39_recovery.py tests/unit/test_m38_control_plane_degraded.py --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4b-focused-full
$env:RUN_M4_F4B_REDIS='1'
$env:RUN_M4_F4A_REDIS='1'
.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests/integration/test_m4_p1c_f4b_acceptance.py tests/integration/test_guarded_renewal_runtime.py --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4b-real-final
$env:RUN_M4_F4B_REDIS=$null
$env:RUN_M4_F4A_REDIS=$null
.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests/integration/test_m4_p1c_f4b_acceptance.py tests/integration/test_guarded_renewal_runtime.py --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4b-optout
.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests --ignore=tests/integration/test_migrations.py --ignore=tests/integration/test_rate_limit_runtime.py --ignore=tests/integration/test_guarded_renewal_runtime.py --ignore=tests/integration/test_m4_p1c_f4b_acceptance.py --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4b-broad-final
$env:M311_MULTIPROCESS_ACCEPTANCE='1'
.venv/Scripts/python.exe -B -m pytest -q -p no:cacheprovider tests/integration/test_m311_multiprocess_acceptance.py --basetemp C:/Users/ha_hamedani/AppData/Local/Temp/gateway-f4b-multiprocess
```

## Pre-resumption SHA-256 manifest

This is the recorded starting working-tree manifest, not the frozen commit's tree. Authorized resumption edits are compared against it; all other paths, including accepted backend code and eight logs, must match.

| Path | SHA-256 before resumption |
| --- | --- |
| `access_11_20.err.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `access_11_20.out.log` | `0cdbaae99d491e668b89017b3ff2b6bf8a05a627ce328347d4ca4b067e58325e` |
| `access_21_30.err.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `access_21_30.out.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `access_2_10.err.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `access_2_10.out.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `access_31_39.err.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `access_31_39.out.log` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `app/normal_api/lifecycle.py` | `8cc9ff908530632b0271c2b277635eb9b664dd6f526fb89123595aac88920a6d` |
| `app/normal_api/supervisor.py` | `b93e232f7ccc757eade945b86c3460f970b87ce7ffe8c949207e6e0f695032a4` |
| `app/redis/guarded_renewal.py` | `9d800d74b340e8e511d6f62740436ac369f7ebd37ef045accf839c5458acb8de` |
| `app/server.py` | `c6ca9e7eb938b17b0f8508b09205b78ea4a7af576bad15d14f328c404263109a` |
| `docs/05-implementation/Enterprise_AI_API_Gateway_M4_P1C_F4A_Guarded_Renewal_Notes.md` | `4bb557d5821677cbcb169140450dbe2c471ba78f468256ef5d4a4eb1a91f58bb` |
| `docs/05-implementation/Enterprise_AI_API_Gateway_M4_P1C_Lifecycle_Notes.md` | `c5f158c317a190b2a110fe58107256ad74a23ff8c0ee6fa1308860bb356dcc10` |
| `tests/integration/test_guarded_renewal_runtime.py` | `27dc56f78502bde6dc09d4a1cd693b3a96149d35422a756bb366a9eac7adb9eb` |
| `tests/integration/test_m4_p1c_f2_lifespan.py` | `6448e207df117d10e5c878697eb0d5bd925aefd2cbe1c6d146293c3a5c4ac2ea` |
| `tests/integration/test_m4_p1c_f3_server_process.py` | `6562907fac788583db46ed8aae9cb3c160ebdb925ab0660b143c70e1335ab6ef` |
| `tests/integration/test_m4_p1c_lifecycle.py` | `c2b5a36518828c6e23804cab018827d46cc4a759413a79bbe570a6c9fcae83dd` |
| `tests/unit/test_guarded_renewal.py` | `d31d19692209fb4b93bba667fe3c1a6570feb2963e425effc224e936fc052e57` |
| `tests/unit/test_m4_p1c_f2_supervisor.py` | `9ef413fc9255556c3f3938d9c865db6432cc8f14d9c6de8c811585508f320459` |
| `tests/unit/test_m4_p1c_f3_repairs.py` | `77d43e63784c493174cf9e79046ee66ed0ae70f2b9fabccf48fbaf55be5f0f1a` |
| `tests/unit/test_m4_p1c_f4b_lifecycle.py` | `0be7b7edd933e3c85b8cb15df69c69749ec9ce211fd65cb1eec08263dd9f30b5` |
| `tests/unit/test_m4_p1c_lifecycle.py` | `87efb9619bfebcd0e1ceb3cba06ce7295417b92e43c6449f0afb73f142e18d0a` |
| `.env.example` | `f67888c9cc1abe7b3b756b7d767551898ebed12c1a47f002abeaa9b0b237042f` |
| `app/core/config.py` | `2cb565ba2d05a933decab9586add1ce50d866a27ec596dc50acd79f296bcfb33` |
| `app/main.py` | `581006309ba2428824c924153337cf64bb2cbfb6619bb2b8cc9c774df43bf143` |
| `app/redis/concurrency.py` | `0fd980e698317592511cbf1be0a815f2d138a24d6d2f3aaf87fff85ad2c8780f` |
| `app/redis/local_degraded.py` | `3bf232861ae4443fe21bf0e11903cf5bd236dffd62d7fce1e17568292ea43ac2` |
| `app/redis/protection_facade.py` | `3f66b91044543808ca7b186165eef1c9921b0627fcc061011f7608578277d6bb` |
| `app/redis/runtime.py` | `a1a2c19b18fd731850f604c84afbfb31ff2c81f61d37758a26acf99977e0fbc8` |
| `scripts/run-dev.ps1` | `7c0370355dd9781d2260225a31df33dd39b9fe6a9fe0a66f13e9c1e5be790c8e` |
| `scripts/run-dev.sh` | `342fc171b9745fc5a4a1c6448695643f01e39ba0996ac387962cff722bbbf80d` |
| `tests/unit/test_m38_control_plane_degraded.py` | `91ffbc30b9cf93f4ba18b552ff22c66ceb0a86fd3c7a1db6a87d440169d6793a` |
