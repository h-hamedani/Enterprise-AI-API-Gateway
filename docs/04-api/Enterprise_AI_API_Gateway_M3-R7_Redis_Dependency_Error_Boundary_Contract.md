# M3-R7 Redis Dependency Error Boundary Contract

Status: authoritative prerequisite for M3.8 local degraded protection. This
addendum narrows the historical broad dependency wrapping in the M3.2 rate
limiter, M3.3 concurrency semaphore, and M3.4 circuit store. It changes no
runtime code in this documentation step.

## Fallback eligibility

M3.8 may use local degraded enforcement only when the normal Redis operation
cannot produce a usable shared decision because Redis communication or
availability failed. A historical `RateLimitDependencyError`,
`ConcurrencyDependencyError`, or `CircuitDependencyError` is not sufficient
evidence until its originating primitive has been narrowed under this
contract. Validation, command, response-contract, configuration,
state-safety, and programming failures are **not** fallback eligible.

The M3.8 implementation should keep each existing sanitized dependency
exception exclusively for the eligible availability category and introduce
or preserve distinct non-dependency protocol/internal failures. Callers must
never inspect exception text to choose fallback. Normal successful Redis
decisions, normal rejection/open-circuit results, keys, and Lua algorithms
are unchanged.

## Installed redis-py classification

The repository's installed redis-py version inspected for this addendum is
`8.1.0`. Its exception hierarchy makes broad parent catches unsafe:
`AuthenticationError`, `AuthorizationError`, and
`ExternalAuthProviderError` all inherit `ConnectionError`;
`BusyLoadingError` and `MaxConnectionsError` do too. `NoScriptError` and
`NoPermissionError` inherit `ResponseError`. A bare
`except ConnectionError` would therefore incorrectly classify Redis
credential/ACL failures as availability failures.

For the current Redis client path, the narrow availability whitelist is:

| Exact exception type | Classification |
| --- | --- |
| `redis.exceptions.ConnectionError` | Connection establishment/loss or Redis network I/O failure. |
| `redis.exceptions.TimeoutError` | Redis command/socket timeout. |
| `asyncio.TimeoutError` | Explicit client-side timeout around Redis dependency I/O. |
| `redis.exceptions.BusyLoadingError` | Redis temporarily cannot execute commands while loading. |
| `redis.exceptions.MaxConnectionsError` | No Redis client connection is available to execute the operation. |

The implementation must use exact-type matching or an equivalent explicit
subclass exclusion so the whitelist cannot admit auth/configuration
subclasses of `ConnectionError`. Do not catch all `RedisError`, all
`ResponseError`, or all `ConnectionError` subclasses. Raw `OSError` is not
whitelisted by default; add only a narrowly scoped transport subtype if the
actual Redis command path is shown to leak it. Cluster-only or other
temporarily unavailable named exceptions may be added only after checking
their documented semantics and applicability to the deployed client, with
tests for the exact new class. No generic parent class is a shortcut.

In particular, `AuthenticationError`, `AuthorizationError`,
`ExternalAuthProviderError`, `NoPermissionError`, `DataError`, generic
`ResponseError`, and `ReadOnlyError` are not on this availability whitelist.
Bad credentials, ACL denial, invalid arguments, server command/configuration
faults, and incompatible deployment behavior remain visible failures rather
than indefinite local degraded service.

## Script load, retry, and response contracts

`NoScriptError` alone is not unavailability. Preserve the existing bounded
sequence: on the first `NOSCRIPT`, force `SCRIPT LOAD`, then retry the command
once. No unbounded reload loop is permitted. Availability failure during
reload or retry is fallback eligible; command/configuration/contract failure
is not. The same classification applies to the initial `SCRIPT LOAD`.

An unexpected Lua response shape or value is not Redis unavailability. Wrong
list length or element type, non-integral numeric field, impossible state or
status marker, malformed returned UUID/token, or unsupported result means
protocol/contract mismatch, incompatible deployment, corruption, or a
programming defect. Raise a distinct non-dependency protocol/internal error.
Do not convert `ValueError`, `TypeError`, `KeyError`, `IndexError`, assertion
failures, or local serialization/parsing defects into fallback eligibility.

| Primitive | Fallback-eligible typed result | Never fallback on |
| --- | --- | --- |
| M3.2 rate limiter | Narrow Redis transport/timeout failure → sanitized `RateLimitDependencyError`. | `RateLimitPolicyError`, malformed Lua result, conversion/parse or programming error. |
| M3.3 concurrency semaphore | Narrow Redis transport/timeout failure → sanitized `ConcurrencyDependencyError`. | `ConcurrencyPolicyError`, unexpected Lua result, parse or programming error. |
| M3.4 circuit store | Narrow Redis transport/timeout failure → sanitized `CircuitDependencyError`. | `CircuitContractError`, `CircuitStateError`, malformed row/token/state, or programming error. |

`CircuitStateError`, including generation or safe-integer overflow and unsafe
transition, must not be caught again and reclassified as Redis outage. Policy
and identity validation must occur before or independently of fallback; M3.8
must not create local state from invalid input.

`asyncio.CancelledError` always propagates as cancellation. It is neither a
dependency nor an internal error wrapper and never activates degraded mode.

## Operational boundary

A genuine Redis command/transport timeout is eligible even if Redis may have
mutated shared state before the client timed out. Local fallback then
prioritizes conservative protection without claiming exact Redis/local
counter equivalence or merging the two states. M3.9 owns later recovery and
cutover.

Upward-facing dependency errors and telemetry remain sanitized: no Redis URL,
host/port, credentials, command body, keys, Lua arguments, or raw exception
text is exposed to clients or unsafe logs. An original exception may remain
an internal cause only when safe. RedisRuntime's health probe may classify
availability more broadly for diagnostics; its result does not authorize a
traffic request to fall back. The traffic primitive's own classified failure
is authoritative.

## Required tests before M3.8 fallback integration

Rate limiter tests must prove: connection and command timeout are eligible;
first `NOSCRIPT` reloads and retries once; transport failure during that
reload/retry is eligible; malformed result, numeric conversion error,
`RateLimitPolicyError`, and cancellation are not.

Concurrency tests must prove: connection and timeout failures are eligible;
malformed result and `ConcurrencyPolicyError` are not; cancellation
propagates.

Circuit tests must prove: connection and timeout failures are eligible;
malformed row, `CircuitStateError`, and `CircuitContractError` are not;
cancellation propagates.

Across primitives, tests must prove Redis authentication/ACL failures and
generic `ResponseError` do not activate degraded fallback. Cover the exact
installed-version whitelist, including the auth subclasses of
`ConnectionError`, and preserve existing successful Redis behavior and
normal rejection results. M3.8 wrappers may catch only the corrected
fallback-eligible typed dependency errors; they must never use broad
`except Exception` or `except RedisError` to select local protection.

This contract introduces no database change, Alembic migration, persisted
failure classification, or M3.8 implementation.
