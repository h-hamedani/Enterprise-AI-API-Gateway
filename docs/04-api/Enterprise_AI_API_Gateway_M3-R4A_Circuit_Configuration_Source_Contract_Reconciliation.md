# M3-R4A Circuit Configuration Source Contract Reconciliation

Status: authoritative addendum for M3 circuit configuration source and scope.
It resolves the HLD configurability conflict without implementing M3.4.

## Conflict and source precedence

HLD V1.1 Corrected marks failure threshold, observation window, OPEN cooldown,
HALF_OPEN probe count, and successes-to-close as `Configurable: Yes`. The M3
Execution Pack supplies V1 defaults and requires one distributed probe and one
successful probe to close. Neither the physical database nor the frozen Control
Plane defines persisted per-resource circuit settings. These values therefore
must not be hard-coded immutable algorithm constants, and no persisted source
may be invented.

For V1, validated deployment runtime settings are the circuit parameter
authority. They are deployment-wide infrastructure configuration, not
PostgreSQL business configuration, Redis configuration truth, a request
parameter, or tenant/route/service/provider-target/model override. All gateway
instances participating in one deployment must use identical values. This is
an operational deployment prerequisite; Redis does not synchronize settings.

## Defaults and validation

| Runtime setting | V1 default | Supported validation |
| --- | ---: | --- |
| `circuit_failure_threshold` | `5` | strict integer >= 1 |
| `circuit_failure_window_ms` | `60000` | strict integer > 0 |
| `circuit_open_duration_ms` | `30000` | strict integer > 0 |
| `circuit_half_open_probe_limit` | `1` | strict integer equal to 1 in V1 |
| `circuit_successes_to_close` | `1` | strict integer equal to 1 in V1 |
| `circuit_probe_lease_duration_ms` | `30000` | strict integer > 0 |

Booleans, fractional values, and string-to-integer coercion are invalid.
No arbitrary upper bound is invented for the positive integer settings;
implementation must additionally validate values against safe Redis integer
millisecond arithmetic before serving traffic.

The HLD values are configurable defaults, not immutable constants. HLD's
`Configurable: Yes` means validated deployment configuration for V1, not a
dynamic per-tenant Control Plane setting. The M3 single-probe invariant means
configured values other than 1 for `circuit_half_open_probe_limit` fail startup.
The frozen one-success transition likewise restricts
`circuit_successes_to_close` to 1. This retains explicit configuration names
without claiming unsupported multi-probe or multi-success V1 behavior.

The probe lease setting is distributed ownership infrastructure, separate from
provider, route, and request timeouts. Its default is 30000 ms; it does not
define a maximum execution lifetime.

## Startup, reload, and existing Redis state

All values are validated at application startup before serving traffic.
Invalid explicit configuration fails validation; it never silently falls back
to a default. Settings are read at process startup and changing them requires
process restart. V1 adds no hot reload, Redis settings broadcast, Admin
mutation, or database mutation.

Existing Redis state contains state, timestamps, and ownership—not an
authoritative copy of circuit configuration. After a deployment settings
change, already-established `open_until_ms` and `probe_expires_at_ms` deadlines
retain their absolute meaning until they expire or transition. A CLOSED failure
history is pruned against the newly configured window on the next observation;
future transitions use the current settings. This avoids retroactively
rewriting deadlines or assuming a Redis flush. Mixed settings across live
instances are unsupported, so deployment must coordinate restart/config
rollout before those instances jointly serve traffic.

## M3-R4 handoff and impact

M3-R4 must consume typed, validated `CircuitConfig` (name illustrative) with
the six settings above rather than embed the defaults in Python or Lua.
Target identity, failure-window, probe ownership, and transition semantics
remain for M3-R4 to reconcile.

This addendum introduces no circuit runtime, Lua script, dynamic reload,
database table/column/constraint, Alembic migration, or Control Plane endpoint.
PostgreSQL remains authoritative for persisted configuration already represented
by its schema; Redis remains ephemeral circuit coordination state.
