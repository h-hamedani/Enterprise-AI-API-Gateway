# M3-R1 Redis Invalidation Channel Contract Reconciliation

Status: authoritative addendum to the V1 runtime configuration-invalidation
contract. It supersedes only the invalidation channel name and its M2-to-M3
compatibility treatment; all historical M2 artifacts remain unchanged.

## Original contradiction and precedence

M2 shipped its best-effort Redis Pub/Sub publisher on `gateway:config`, matching
the HLD V1.1 channel name. The later, milestone-specific M3 Execution Pack V1.1
locks the versioned Redis namespace and names `gw:v1:invalidate` as the
invalidation channel.

The M3 Execution Pack is the more specific and later authority for M3 runtime
coordination. Therefore there is exactly one canonical M3 channel:
`gw:v1:invalidate`. The accepted `m2-pass` release remains historically correct
for M2 and must not be rewritten.

## Compatibility and transition

M3.1 does not change invalidation publication or consumption. In M3.6, before
an M3 invalidation consumer is enabled, the Control Plane publisher must publish
the same encoded event to both channels:

- canonical M3 channel: `gw:v1:invalidate`
- historical M2 compatibility channel: `gateway:config`

M3 consumers subscribe only to `gw:v1:invalidate`. They must not subscribe to
both channels, because dual subscription would turn one logical mutation into
duplicate delivery during the transition.

Publication remains post-commit and best effort. Failure to publish to either
channel cannot roll back committed PostgreSQL state. Partial dual-publication
success is permitted by these delivery semantics and is repaired for M3
consumers by the frozen PostgreSQL `config_versions` reconciliation contract.
There is no exactly-once claim.

The compatibility publication to `gateway:config` may be retired only after an
explicit release boundary confirms that no supported M2 consumer remains. No
automatic retirement date is implied. At steady state, publishers and consumers
use only `gw:v1:invalidate`.

## Payload and authority

Both transition publications contain the exact same M2 payload:

```json
{
  "tenant_id": "<uuid>",
  "resource_type": "<stable type>",
  "resource_id": "<uuid>",
  "version": 42
}
```

No timestamp, sequence ID, retry marker, request ID, configuration body,
authentication material, or secret is added. PostgreSQL remains the durable
source of configuration truth. Redis contains only the non-durable invalidation
signal.

Delivery continues to use Redis Pub/Sub. It is best effort, non-durable, and not
exactly once. This reconciliation introduces no outbox, durable event record,
background retry worker, or alternate transport.

## Redis namespace rule

M3 runtime Redis names use the frozen `gw:v1:` prefix so incompatible future
formats can coexist during a controlled migration. This decision governs new M3
runtime keys and channels. It does not retroactively rename unrelated M2 keys.

## Package and release impact

- M3.1 may implement Redis client lifecycle, reconnect behavior, namespace
  foundations, and health telemetry without changing the M2 publisher.
- M3.6 owns the minimal dual-publication compatibility change and the M3
  subscriber on `gw:v1:invalidate`.
- M3.7 owns PostgreSQL version reconciliation for missed, partial, duplicate, or
  out-of-order Pub/Sub delivery.
- The `m2-pass` tag and all M2 commits remain immutable.
- No database or Alembic migration is required.

Invalidation messages must never contain raw `adm_` tokens, raw `gw_` keys,
provider or service credentials, ciphertext, authorization headers,
Idempotency-Key values, request bodies, or configuration bodies.
