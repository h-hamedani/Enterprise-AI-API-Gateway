# M2.11R Admin Read/List OpenAPI Contract Reconciliation

Status: authoritative addendum to the V1.2 OpenAPI contract for the affected
M2 Admin read/list operations only.

## Finding and resolution

The established M2 runtime uses signed, tenant-bound keyset pagination for
applications, API keys, services, routes, LLM providers, LLM targets, LLM
models, LLM aliases, and audit logs. Some OpenAPI operations omitted their
query parameters or typed response schemas. The authoritative OpenAPI now
describes the existing behavior; runtime pagination is unchanged.

Each paginated operation accepts optional `cursor` and `limit`, where `limit`
defaults to 50 and is constrained to 1 through 200. Responses contain exactly
`data` and nullable `next_cursor`. Page items use the same schema as their
corresponding single-resource response. Ordering remains `created_at DESC,
id DESC`; cursor encoding and signing remain opaque.

API-key permissions and model-price collections remain complete ordered child
sets without cursor parameters.

Audit logs remain tenant-scoped, secret-safe, and ordered by `created_at DESC,
id DESC`. The config-version read remains tenant-scoped and returns zero when
no version row exists. Admin health remains a safe dependency summary and does
not expose configuration, cache, or invalidation state.

M2 Admin operations declare `AdminToken` bearer authentication only. Admin
session/cookie behavior is deferred to the contracted UI/session milestone.
Rate-limit policy administration is deferred to M3. Their documented paths are
intentional later-milestone contract surfaces, not missing M2 implementation.

## Impact

No database migration, persistence change, new endpoint, mutation behavior, or
runtime pagination change is introduced.
