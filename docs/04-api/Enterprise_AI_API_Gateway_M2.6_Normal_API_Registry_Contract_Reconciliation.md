# M2.6 Normal API Registry Contract Reconciliation

Status: authoritative addendum to the V1.2 OpenAPI contract for the affected Normal API Admin schemas only.

## Purpose and precedence

This addendum resolves three inconsistencies between the frozen HTTP schemas and the implemented M1 physical model. It supersedes only the affected `Service`, `ServiceCreate`, `ServicePatch`, `Route`, `RouteCreate`, and `RoutePatch` schema clauses. Endpoint paths, methods, security, responses, and all unrelated schemas remain unchanged.

The existing M1 physical model is the persistence authority for these decisions. Historical DOCX artifacts remain unchanged for traceability. The authoritative OpenAPI YAML is updated alongside this addendum.

## Service timeout reconciliation

The former HTTP field `timeout_ms` is removed from the affected Service schemas. Keeping it would require an undocumented mapping to five independently persisted phase-specific timeout columns.

| HTTP field | Physical column | Type | Null/default | Validation |
| --- | --- | --- | --- | --- |
| `connect_timeout_seconds` | `normal_api_services.connect_timeout_seconds` | integer seconds | non-null; create default 5 | greater than zero |
| `pool_timeout_seconds` | `normal_api_services.pool_timeout_seconds` | integer seconds | non-null; create default 5 | greater than zero |
| `write_timeout_seconds` | `normal_api_services.write_timeout_seconds` | integer seconds | non-null; create default 30 | greater than zero |
| `read_idle_timeout_seconds` | `normal_api_services.read_idle_timeout_seconds` | integer seconds | non-null; create default 60 | greater than zero |
| `pre_response_timeout_seconds` | `normal_api_services.pre_response_timeout_seconds` | integer seconds | non-null; create default 120 | greater than zero |

All five fields are returned by `Service`, optional on `ServiceCreate` with the listed defaults, and optional mutable fields on `ServicePatch`. Their names, units, defaults, and positive-value constraints match the current SQLAlchemy model and M1 migration. No generic timeout column or mapping is introduced.

## Route identity reconciliation

One Admin Route resource now corresponds to exactly one `normal_api_routes` row, one stable route ID, and one scalar HTTP `method`. The former `methods[]` property is replaced with `method` in `RouteCreate`, `RoutePatch`, and therefore `Route`.

The allowed values remain `GET`, `POST`, `PUT`, `PATCH`, and `DELETE`. `method` is required on create and may be changed by patch. Existing tenant/service/method/path uniqueness and overlap validation apply to the resulting row state.

Retaining `methods[]` would require a new logical route or grouping identity to make GET, PATCH, lifecycle, and `resource_type=ROUTE` permissions refer to one stable resource. No such grouping structure exists in the physical model, and this reconciliation intentionally introduces none. A ROUTE permission continues to reference one concrete method-specific route ID.

## Route configuration fields

`upstream_path_template` is required on create, optional on patch, and present in Route responses. It is a non-empty string with a maximum length of 1024, matching the non-null `varchar(1024)` physical column. This reconciliation defines representation only; semantic placeholder validation remains the responsibility of the M2.6 Admin validator and does not define a new templating language.

`header_policy` is optional on create and patch, present in Route responses when represented through `RouteCreate`, and defaults to null on create. It is an object or null matching the nullable JSONB/dictionary persistence representation. No policy keys or runtime header-rewrite behavior are introduced by this addendum.

Route `priority` is explicitly non-negative, matching the database constraint. Existing `timeout_ms` remains the nullable, positive route-level override and is not the superseded service timeout.

## Database and implementation impact

The reconciled HTTP contract maps directly to the existing M1 tables and constraints. No database migration, route grouping field, grouping table, or runtime behavior is required. M2.6 business endpoints remain outside this reconciliation task.
