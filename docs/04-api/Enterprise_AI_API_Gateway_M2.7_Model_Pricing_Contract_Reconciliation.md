# M2.7 Model Pricing Contract Reconciliation

Status: authoritative addendum to the V1.2 OpenAPI contract for model-pricing schemas only.

## Purpose and precedence

The former HTTP contract exposed optional, nullable `input_price_per_1m_tokens` and `output_price_per_1m_tokens`, while the M1 physical model requires non-null per-unit prices, an explicit positive token unit, and an explicit currency. No frozen rule defined null conversion, an implicit currency, or normalization to one million tokens.

This addendum supersedes only `ModelPriceWrite`, `ModelPricePatch`, and `ModelPrice` pricing-field definitions. Historical DOCX artifacts remain unchanged. Pricing endpoint paths, methods, security, effective-window behavior, and unrelated schemas are unchanged.

## Rejected implicit assumptions

The Control Plane must not convert a missing price to zero, assume USD or another currency, assume `unit_tokens = 1_000_000`, or maintain parallel per-million and per-unit representations.

## Canonical physical/API crosswalk

| HTTP field | Physical column/type | Create | Patch | Validation and default |
| --- | --- | --- | --- | --- |
| `input_price_per_unit` | `numeric(20,10) NOT NULL` | required, non-null | optional; non-null when supplied | value >= 0; no default |
| `output_price_per_unit` | `numeric(20,10) NOT NULL` | required, non-null | optional; non-null when supplied | value >= 0; no default |
| `unit_tokens` | `integer NOT NULL` | required, non-null | optional; non-null when supplied | value > 0; no default |
| `currency` | `char(3) NOT NULL` | required, non-null | optional; non-null when supplied | exactly three uppercase ASCII letters; no default or normalization |
| `effective_from` | `timestamptz NOT NULL` | required, non-null | optional; non-null when supplied | date-time; no default |
| `effective_to` | `timestamptz NULL` | optional; null allowed | optional; null explicitly opens the window | null or later than `effective_from`; no default |

`numeric(20,10)` is the persistence precision and scale. OpenAPI represents these decimal values as `number` and documents the physical decimal representation; implementations must use decimal-safe parsing and persistence rather than binary-float normalization.

## Create, patch, and response contracts

`ModelPriceCreate` replaces `ModelPriceWrite`. It requires both price values, `unit_tokens`, `currency`, and `effective_from`. `effective_to` is optional and nullable because null represents an open-ended interval.

`ModelPricePatch` uses normal PATCH semantics: omission means unchanged. Supplied prices, `unit_tokens`, `currency`, and `effective_from` cannot be null. `effective_to` may explicitly be null to make the interval open-ended.

`ModelPrice` references `ModelPriceCreate` and adds the already-frozen `id` and `model_id`, so responses expose the canonical persisted representation without a second per-million view.

## Effective windows and overlap

Validity intervals are half-open: `[effective_from, effective_to)`. A null `effective_to` means positive infinity. `effective_to` must be later than `effective_from` when present. Windows may not overlap for the same `model_id`, regardless of currency. PostgreSQL's exclusion constraint is the final backstop; a conflicting public mutation returns the frozen pricing conflict response without exposing constraint details.

## Database and implementation impact

The reconciled schemas map directly to the existing M1 `model_prices` table. No migration, conversion formula, implicit unit, implicit currency, FX behavior, or runtime cost-calculation behavior is introduced. M2.7 business endpoints remain outside this reconciliation task.
