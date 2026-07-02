# NOTES.md — Northstar Pay Pipeline

Lightweight project notes. Replaces a separate `common/` and `docs/` folder —
shared config lives in `config.py` at project root, and project knowledge
lives here.

---

## Project Layout

```
/src
  /bronze   -> raw ingestion (Auto Loader), no transformations
  /silver   -> typing, cleansing, business-rule flags, SCD2
  /gold     -> dimensions, facts, marts
  /dq       -> reusable DQ checks + thresholds + scorecard builder
  /tests    -> unit tests for silver transforms and dq checks
config.py    -> shared constants (catalog, schema, thresholds)
NOTES.md     -> this file
```

---

## Config Reference (config.py)

Keep these in one place, import everywhere instead of hardcoding:

- `CATALOG` / `SCHEMA_BRONZE` / `SCHEMA_SILVER` / `SCHEMA_GOLD`
- `VOLUME_PATH` (landing zone root)
- DQ thresholds (see below)

---

## Known Data Quality Issues (do not silently fix — flag instead)

### Blocker 1 — signup_date is not source-of-truth
- 107,232 transactions (~20%) occur before the customer's `signup_date`.
- Broad across all segments/risk bands -> systemic, not a niche cohort.
- Pattern matches a customer-master overwrite/migration, not a timezone bug.
- **Rule:** don't use raw `signup_date` for tenure/funnel/eligibility features.
- **Silver flag:** `is_signup_date_suspect`

### Blocker 2 — geo velocity anomalies
- 1,019 impossible geographic velocity pairs in device_session_logs.
- Not concentrated by app_version/OS/device -> not a single client bug.
- Fraud correlation is negligible -> looks like a DQ issue, not a fraud signal.
- `accuracy_m` filter logic has a bug: reported accuracy is mostly low
  (good), but "reliable GPS" count is much smaller than expected. Fix the
  threshold/direction logic before trusting this field.
- **Silver flags:** `is_geo_velocity_anomaly`, `is_geo_low_accuracy`

### Warning — amount outliers
- 573 transactions in top 0.1% by amount.
- Not synthetic/test data (no round-number clustering).
- Outliers align with legitimate high-value txn types/currencies.
- Open question: outliers correlate heavily with `is_fraud_flag = true`.
  Could be real fraud behavior, could be label leakage (flag may be
  amount-threshold-derived), unconfirmed.
- **Silver flag:** `is_amount_outlier` — keep in pipeline, never suppress.

### Resolvable in-house (no escalation needed)
- Fix `accuracy_m` reliability filter logic.
- Decide on 526 negative transaction amounts: valid refunds vs. data error.

### Needs escalation to data owner
- Confirm whether `signup_date` is authoritative or migration-overwritten.
- Confirm whether `is_fraud_flag` is a true outcome label or rules-derived.

---

## DQ Thresholds (baseline, for regression detection)

| Metric | Baseline | Notes |
|---|---|---|
| Pre-signup transaction rate | ~20% | Known issue, alert if it moves materially |
| Geo velocity anomaly rate | ~0.4% (1,019 / 261,414) | Alert on increase |
| Amount outlier rate | 0.1% by definition | Monitor composition, not just count |
| Referential integrity orphan rate | 0% | Hard fail if this regresses |

---

## Reminders

- Bronze = raw, untouched, audit trail only.
- Silver = typed + flagged, nothing silently dropped except true parse failures.
- Gold = flags carried forward as dimensions, not used as filters, unless a
  mart explicitly documents why.
- Any mart using `is_fraud_flag` must note the label-provenance caveat above.