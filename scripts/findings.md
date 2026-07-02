# Northstar Pay — Data Discovery Findings

## 1. Source Datasets & Schema

### customers
`/Volumes/northstar_dev/raw/landing/customers` — 50,000 records

```
root
 |-- customer_id: string      -- (PK, nullfalse)
 |-- full_name: string        -- (PII, nullfalse)
 |-- dob: string               -- (PII, date(Y-M-D), nullfalse)
 |-- signup_date: string      -- (date(Y-M-D))  ** SEE BLOCKER 1 **
 |-- segment: string          -- (enum[business, premium, retail])
 |-- risk_band: string        -- (enum[low, medium, high])
 |-- address: string          -- (PII)
 |-- kyc_status: string       -- (enum[verified, rejected])
 |-- last_updated_ts: string  -- (timestampZ, nullfalse)
```

### merchants
`/Volumes/northstar_dev/raw/landing/merchants` — 9,000 records

```
root
 |-- merchant_id: string          -- (PK, nullfalse)
 |-- merchant_name: string        -- (PII, nullfalse)
 |-- mcc_code: string             -- (integer, nullfalse, FK)
 |-- category: string             -- ()
 |-- country: string              -- ()
 |-- onboarding_date: string      -- (date(Y-M-D))
 |-- merchant_risk_score: string  -- (float)
```

### transactions
`/Volumes/northstar_dev/raw/landing/transactions` — 525,292 records

```
root
 |-- transaction_id: string   -- (PK, nullfalse)
 |-- card_id: string          -- (nullfalse)
 |-- customer_id: string      -- (FK, nullfalse)
 |-- merchant_id: string      -- (FK, nullfalse)
 |-- txn_timestamp: string    -- (timestampZ, nullfalse)
 |-- amount: string           -- (float, nullfalse)  ** SEE SECTION 4 / OUTLIERS **
 |-- currency: string         -- (currency_code, nullfalse)
 |-- mcc_code: string         -- (FK, nullfalse)
 |-- channel: string          -- (enum[POS, online, ATM])
 |-- txn_type: string         -- (enum[purchase, withdrawl])
 |-- status: string           -- (enum[approved, declined, flagged])
 |-- country: string          -- ()
 |-- is_fraud_flag: string    -- (primary ML target/label, enum[true, false])  ** SEE OPEN QUESTION: LABEL PROVENANCE **
 |-- txn_date: string         -- (date(Y-M-D), nullfalse)
```

### device_session_logs
`/Volumes/northstar_dev/raw/landing/device_session_logs` — 261,414 records

```
root
 |-- customer_id: string       -- (FK, nullfalse)
 |-- device_id: string         -- (identifier/FK)
 |-- device_meta: struct
 |    |-- app_version: string
 |    |-- device_model: string
 |    |-- os: string
 |-- event_timestamp: string   -- (timestamp, nullfalse)
 |-- event_type: string        -- (enum[login, app_open, card_freeze])
 |-- geo: struct                -- (PII)
 |    |-- accuracy_m: double
 |    |-- city: string
 |    |-- lat: double
 |    |-- long: double
 |-- ip_address: string        -- (PII)
 |-- session_id: string        -- (PK, nullfalse)
```

**Schema note:** all business fields across all four tables were inferred by Spark as `string`, including dates, timestamps, amounts, and the fraud flag. Type casting to proper `date`/`timestamp`/`decimal`/`boolean` is required in Silver — not fixed at source.

---

## 2. Data Quality Summary

**transactions**
- Total rows: 525,292
- Avg txns per customer: 11.16
- Avg txns per card: 6.7

**device_session_logs**
- Total rows: 261,414
- Avg sessions per customer: 5.26
- Avg sessions per device: 2.96

Per-column null and duplicate counts captured in EDA notebook (Step 1 cell output); no PK duplication found in any of the four tables.

---

## 3. Referential Integrity — ALL PASS

| # | Check | non_null_rows | orphan_rows | orphan_% | status |
|---|---|---|---|---|---|
| 1 | transactions.customer_id → customers.customer_id | 524,740 | 0 | 0% | PASS |
| 2 | transactions.merchant_id → merchants.merchant_id | 525,292 | 0 | 0% | PASS |
| 3 | transactions.mcc_code → merchants.mcc_code | 525,292 | 0 | 0% | PASS |
| 4 | device_session_logs.customer_id → customers.customer_id | 261,414 | 0 | 0% | PASS |

No referential integrity issues. Safe to build FK-based joins in Silver/Gold without additional guarding logic beyond standard `expect` checks.

---

## 4. Key Distributions

**Transaction amount**
- Min: -1590.23
- Max: 14,366.62
- Mean: 46.9607
- Median: 25.06
- Std dev: 128.0401
- Negative values: 526 *(unresolved — see Open Question below)*
- Zero values: 0

Channel breakdown, status breakdown, and fraud rate captured in EDA notebook (Step 5 cell output).

---

## 5. Temporal Validation Flags

| Check | Result | Status |
|---|---|---|
| Future-dated transactions | 0 | PASS |
| Pre-signup transactions | 107,232 | **FAIL — Blocker 1** |
| Days with zero transactions | 0 | PASS |
| Future DOB | 0 | PASS |
| Customers age < 18 | 0 | PASS |
| Customers age > 100 | 0 | PASS |

---

## 6. Outlier Analysis

| Check | Result | Status |
|---|---|---|
| Amount outliers (top 0.1%) | 573 | WARNING |
| Impossible geo velocity pairs | 1,019 | **FAIL — Blocker 2** |

---

## 7. PII Inventory

Total tagged columns: 13 — High: 6, Medium: 6, Low: 1

| Table | Column | Sensitivity | Notes |
|---|---|---|---|
| customers | address | High | Direct identifier — physical address |
| customers | dob | High | Direct identifier — date of birth |
| customers | full_name | High | Direct identifier — personal name |
| device_session_logs | geo.lat | High | Precise location — PII under GDPR |
| device_session_logs | geo.long | High | Precise location — PII under GDPR |
| device_session_logs | ip_address | High | Direct identifier — PII under GDPR |
| customers | customer_id | Medium | Indirect identifier — links to all other tables |
| device_session_logs | customer_id | Medium | Indirect identifier — FK to customers |
| device_session_logs | device_id | Medium | Device fingerprint — linkable to a person over time |
| device_session_logs | geo.city | Medium | Coarse location — lower sensitivity than lat/long |
| transactions | card_id | Medium | Indirect identifier — card-level fingerprint linkable to a person |
| transactions | customer_id | Medium | Indirect identifier — FK to customers |
| merchants | merchant_name | Low | Business PII — identifies a business, not an individual |

---

## 8. Blocker Investigations

### Blocker 1 — Pre-signup transactions (107,232 rows, ~20% of transaction volume)

Findings from investigation notebook (`northstar_pay_blocker_investigation`):
- **Systemic, not niche**: distributed broadly across `segment` and `risk_band` — not concentrated in a specific cohort.
- **Large-gap, not rounding/timezone noise**: time-gap distribution skews toward large gaps, not off-by-one-day discrepancies.
- **last_updated_ts overwrite check**: pattern is consistent with `signup_date` having been overwritten during a customer-master migration, rather than reflecting the true original signup date.

**Conclusion**: `signup_date` is likely not source-of-truth for customer tenure. Field semantics may actually be closer to "last written/migrated date" than "true signup date."

**Action**: Freeze all downstream logic using raw `signup_date` for eligibility, tenure, onboarding funnel, or fraud features until confirmed. Escalate to customer/master-data owner to identify the authoritative signup field and confirm/deny the migration-overwrite hypothesis.

### Blocker 2 — Impossible geographic velocity pairs (1,019 rows)

Findings:
- **Not concentrated by app_version, OS, or device** — rules out a single buggy client version as root cause.
- **Fraud correlation negligible** (Cell 13: Fraud Correlation) — velocity anomalies do not meaningfully co-occur with `is_fraud_flag = true`. Looks more like a data-quality/control issue than a fraud signal.
- **Internal inconsistency found**: reported `accuracy_m` range is very low (implying high GPS confidence) across most rows, but the count of records passing a "reliable GPS" filter is much smaller than expected. This suggests either a null/incomplete `accuracy_m` coverage gap, or a logic/threshold-direction bug in the reliability filter — needs to be resolved before this field is trusted for any rule.

**Action**: Demote from launch blocker to secondary investigation/backlog item. Resolve the `accuracy_m` filter logic independently (does not require escalation to source owner — likely resolvable in-house). Re-evaluate velocity anomalies once accuracy coverage is understood.

### Warning — Amount outliers (573 rows, top 0.1%)

Findings:
- Eyeball + round-number check: values do not look like synthetic/test data (no suspicious round-number clustering).
- Breakdown by `txn_type`, `channel`, `currency`: outliers align with legitimate high-value transaction types/currencies — not an artifact of unconverted currency or an unexpected channel.
- **Open concern**: outlier transactions in the top sample appear heavily aligned with `flagged`/fraud-marked rows. Not yet determined whether this is (a) real fraud behavior (high-value extraction is a known fraud pattern), (b) labeling leakage (i.e., `is_fraud_flag` may have been rules-generated using an amount threshold, making the correlation circular), or (c) a synthetic test-data artifact.

**Action**: Keep amount outliers in the pipeline as flagged, not suppressed. Do not build any amount-based fraud feature until the `is_fraud_flag` provenance question (below) is resolved.

---

## 9. Open Questions to Escalate (source/data owner)

1. Is `signup_date` source-of-truth, or was it overwritten during a migration? What is the authoritative signup field, if different?
2. Is `is_fraud_flag` a true historical outcome (confirmed fraud/chargeback), or a rules-engine-generated flag? This directly affects whether amount-outlier/fraud correlation is real signal or label leakage.

## 10. Items Resolvable In-House (no escalation needed)

1. `accuracy_m` reliable-GPS filter logic — re-check threshold direction and null handling.
2. Negative transaction amounts (526 rows) — determine whether these represent valid refunds/reversals or a data error, and encode the decision as a Silver rule either way.

---

## 11. Deployment Readiness Verdict

**VERDICT: RED — NOT READY** for a fully trusted pipeline; approved to proceed in **quarantine-friendly form** (Bronze raw, Silver flags known issues, Gold excludes untrusted lifecycle fields pending source confirmation).

Blockers:
- [ ] Pre-signup transactions: 107,232 — pending source-owner confirmation
- [ ] Impossible geo velocity pairs: 1,019 — pending accuracy_m logic fix + re-evaluation

Warnings:
- [ ] Amount outliers: 573 — flag and monitor, do not suppress