# NorthStar Pay — Data Generation: How to Run This

## What's here
```
northstar_pay/
├── databricks.yml                          # Asset Bundle config (dev/staging/prod)
└── datagen/
    ├── 01_config_and_dimensions.py         # customers + merchants -> UC Volume
    ├── 02_generate_transactions.py         # fact_transactions w/ Pareto skew + fraud
    └── 03_generate_device_session_logs.py  # nested JSON, intentional schema drift
```

## Option A — Run manually in the Databricks UI (fastest to validate)
1. Import the three `.py` files into your workspace as notebooks (Workspace → Import → choose "Source" format; the `# Databricks notebook source` header makes them paste in as proper notebooks with cell breaks already).
2. Attach to a cluster (DBR 15.4 LTS or later; no special libraries needed beyond `faker`, which you can add via `%pip install faker` in a cell at the top of notebook 01 and 03 if it isn't already on your cluster).
3. Run `01_config_and_dimensions.py` first — set the `catalog` widget to wherever you want this to land (e.g. `northstar_dev`). It creates the catalog/schema/volume if missing.
4. Run `02_generate_transactions.py` and `03_generate_device_session_logs.py` — order between these two doesn't matter, both only depend on notebook 01 having run.

## Option B — Deploy via Databricks Asset Bundle (the portfolio-correct path)
This is what `databricks.yml` is for — it wires the three notebooks into a single Workflow job with proper task dependencies (dimensions → transactions, dimensions → device logs, running the latter two in parallel).

```bash
# from the northstar_pay/ directory
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev datagen_job
```

Before running, edit `databricks.yml`:
- Replace `https://adb-xxxxxxxxxxxxxxxx.azuredatabricks.net` with your actual Azure Databricks workspace URL (find it under your workspace's URL bar).
- Replace `your-email@example.com` with your own user identity (used for `run_as` in `dev` and failure notifications).
- For `prod`, replace `northstar-prod-sp` with a real service principal if you have one — otherwise switch `run_as` to your own user for now.

To promote to staging/prod once dev is validated:
```bash
databricks bundle deploy -t staging
databricks bundle run -t staging datagen_job
```

## What gets produced
Lands in `/Volumes/<catalog>/raw/landing/`:
- `customers/full_load/*.csv` — 50k rows
- `merchants/full_load/*.csv` — 9k rows
- `transactions/daily_drops/txn_date=YYYY-MM-DD/*.csv` — ~820k rows, partitioned by day
- `device_session_logs/event_date=YYYY-MM-DD/*.json` — ~260k nested JSON events, one file per day

A small scratch schema (`<catalog>.datagen_scratch`) is also created — this just holds lightweight customer/merchant ID lookup tables so notebook 02 can guarantee referential integrity without re-parsing the full CSVs. Safe to drop after generation if you want a clean catalog.

## Verifying the skew/fraud injection actually landed correctly
Notebook 02 prints sanity checks before writing to disk:
- Actual fraud rate vs target (~1.8%)
- Count of injected dirty rows (negative amounts, null customer_id) — these are your bait for the Silver-layer quarantine logic
- Min/max transaction timestamp

If the printed fraud rate or skew looks off, it's almost certainly because the `num_transactions` or `random_seed` widget was changed without re-running notebook 01 first (the customer/merchant ID pools must match what notebook 02 reads).

## Next step
Once this lands cleanly in the Volume, you're ready for the EDA/discovery checklist (Section 3 of the blueprint) before writing any Bronze/Silver pipeline code. Want me to write the Autoloader/DLT Bronze→Silver pipeline next, or the EDA notebook first?