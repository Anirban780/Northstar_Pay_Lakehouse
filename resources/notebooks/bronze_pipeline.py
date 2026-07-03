# Databricks notebook source
# ruff: noqa
# Bronze Ingestion Pipeline Notebook Wrapper
# This notebook is deployed via Declarative Automation Bundles and orchestrates the Bronze Auto Loader streams.

# ==========================================
# 0. Fix Python Path to resolve (root)/src
# ==========================================
import os
import sys

# Inject repo root dynamically based on current notebook location
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()

# Walks up 2 levels: /Workspace/.../resources/notebooks/ → repo root
repo_root = "/Workspace/" + "/".join(notebook_path.strip("/").split("/")[:-3])
sys.path.insert(0, repo_root)

print(f"Injected sys.path: {repo_root}")

# ==========================================
# 1. Imports
# ==========================================
import json
import uuid
from src.bronze import run_bronze

# ==========================================
# 2. Define Databricks widgets for configuration
# ==========================================
# Databricks automatically provides the 'dbutils' object globally in notebooks.
dbutils.widgets.text("catalog", "northstar_dev")  # noqa: F821
dbutils.widgets.text("env", "dev")

# ==========================================
# 3. Retrieve widget values
# ==========================================
catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("env")

# ==========================================
# 4. Extract and Pass the Native Run ID
# ==========================================
try:
    context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context = json.loads(context_str)
    native_run_id = context.get("tags", {}).get("runId") or context.get("currentRunId")
    run_id = str(native_run_id) if native_run_id else str(f"local-{uuid.uuid4()}")
except Exception:
    run_id = str(f"local-{uuid.uuid4()}")

print(f"Executing Bundle Environment: {env.upper()}")
print(f"Target Unity Catalog: {catalog}")
print(f"Resolved Databricks Runtime Run ID: {run_id}")



# ==========================================
# 5. Pre-requisite: Create Bronze Schema + Volumes If Not Exists
# ==========================================
schema_name = "bronze"
full_schema_path = f"`{catalog}`.`{schema_name}`"

print(f"Ensuring destination schema exists: {full_schema_path}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema_path}")

# Auto Loader requires these two volumes for schema inference and checkpointing
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema_name}`.`schemas`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema_name}`.`checkpoints`")


# ==========================================
# 6. Run the Bronze ingestion pipeline orchestrator
# ==========================================
# Databricks automatically provides the 'spark' object globally in notebooks.
# We pass it directly into your backend python module.
run_bronze.main(spark, catalog=catalog, env=env, run_id=run_id)