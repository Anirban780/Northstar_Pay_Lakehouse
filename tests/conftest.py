"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import json
import csv
import os
import sys
import pathlib
from contextlib import contextmanager

# 1. Standard local dependencies (Must be present)
try:
    from pyspark.sql import SparkSession
    import pytest
except ImportError:
    raise ImportError(
        "Core test dependencies not found.\n\nRun tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv."
    )

# 2. Optional Databricks dependencies (Safe if missing locally)
try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    HAS_DATABRICKS = True
except ImportError:
    HAS_DATABRICKS = False


def _should_use_remote() -> bool:
    """Determine if we should attempt a remote Databricks Connect connection."""
    # Check for active Databricks Connect environment variables
    remote_env_vars = [
        "SPARK_REMOTE",
        "DATABRICKS_HOST",
        "DATABRICKS_CLUSTER_ID",
        "DATABRICKS_SERVERLESS_COMPUTE_ID",
    ]
    return any(os.environ.get(var) for var in remote_env_vars)


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests."""
    return SparkSession.builder.getOrCreate()


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    if not HAS_DATABRICKS:
        return

    conf = WorkspaceClient().config
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    with _allow_stderr_output(config):
        # Only attempt Databricks initialization if the libraries are installed
        if HAS_DATABRICKS:
            _enable_fallback_compute()
            if hasattr(DatabricksSession.builder, "validateSession"):
                DatabricksSession.builder.validateSession(True).getOrCreate()
            else:
                DatabricksSession.builder.getOrCreate()
        else:
            # Fallback to standard local PySpark engine
            print("\n🏎️ Databricks Connect not found. Initializing local PySpark...", file=sys.stderr)
            SparkSession.builder \
                .master("local[*]") \
                .appName("northstar-pay-local-tests") \
                .config("spark.sql.shuffle.partitions", "1") \
                .getOrCreate()