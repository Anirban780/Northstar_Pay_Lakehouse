import uuid
import sys
import os
import json
import importlib 
from pyspark.sql import SparkSession

# Standard imports for loading loaders
from src.bronze.load_customers import CustomersBronzeLoader
from src.bronze.load_merchants import MerchantsBronzeLoader
from src.bronze.load_transactions import TransactionsBronzeLoader
from src.bronze.load_device_logs import DeviceLogsBronzeLoader

def get_databricks_run_id(spark: SparkSession) -> str:
    """
    Retrieves the native Databricks Job or Notebook Run ID within a .py module.
    Falls back to a generated UUID if executed outside of Databricks.
    """
    try:
        # In a raw .py file, we safely initialize DBUtils using the passed-in spark session
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
        context = json.loads(context_str)
        
        run_id = context.get("tags", {}).get("runId") or context.get("currentRunId")
        if run_id:
            return str(run_id)
            
    except (NameError, ImportError, AttributeError):
        # Triggered if running locally or outside of standard Databricks
        pass
    except Exception:
        pass
        
    return str(f"local-{uuid.uuid4()}")

def run_all(spark: SparkSession, run_id: str = None) -> None:
    """
    Executes all Bronze loaders sequentially, passing the spark session down.
    """
    run_id = run_id or get_databricks_run_id(spark)
    print(f"Starting Bronze Ingestion Run. Run ID: {run_id}")
    
    # Pass the spark session to each individual table loader
    loaders = [
        ("Customers", CustomersBronzeLoader(spark)),
        ("Merchants", MerchantsBronzeLoader(spark)),
        ("Transactions", TransactionsBronzeLoader(spark)),
        ("Device Logs", DeviceLogsBronzeLoader(spark)),
    ]
    
    failures = []
    for name, loader in loaders:
        print("\n==================================================")
        print(f"Starting Ingestion for: {name}")
        print("==================================================")
        try:
            loader.run(run_id)
            print(f"STATUS: {name} completed successfully.")
        except Exception as e:
            print(f"STATUS: {name} FAILED with error: {str(e)}", file=sys.stderr)
            failures.append((name, e))
            
    print("\n==================================================")
    print("Bronze Ingestion Run Summary")
    print("==================================================")
    if failures:
        print("Errors encountered during the run in the following loaders:")
        for name, err in failures:
            print(f" - {name}: {str(err)}")
        raise RuntimeError("Bronze ingestion pipeline completed with failures.")
    else:
        print("All loaders completed successfully without errors.")

def main(spark: SparkSession, catalog: str = None, env: str = None, run_id: str = None) -> None:
    """
    Entry point that receives configuration arguments from the notebook wrapper.
    """
    if env:
        env_clean = env.strip().lower()
        os.environ["DBX_ENV"] = env_clean
        
        try:
            import src.config
            importlib.reload(src.config)
            print(f"Successfully forced configuration reload to environment: {env_clean.upper()}")
        except ImportError:
            pass
            
    run_all(spark, run_id=run_id)

if __name__ == "__main__":
    # If this file is executed directly as a script instead of a notebook,
    # it generates its own spark session locally.
    spark_session = SparkSession.builder.getOrCreate()
    main(spark_session)