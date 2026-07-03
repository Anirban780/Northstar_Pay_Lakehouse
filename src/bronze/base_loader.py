from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

import pyspark.sql.functions as F
from src.config import CATALOG, SCHEMA_BRONZE

class BronzeLoader(ABC):
    """
    Base class for all Bronze Layer Auto Loader streams.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Implement __init__ to store the SparkSession and configuration values from config.py.
    2. Implement _add_audit_cols to add:
       - '_ingested_at': current timestamp of ingestion.
       - '_source_file': the name of the file being ingested (use input_file_name() function).
       - '_pipeline_run_id': the run_id passed to the method to correlate all records in a run.
    3. Implement _checkpoint_path to construct a path like:
       - "/Volumes/<catalog>/<schema_bronze>/checkpoints/<table_key>"
       - Note: Use values from config.py (CATALOG, SCHEMA_BRONZE).
    4. Implement _schema_location to construct a path like:
       - "/Volumes/<catalog>/<schema_bronze>/schemas/<table_key>"
    5. Implement run as an abstract method to execute the streaming pipeline.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        
        # TODO: Load CATALOG, SCHEMA_BRONZE, etc. from src.config
        self.catalog = CATALOG
        self.schema_bronze = SCHEMA_BRONZE

        pass

    def _add_audit_cols(self, df: DataFrame, run_id: str) -> DataFrame:
        """
        Adds metadata audit columns to the DataFrame:
        - _ingested_at (TimestampType)
        - _source_file (StringType)
        - _pipeline_run_id (StringType)
        """

        # TODO: Add _ingested_at, _source_file, and _pipeline_run_id to df
        # Hint: use F.current_timestamp(), F.input_file_name(), and F.lit(run_id)
        df = df.withColumn("_ingested_at", F.current_timestamp())   \
               .withColumn("_source_file", F.col("_metadata")["file_path"]) \
               .withColumn("_pipeline_run_id", F.lit(run_id))
        
        return df

    def _checkpoint_path(self, table_key: str) -> str:
        """
        Standardizes checkpoint locations within the Unity Catalog volume or DBFS.
        Example: /Volumes/{catalog}/{schema_bronze}/checkpoints/{table_key}
        """
        # TODO: Construct and return the checkpoint path
        return f"/Volumes/{self.catalog}/{self.schema_bronze}/checkpoints/{table_key}"

    def _schema_location(self, table_key: str) -> str:
        """
        Standardizes schema inference paths for Auto Loader (cloudFiles).
        Example: /Volumes/{catalog}/{schema_bronze}/schemas/{table_key}
        """
        # TODO: Construct and return the schema location path
        return f"/Volumes/{self.catalog}/{self.schema_bronze}/schemas/{table_key}"

    @abstractmethod
    def run(self, run_id: str) -> None:
        """
        Abstract method to execute the ingestion pipeline.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Each loader must implement its own run logic.")
