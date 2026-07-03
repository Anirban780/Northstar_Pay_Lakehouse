from src.config import SOURCE_PATHS, bronze_fq
from src.bronze.base_loader import BronzeLoader
import pyspark.sql.functions as F

class CustomersBronzeLoader(BronzeLoader):
    """
    Auto Loader stream for Customers dataset.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Import necessary configuration parameters from src.config (e.g. SOURCE_PATHS, BRONZE_TABLES).
    2. In run(self, run_id):
       - Setup cloudFiles reader options:
         * format -> "csv"
         * header -> "true"
         * inferSchema -> "true" (Auto Loader will infer schemas, all strings at source)
         * cloudFiles.schemaLocation -> self._schema_location("customers")
       - Read stream using spark.readStream with format("cloudFiles") and path SOURCE_PATHS["customers"].
       - Add audit columns using self._add_audit_cols(df, run_id).
       - Write stream using df.writeStream:
         * format -> "delta"
         * outputMode -> "append"
         * option("checkpointLocation", self._checkpoint_path("customers"))
         * trigger(availableNow=True)
         * toTable(CATALOG.SCHEMA_BRONZE.bronze_customers_raw) -> use fq_table or fq string from config
    """

    def run(self, run_id: str) -> None:
        # TODO: Implement Structured Streaming / Auto Loader read logic for Customers
        customers_df = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("recursiveFileLookup", "true")
            .option("cloudFiles.schemaLocation", self._schema_location("customers"))
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(SOURCE_PATHS["customers"])
        )

        # TODO: Apply audit columns
        customers_df = self._add_audit_cols(customers_df, run_id)

        # TODO: Implement writeStream to append to bronze_customers_raw with trigger(availableNow=True)
        query = (
            customers_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self._checkpoint_path("customers"))
            .trigger(availableNow=True)
            .toTable(bronze_fq("customers"))
        )
        query.awaitTermination()
