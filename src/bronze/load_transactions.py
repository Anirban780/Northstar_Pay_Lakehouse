from src.bronze.base_loader import BronzeLoader
from src.config import SOURCE_PATHS, bronze_fq
from pyspark.sql import functions as F


class TransactionsBronzeLoader(BronzeLoader):
    """
    Auto Loader stream for Transactions dataset.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Import necessary configuration parameters from src.config (e.g. SOURCE_PATHS, BRONZE_TABLES).
    2. In run(self, run_id):
       - Setup cloudFiles reader options:
         * format -> "csv"
         * header -> "true"
         * inferSchema -> "true"
         * recursiveFileLookup -> "true" (required to traverse partitioned structure)
         * cloudFiles.schemaLocation -> self._schema_location("transactions")
       - Read stream using spark.readStream with format("cloudFiles") and path SOURCE_PATHS["transactions"].
       - Add audit columns using self._add_audit_cols(df, run_id).
       - Extract '_partition_date' from the source file path using F.regexp_extract(F.input_file_name(), ...)
         or standard string parsing to extract the date portion for observability.
       - Write stream using df.writeStream:
         * format -> "delta"
         * outputMode -> "append"
         * option("checkpointLocation", self._checkpoint_path("transactions"))
         * trigger(availableNow=True)
         * toTable(bronze_transactions_raw FQ name)
    """

    def run(self, run_id: str) -> None:
        schema_loc = self._schema_location("transactions")
        checkpoint_path = self._checkpoint_path("transactions")

        cloud_files_options = {
            "cloudFiles.format": "csv",
            "header": "true",
            "cloudFiles.schemaLocation": schema_loc,
            "cloudFiles.schemaEvolutionMode": "rescue",
            "recursiveFileLookup": "true"
        
        }

        # TODO: Implement Structured Streaming / Auto Loader read logic for Transactions (recursive files)
        transactions_df = (
            self.spark.readStream
            .format("cloudFiles")
            .options(**cloud_files_options)
            .load(SOURCE_PATHS["transactions"])
        )

        # TODO: Apply audit columns
        transactions_df = self._add_audit_cols(transactions_df, run_id)

        # TODO: Add _partition_date column derived from file path
        transactions_df = transactions_df.withColumn(
            "_partition_date",
            F.regexp_replace(
                F.regexp_extract(F.col("_metadata")["file_path"], r"transactions/(\d{4}/\d{2})/", 1),
                r"/",
                "-"
            )
        )

        # TODO: Implement writeStream to append to bronze_transactions_raw with trigger(availableNow=True)
        query = (
            transactions_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(bronze_fq("transactions"))
        )
        query.awaitTermination()
