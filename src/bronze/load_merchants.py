from src.config import SOURCE_PATHS, bronze_fq
from src.bronze.base_loader import BronzeLoader

class MerchantsBronzeLoader(BronzeLoader):
    """
    Auto Loader stream for Merchants dataset.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Import necessary configuration parameters from src.config (e.g. SOURCE_PATHS, BRONZE_TABLES).
    2. In run(self, run_id):
       - Setup cloudFiles reader options:
         * format -> "csv"
         * header -> "true"
         * inferSchema -> "true"
         * cloudFiles.schemaLocation -> self._schema_location("merchants")
       - Read stream using spark.readStream with format("cloudFiles") and path SOURCE_PATHS["merchants"].
       - Add audit columns using self._add_audit_cols(df, run_id).
       - Write stream using df.writeStream:
         * format -> "delta"
         * outputMode -> "append"
         * option("checkpointLocation", self._checkpoint_path("merchants"))
         * trigger(availableNow=True)
         * toTable(bronze_merchants_raw FQ name)
    """

    def run(self, run_id: str) -> None:
        schema_loc = self._schema_location("merchants")
        checkpoint_path = self._checkpoint_path("merchants")

        cloud_files_options = {
            "cloudFiles.format": "csv",
            "header": "true",
            "cloudFiles.schemaLocation": schema_loc,
            "cloudFiles.schemaEvolutionMode": "rescue",
            "recursiveFileLookup": "true"
        
        }
        # TODO: Implement Structured Streaming / Auto Loader read logic for Merchants
        merchants_df = (
            self.spark.readStream
            .format("cloudFiles")
            .options(**cloud_files_options)
            .load(SOURCE_PATHS["merchants"])
        )

        # TODO: Apply audit columns
        merchants_df = self._add_audit_cols(merchants_df, run_id)

        # TODO: Implement writeStream to append to bronze_merchants_raw with trigger(availableNow=True)
        query =(
            merchants_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(bronze_fq("merchants"))
        )
        query.awaitTermination()
