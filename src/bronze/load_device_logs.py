from src.bronze.base_loader import BronzeLoader
from src.config import SOURCE_PATHS, bronze_fq 

class DeviceLogsBronzeLoader(BronzeLoader):
    """
    Auto Loader stream for Device Session Logs dataset.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Import necessary configuration parameters from src.config.
    2. Read stream from SOURCE_PATHS["device_session_logs"] using format("cloudFiles") and the options dictionary provided below.
    3. Add audit columns using self._add_audit_cols(df, run_id).
    4. Write stream using df.writeStream:
       - format -> "delta"
       - outputMode -> "append"
       - option("checkpointLocation", self._checkpoint_path("device_session_logs"))
       - trigger(availableNow=True)
       - toTable(bronze_device_session_logs_raw FQ name)
    """

    def run(self, run_id: str) -> None:
        # ============================
        # Construction of the schema location path
        schema_loc = self._schema_location("device_session_logs")
        checkpoint_path = self._checkpoint_path("device_session_logs")
        
        # Auto Loader option dictionary for schema evolution and JSON format
        cloud_files_options = {
            "cloudFiles.format": "json",
            "cloudFiles.schemaLocation": schema_loc,
            "cloudFiles.schemaEvolutionMode": "rescue",
            "recursiveFileLookup": "true"
        }
        # ============================

        # TODO: Implement spark.readStream using format("cloudFiles") and options(cloud_files_options)
        # Path should be config.SOURCE_PATHS["device_session_logs"]
        device_session_logs_df = (
            self.spark.readStream
            .format("cloudFiles")
            .options(**cloud_files_options)
            .load(SOURCE_PATHS["device_session_logs"])
        )
        
        # TODO: Apply audit columns via self._add_audit_cols(df, run_id)
        device_session_logs_df = self._add_audit_cols(device_session_logs_df, run_id)
        
        # TODO: Implement writeStream to append to bronze_device_session_logs_raw with trigger(availableNow=True)
        query =(
            device_session_logs_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(bronze_fq("device_session_logs"))
        )
        query.awaitTermination()
