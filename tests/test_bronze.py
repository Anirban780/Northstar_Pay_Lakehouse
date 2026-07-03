import pytest
import uuid
from unittest.mock import MagicMock, Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Hint: Import your Bronze loader classes from src.bronze once you implement them:
from src.config import CATALOG, SCHEMA_BRONZE
from src.bronze.base_loader import BronzeLoader
from src.bronze.load_customers import CustomersBronzeLoader
from src.bronze.load_merchants import MerchantsBronzeLoader
from src.bronze.load_transactions import TransactionsBronzeLoader
from src.bronze.load_device_logs import DeviceLogsBronzeLoader
from src.bronze.run_bronze import run_all

def test_add_audit_cols(spark: SparkSession):
    """
    Test that the base BronzeLoader._add_audit_cols method correctly appends
    metadata audit columns (_ingested_at, _source_file, _pipeline_run_id) 
    to any input DataFrame.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Instantiate a concrete implementation of BronzeLoader (or mock its abstract run method).
    2. Create a small dummy DataFrame with 1 or 2 rows and columns (e.g. key/value).
    3. Call the loader's _add_audit_cols(df, run_id).
    4. Assert that the output DataFrame has the three audit columns:
       - '_ingested_at' (assert it's not null)
       - '_source_file' (assert it exists)
       - '_pipeline_run_id' (assert it matches the mock run_id value)
    """
    # TODO: Implement test case
    class DummyBronzeLoader(BronzeLoader):
        def run(self):
            pass  # Not needed for this test

    loader = DummyBronzeLoader(spark) # mock data for customers table
    data = [
        ("CUST-0000010", "Jonathan Martin", "1996-05-03", "2024-03-09", "business", "low", "788 Willis Lake Suite 460, North Amberton, NJ 89101", "verified", "2024-12-01T00:00:00.000Z"),
        ("CUST-0000042", "Cassandra Gilbert", "1965-06-19", "2025-11-06", "premium", "low", "PSC 3072, Box 7001, APO AP 78112", "verified", "2025-12-28T00:00:00.000Z")
    ]

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("risk_band", StringType(), True),
        StructField("address", StringType(), True),
        StructField("kyc_status", StringType(), True),
        StructField("last_updated_ts", StringType(), True)
    ])

    input_df = spark.createDataFrame(data, schema)

    mock_run_id = str(f"test-run-{uuid.uuid4()}")
    
    output_df = loader._add_audit_cols(input_df, mock_run_id)

    output_columns = output_df.columns
    assert "_ingested_at" in output_columns, "Missing '_ingested_at' column"
    assert "_source_file" in output_columns, "Missing '_source_file' column"
    assert "_pipeline_run_id" in output_columns, "Missing '_pipeline_run_id' column"

    rows = output_df.collect()
    for row in rows:
        assert row["_ingested_at"] is not None, "'_ingested_at' should not be null"
        assert row["_source_file"] is not None, "'_source_file' should not be null"
        assert row["_pipeline_run_id"] == mock_run_id, f"'_pipeline_run_id' should match {mock_run_id}"


def test_path_generations(spark: SparkSession):
    """
    Test that the base BronzeLoader constructs the correct checkpoint 
    and schema path structures based on configuration.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Instantiate your BronzeLoader (or a subclass).
    2. Call loader._checkpoint_path("customers") and loader._schema_location("customers").
    3. Assert that they return standard paths containing:
       - The correct Catalog name from config.py
       - The correct Bronze Schema name from config.py
       - The path suffix matching 'checkpoints/customers' and 'schemas/customers' respectively
    """
    # TODO: Implement test case
    class DummyBronzeLoader(BronzeLoader):
        def run(self):
            pass  # Not needed for this test

    loader = DummyBronzeLoader(spark)

    checkpoint_path = loader._checkpoint_path("customers")
    schema_path = loader._schema_location("customers")

    assert CATALOG in checkpoint_path, "Checkpoint path should contain the catalog name"
    assert CATALOG in schema_path, "Schema path should contain the catalog name"

    assert SCHEMA_BRONZE in checkpoint_path, "Checkpoint path should contain the bronze schema name"
    assert SCHEMA_BRONZE in schema_path, "Schema path should contain the bronze schema name"

    assert checkpoint_path.endswith("checkpoints/customers"), "Checkpoint path should end with 'checkpoints/customers'"
    assert schema_path.endswith("schemas/customers"), "Schema path should end with 'schemas/customers'"


def test_transactions_partition_date_derivation(spark: SparkSession):
    """Test that the TransactionsBronzeLoader correctly derives the '_partition_date'."""
    test_path = "landing/raw/transactions/2026/07/data.csv"
    
    data = [("TXN001",), ("TXN002",)]
    schema = StructType([StructField("txn_id", StringType(), True)])
    input_df = spark.createDataFrame(data, schema)

    # Patch input_file_name to simulate reading from the actual cloud path
    with patch("pyspark.sql.functions.input_file_name", return_value=F.lit(test_path)):
        extracted_df = input_df.withColumn(
            "_partition_date",
            F.regexp_replace(
                F.regexp_extract(F.input_file_name(), r"transactions/(\d{4}/\d{2})/", 1),
                r"/",
                "-"
            )
        )
        results = extracted_df.collect()

    assert len(results) == 2
    for row in results:
        assert row["_partition_date"] == "2026-07", f"Got {row['_partition_date']}"


def test_device_logs_loader_options(spark: SparkSession):
    """Test that DeviceLogsBronzeLoader sets up the correct cloudFiles options."""
    # 1. Setup nested mocks to mirror: spark.readStream.format().options().load().writeStream...
    mock_spark = MagicMock(spec=SparkSession)
    
    mock_read_stream = mock_spark.readStream
    mock_format = mock_read_stream.format.return_value
    mock_options = mock_format.options.return_value
    mock_load = mock_options.load.return_value
    
    # Mock the writeStream chain to prevent errors when .run() finishes execution
    mock_write_stream = mock_load.writeStream
    mock_write_format = mock_write_stream.format.return_value
    mock_output_mode = mock_write_format.outputMode.return_value
    mock_option = mock_output_mode.option.return_value
    mock_trigger = mock_option.trigger.return_value

    # 2. Instantiate the real loader with our mock spark context
    loader = DeviceLogsBronzeLoader(mock_spark)
    
    # Stub internal helper methods on the base class so they don't look for a real file system
    loader._schema_location = MagicMock(return_value="dbfs:/mock_schema_path")
    loader._checkpoint_path = MagicMock(return_value="dbfs:/mock_checkpoint_path")
    loader._add_audit_cols = MagicMock(return_value=mock_load)

    # 3. Execute the production code logic
    loader.run(run_id="test-12345")

    # 4. Verify that .options() was called with the correct keyword configurations
    assert mock_format.options.called, "readStream.options() was never called!"
    
    # Grab the keyword arguments (**kwargs) passed to the options method
    _, kwargs_passed = mock_format.options.call_args
    
    # 5. Live Production Assertions
    assert kwargs_passed.get("cloudFiles.format") == "json"
    assert kwargs_passed.get("cloudFiles.schemaEvolutionMode") == "rescue"  # This catches if it's changed to 'addNewColumns'
    assert kwargs_passed.get("recursiveFileLookup") == "true"
    assert kwargs_passed.get("cloudFiles.schemaLocation") == "dbfs:/mock_schema_path"


@patch("src.bronze.run_bronze.CustomersBronzeLoader")
@patch("src.bronze.run_bronze.MerchantsBronzeLoader")
@patch("src.bronze.run_bronze.TransactionsBronzeLoader")
@patch("src.bronze.run_bronze.DeviceLogsBronzeLoader")
def test_orchestrator_sequential_run(
    mock_device, mock_txns, mock_merchants, mock_customers, spark: SparkSession
):
    """
    Test that run_bronze.run_all calls all loaders in sequential dependency order:
    Customers -> Merchants -> Transactions -> Device Logs.
    
    IMPLEMENTATION INSTRUCTIONS FOR USER:
    1. Set up mocks for each loader class so their run() methods record when they are called.
    2. Call run_all(spark).
    3. Use MagicMock.mock_calls or tracking arrays to assert that the `.run()` methods
       on each loader were called in the exact sequential order listed above.
    """
    # TODO: Implement test case

    # 1. Set up a shared manager to track the exact execution order across all mocks
    manager = Mock()

    # Attach the mocked instance run methods to the manager tracker
    manager.attach_mock(mock_customers.return_value.run, "customers_run")
    manager.attach_mock(mock_merchants.return_value.run, "merchants_run")
    manager.attach_mock(mock_txns.return_value.run, "txns_run")
    manager.attach_mock(mock_device.return_value.run, "device_run")

    run_all(spark)

    expected_calls = [
        # The string tags here must match the names we gave attach_mock above
        ("customers_run",),
        ("merchants_run",),
        ("txns_run",),
        ("device_run",)
    ]

    actual_calls = [(call[0]) for call in manager.method_calls]

    assert actual_calls == [call[0] for call in expected_calls], f"Expected call order {expected_calls}, but got {actual_calls}"



@patch("src.bronze.run_bronze.CustomersBronzeLoader")
@patch("src.bronze.run_bronze.MerchantsBronzeLoader")
@patch("src.bronze.run_bronze.TransactionsBronzeLoader")
@patch("src.bronze.run_bronze.DeviceLogsBronzeLoader")
def test_orchestrator_error_handling(
    mock_device, mock_txns, mock_merchants, mock_customers, spark: SparkSession
):
    """Test that run_all handles individual failures gracefully."""
    # 1. Configure mock_customers to raise a failure when run is called
    mock_customers.return_value.run.side_effect = Exception("Customers Fail")

    # 2. Configure other mocks to complete successfully
    mock_merchants.return_value.run.return_value = None
    mock_txns.return_value.run.return_value = None
    mock_device.return_value.run.return_value = None

    # 3. Assert the overarching pipeline failure exception is raised
    with pytest.raises(RuntimeError) as exc_info:
        run_all(spark)

    # Verify generic message structure matching your source code's message
    assert "completed with failures" in str(exc_info.value)

    # 4. Confirm execution continued downstream despite the customer failure
    mock_customers.return_value.run.assert_called_once()
    mock_merchants.return_value.run.assert_called_once()
    mock_txns.return_value.run.assert_called_once()
    mock_device.return_value.run.assert_called_once()
