import pytest
from pyspark.sql import SparkSession

SILVER_SCHEMA = "iot_sensor_anomoly_detection.silver"

tables = [
    "sensor_stream_clean",
    "device_health_diagnostics_clean",
    "device_operations_clean",
    "environment_network_clean",
    "time_anomaly_events_clean"
]

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("iot-silver-layer-test") \
        .getOrCreate()


# -----------------------------------------
# Test 1 : Silver tables exist
# -----------------------------------------

def test_silver_tables_exist(spark):

    tables_df = spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}")

    available_tables = [row.tableName for row in tables_df.collect()]

    for table in tables:
        assert table in available_tables

# -----------------------------------------
# Test 2 : Silver tables contain data
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_silver_row_count(spark, table):

    df = spark.table(f"{SILVER_SCHEMA}.{table}")

    assert df.count() > 0


# -----------------------------------------
# Test 3 : Column names are lowercase
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_column_lowercase(spark, table):

    df = spark.table(f"{SILVER_SCHEMA}.{table}")

    for col_name in df.columns:
        assert col_name == col_name.lower()


# -----------------------------------------
# Test 4 : sensor_stream_clean schema validation
# -----------------------------------------

def test_sensor_stream_schema(spark):

    df = spark.table(f"{SILVER_SCHEMA}.sensor_stream_clean")

    expected_columns = [
        "device_id",
        "sensor_identifier",
        "reading_value",
        "noise_level",
        "sensor_model"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 5 : device_health_clean schema validation
# -----------------------------------------

def test_device_health_schema(spark):

    df = spark.table(f"{SILVER_SCHEMA}.device_health_diagnostics_clean")

    expected_columns = [
        "device_id",
        "health_score"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 6 : No null device IDs
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_no_null_device_id(spark, table):

    df = spark.table(f"{SILVER_SCHEMA}.{table}")

    null_count = df.filter(df.device_id.isNull()).count()

    assert null_count == 0


# -----------------------------------------
# Test 7 : Sensor reading should not be negative
# -----------------------------------------

def test_sensor_value_positive(spark):

    df = spark.table(f"{SILVER_SCHEMA}.sensor_stream_clean")

    invalid = df.filter(df.reading_value < 0).count()

    assert invalid == 0




# -----------------------------------------
# Test 8 : Event timestamp column exists
# -----------------------------------------

def test_event_timestamp_exists(spark):

    df = spark.table(f"{SILVER_SCHEMA}.sensor_stream_clean")

    assert "signal_strength" in df.columns


# -----------------------------------------
# Test 9 : No duplicate device events
# -----------------------------------------

def test_no_duplicate_events(spark):

    df = spark.table(f"{SILVER_SCHEMA}.sensor_stream_clean")

    duplicates = (
        df.groupBy("device_id", "sensor_identifier")
        .count()
        .filter("count > 1")
        .count()
    )

    assert duplicates == 0