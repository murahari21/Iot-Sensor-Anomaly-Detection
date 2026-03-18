import pytest
from pyspark.sql import SparkSession

BRONZE_SCHEMA = "iot_sensor_anomoly_detection.bronze"

tables = [
    "device_health_diagnostics",
    "device_operations",
    "environment_network",
    "sensor_stream",
    "time_anomaly_events"
]

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("iot-bronze-layer-test") \
        .getOrCreate()


# -----------------------------------------
# Test 1 : Bronze tables exist
# -----------------------------------------

def test_bronze_tables_exist(spark):

    tables_df = spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}")

    for table in tables:
        assert tables_df.filter(tables_df.tableName == table).count() == 1


# -----------------------------------------
# Test 2 : Bronze tables have data
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_bronze_row_count(spark, table):

    df = spark.table(f"{BRONZE_SCHEMA}.{table}")

    assert df.count() > 0


# -----------------------------------------
# Test 3 : Column names are lowercase
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_column_lowercase(spark, table):

    df = spark.table(f"{BRONZE_SCHEMA}.{table}")

    for col_name in df.columns:
        assert col_name == col_name.lower()


# -----------------------------------------
# Test 4 : sensor_stream required columns
# -----------------------------------------

def test_sensor_stream_columns(spark):

    df = spark.table(f"{BRONZE_SCHEMA}.sensor_stream")

    expected_columns = [
        "device_id",
        "sensor_identifier",
        "reading_value"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 5 : device_health_diagnostics columns
# -----------------------------------------

def test_device_health_columns(spark):

    df = spark.table(f"{BRONZE_SCHEMA}.device_health_diagnostics")

    expected_columns = [
        "device_id",
        "health_score"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 6 : device_operations columns
# -----------------------------------------

def test_device_operations_columns(spark):

    df = spark.table(f"{BRONZE_SCHEMA}.device_operations")

    expected_columns = [
        "device_id",
        "device_temp"
    ]

    for col in expected_columns:
        assert col in df.columns