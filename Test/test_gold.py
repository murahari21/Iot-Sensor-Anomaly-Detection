import pytest
from pyspark.sql import SparkSession

GOLD_SCHEMA = "iot_sensor_anomoly_detection.gold"

tables = [
    "dim_device",
    "dim_sensor",
    "dim_region",
    "dim_health",
    "dim_time",
    "fact_device_monitoring"
]

# -----------------------------------------
# Spark Session Fixture
# -----------------------------------------

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("iot-gold-layer-test") \
        .getOrCreate()


# -----------------------------------------
# Test 1 : Gold tables exist
# -----------------------------------------

def test_gold_tables_exist(spark):

    tables_df = spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA}")

    for table in tables:
        assert tables_df.filter(tables_df.tableName == table).count() == 1


# -----------------------------------------
# Test 2 : Gold tables contain data
# -----------------------------------------

@pytest.mark.parametrize("table", tables)
def test_gold_row_count(spark, table):

    df = spark.table(f"{GOLD_SCHEMA}.{table}")

    assert df.count() > 0


# -----------------------------------------
# Test 3 : No duplicate keys in dimension tables
# -----------------------------------------

def test_dim_device_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_device")

    duplicates = df.groupBy("device_id").count().filter("count > 1").count()

    assert duplicates == 0


def test_dim_sensor_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_sensor")

    duplicates = df.groupBy("sensor_identifier").count().filter("count > 1").count()

    assert duplicates == 0


def test_dim_region_no_duplicates(spark):

    df = spark.table(f"{GOLD_SCHEMA}.dim_region")

    duplicates = df.groupBy("region_name").count().filter("count > 1").count()

    assert duplicates == 0


# -----------------------------------------
# Test 4 : Fact table schema validation
# -----------------------------------------

def test_fact_device_monitoring_columns(spark):

    df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")

    expected_columns = [
        "device_id",
        "sensor_identifier",
        "event_timestamp",
        "reading_value",
        "rolling_avg_reading",
        "z_score",
        "sensor_status",
        "health_score",
        "battery_voltage",
        "error_count",
        "restart_count",
        "anomaly_category",
        "severity_level",
        "downtime_minutes",
        "region_name",
        "network_provider"
    ]

    for col in expected_columns:
        assert col in df.columns


# -----------------------------------------
# Test 5 : Z-score column exists and not null
# -----------------------------------------

def test_zscore_not_null(spark):

    df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")

    null_count = df.filter(df.z_score.isNull()).count()

    assert null_count == 0


# -----------------------------------------
# Test 6 : Sensor status values valid
# -----------------------------------------

def test_sensor_status_values(spark):

    df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")

    valid_values = ["NORMAL", "WARNING", "CRITICAL"]

    invalid = df.filter(~df.sensor_status.isin(valid_values)).count()

    assert invalid == 0


# -----------------------------------------
# Test 7 : Downtime should not be negative
# -----------------------------------------

def test_downtime_positive(spark):

    df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")

    invalid = df.filter(df.downtime_minutes < 0).count()

    assert invalid == 0


# -----------------------------------------
# Test 8 : Fact table device_id exists in dim_device
# -----------------------------------------

def test_fact_device_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_device")

    missing = fact_df.join(
        dim_df,
        fact_df.device_id == dim_df.device_id,
        "left_anti"
    ).count()

    assert missing == 0


# -----------------------------------------
# Test 9 : Fact table sensor exists in dim_sensor
# -----------------------------------------

def test_fact_sensor_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_sensor")

    missing = fact_df.join(
        dim_df,
        fact_df.sensor_identifier == dim_df.sensor_identifier,
        "left_anti"
    ).count()

    assert missing == 0


# -----------------------------------------
# Test 10 : Fact table region exists in dim_region
# -----------------------------------------

def test_fact_region_fk(spark):

    fact_df = spark.table(f"{GOLD_SCHEMA}.fact_device_monitoring")
    dim_df = spark.table(f"{GOLD_SCHEMA}.dim_region")

    missing = fact_df.join(
        dim_df,
        fact_df.region_name == dim_df.region_name,
        "left_anti"
    ).count()

    assert missing == 0