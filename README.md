# Iot-Sensor-Anomaly-Detection

This project implements a robust, real-time data engineering pipeline designed to stream IoT sensor data, detect anomalies using statistical methods, and provide actionable insights for predictive maintenance.

## Project Overview
The pipeline ingests high-velocity parquet data (temperature, humidity, etc.) from Amazon Kinesis, processes it through a Medallion Architecture (Bronze, Silver, Gold), and stores the results in Delta Lake for real-time monitoring.


Objective: Reduce equipment downtime and maintenance costs through early anomaly detection.

## Technical Stack
Orchestration: Databricks Workflows & Git Integration Stream Processing: PySpark & Amazon Kinesis Storage: Delta Lake (Unity Catalog) Data Source: Kaggle “AnoML-IoT” dataset Monitoring/Alerting: Slack & Databricks Logs 


## Pipeline Architecture (Medallion)
The data flows through three distinct layers within the iot_catalog:LayerTable NameDescriptionBronzeraw.sensor_readingsRaw JSON events ingested directly from Kinesis.Silverprocessed.valid_readingsCleaned, filtered, and parsed sensor data.Goldanalytics.device_statsAggregated metrics, z-score anomaly flags, and business logic.


## Implementation 
1. Environment SetupCluster Configuration: Deploy a High Concurrency cluster with 8–32 m5.xlarge workers.Library Installation: Ensure pyspark, delta-spark, and aws-kinesis-spark are available.Connectivity: Configure Amazon Kinesis streams and Unity Catalog permissions.


2. Data Ingestion (Bronze)Establish a continuous streaming connection to Kinesis with 10-second micro-batches.Parse JSON using a defined schema and handle corrupt records.Write raw data to the Bronze layer, partitioned by device_id and date.


3. Transformation & Cleansing (Silver)Filter out invalid records and perform data quality checks.Perform upserts (Merge) into Silver tables to maintain data integrity.Utilize checkpointing for fault-tolerant processing.


4. Anomaly Detection (Gold)Compute rolling metrics, such as 5-minute averages.Apply Z-Score logic to detect statistical outliers in sensor readings.Tag device statuses and calculate final aggregate metrics for the dashboard.


5. Quality Assurance & Alerts

Testing: Run unit tests for parsing logic and validate against batch Kaggle data.


Monitoring: Configure Slack notifications to trigger when high anomaly rates are detected.


Logging: All pipeline issues are logged to iot_catalog.logs.anomaly_log.



## Key Outcomes
Scalability: Efficiently handles high-velocity data using Kinesis and Delta Lake.Reliability: Maintains high data quality through rigorous filtering and error handling.Reproducibility: Version-controlled via GitHub for seamless team collaboration.
