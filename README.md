# 📡 IoT Sensor Anomaly Detection Pipeline

## 🚀 Project Overview

The **IoT Sensor Anomaly Detection Pipeline** is an end-to-end Data Engineering project designed to process real-time IoT sensor data, detect anomalies, and generate actionable insights for monitoring and predictive maintenance.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** using the **Databricks Lakehouse platform**. Data is ingested from AWS services, processed using PySpark, stored in Delta Lake tables, and used for dashboards and alerting systems.

---

## 🎯 Project Objectives

- IOT devices generate large volumes of sensor data continuously, requiring efficient real-time processing and monitoring.
  
- Without automated systems, detecting abnormal sensor behavior is difficult, leading to device failures, downtime, and operational losses.
  
- This project builds a scalable data pipeline to enable real-time anomaly detection and proactive monitoring of IoT systems.
  
- The main objectives of this project are:
  
- Build a real-time streaming data pipeline using Amazon Kinesis.
  
- Implement Medallion Architecture (Bronze, Silver, Gold layers) in Databricks. 

- Clean and transform raw IoT sensor data.

- Apply anomaly detection techniques using Z-score.

- Provide real-time dashboards for monitoring sensor health. 

- Trigger Slack alerts for critical anomalies.

- This enables proactive monitoring and predictive maintenance.

---



## 🏗 System Architecture

The pipeline follows a modern Lakehouse Data Engineering Architecture.

![system architecture](https://github.com/user-attachments/assets/27ba320e-68c6-4a72-b6be-c888fb08272b)



---

## 🛠 Technology Stack

| Technology | Purpose |
|------------|--------|
| AWS S3 | Data Lake Storage |
| Amazon Kinesis | Real-time Data Streaming |
| AWS Data Firehose | Data Delivery |
| AWS Lambda | Stream Processing |
| Databricks | Data Engineering Platform |
| PySpark | Distributed Data Processing |
| Delta Lake | Storage Format |
| Unity Catalog | Data Governance |
| DBT | Advanced Transformations & Modelling|
| Apache Airflow | Pipeline Orchestration |
| Slack | Alert Notifications |
| Git | Version Control |

---

## 📂 Dataset

Dataset Used: **AnoML IoT Dataset (Kaggle)**

Includes:
- Device_health_diagnostics
 
- Device_Operations
  
- Environment_network
  
- Sensor_Stream
  
- Time_Anomoly_Events

Dataset Raw CSV Files Path: 
s3://iot-sensor-raw-anamoly/time_anomaly_events_dataset.csv

DataSets Parquet Files Path:
s3://iot-sensor-target/YYYY/MM/DD/NN/

---

## 🏗 Data Pipeline Layers

### 🥉 Bronze Layer – Raw Data Ingestion

- Ingested IoT dataset (CSV / streaming)  
- Stored raw data in AWS S3  
- Loaded into Delta tables  
- Preserved original schema  
- Added ingestion metadata

| Bronze Tables |
|---------------|
| sensor_stream |
| device_operations |
| environment_network |
| time_anomaly_events |
| device_health_diag |

---

### 🥈 Silver Layer – Data Cleaning & Transformation

- Removed null and invalid records  
- Standardized column formats  
- Performed data validation checks  
- Applied transformations using PySpark    
- Enriched IoT data  

| Silver Tables |
|---------------|
| sensor_stream_clean |
| device_operations_clean |
| environment_network_clean |
| time_anomaly_events_clean |
| device_health_clean |

---

### 🥇 Gold Layer – Analytics & Insights

- Created aggregated device metrics  
- Generated anomaly indicators (Normal, Warning, Critical)  
- Built monitoring datasets
- Calculated anomaly metrics (Z-score)
- Prepared data for dashboards  
- Enabled real-time anomaly tracking  

![star schema](https://github.com/user-attachments/assets/19506dd0-fd22-467c-90e5-dec112b62f8d)


| Gold Tables |
|-------------|
| dim_device |
| dim_location |
| dim_sensor |
| dim_time |
| fact_device_monitoring |

---

## 📊 Business Insights

### Descriptive Analytics
- Sensor readings trend  
- Device activity monitoring  
- Total anomaly events  

### Diagnostic Analytics
- Device health vs anomalies  
- Downtime analysis  
- Network performance impact  

### Advanced Analytics
- Critical device identification  
- Anomaly pattern detection  
- Device performance trends  

---

## 📊 Dashboards

- Critical Alerts

- Critical Device Monitoring

- Sensor Status Distribution

- Sensor Reading Trend

- Anomoly detection Pattern By Time

![Total Critical Alerts](https://github.com/user-attachments/assets/a45ca803-e679-4cff-9d36-789be9760cdf)

![Anomoly Detection Pattern By Time](https://github.com/user-attachments/assets/6e4e758a-ff42-49bc-8074-f3a5e789cb3d)

![Sensor Status Distribution](https://github.com/user-attachments/assets/ea1f5d63-e5d9-47e0-b3e5-2f06dcf55e1f)

![Critical Device Monitoring](https://github.com/user-attachments/assets/a82d2dbe-8c69-4830-9dc6-ec75cb283786)

![Sensor Reading Trend](https://github.com/user-attachments/assets/97979f63-8d54-495b-8052-7e4986995586)




---

## 🔄 Pipeline Orchestration

The pipeline execution is automated using Apache Airflow and Databricks Workflows.

Schedule:
Continuous Streaming / Micro-batch Processing (every 10 seconds)

Workflow tasks include:

1. Data ingestion from AWS Kinesis / S3
2. Bronze layer processing (raw data storage)
3. Silver layer transformation (cleaning & validation)
4. Gold layer processing (aggregation & anomaly detection)
5. Data quality checks and validation
6. Real-time anomaly detection using Z-score
7. Slack alert notifications for critical anomalies
8. Logging pipeline execution and monitoring

---

## ⚠ Alerts, Monitoring & Logging

- Slack alerts for critical anomalies  
- Airflow monitoring for pipeline runs  
- Logging for debugging and tracking  
- Real-time anomaly monitoring  

---

## ✅ Data Quality & Testing

- PyTest validation for all layers  
- Schema validation  
- Row count checks  
- Duplicate detection  
- Data quality validation  

---

## 👨‍💻 My Role

Role: Data Engineer (Analytics / Testing / Monitoring)

- Developed Gold layer analytics tables and implemented anomaly detection logic (Z-score)
  
- Created aggregated metrics for device monitoring and real-time dashboards

- Performed data quality checks across Bronze, Silver, and Gold layers

- Validated schema, null values, and duplicate records using Pytest

- Integrated Slack alerts using Webhook API for anomaly and failure notifications

- Logged pipeline errors and monitored execution for reliability

- Ensured accuracy of analytics results and supported real-time monitoring

---

## 📈 Key Outcomes

- Real-time IoT anomaly detection  
- Reduced device failure risks  
- Scalable pipeline architecture  
- Analytics-ready datasets  

---

## 🔮 Future Enhancements

- Machine learning-based anomaly detection  
- Kafka-based streaming  
- Power BI dashboards  
- CI/CD for data pipelines  

---

## 📌 Conclusion

This project demonstrates a scalable **IoT anomaly detection pipeline** using modern Data Engineering tools and Lakehouse architecture, enabling real-time monitoring and proactive decision-making.
