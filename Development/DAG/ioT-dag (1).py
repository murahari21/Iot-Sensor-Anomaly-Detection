from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import boto3

def task_fail_slack_alert(context):
    slack_msg = f"""
            🔴 *Task Failed!*
            *Task*: {context.get('task_instance').task_id}  
            *Dag*: {context.get('task_instance').dag_id} 
            *Log Url*: {context.get('task_instance').log_url}
            """
    alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack_conn',
        message=slack_msg)
    return alert.execute(context=context)

# Default args
default_args = {
    'owner': 'varun_de',
    'start_date': datetime(2026, 3, 16),
    'on_failure_callback': task_fail_slack_alert, # <--- Add this line
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Streams as defined in your project
streams = [
    "environment-stream",
    "health-stream",
    "operations-stream",
    "sensor-stream",
    "time-stream"
]

# AWS configuration
AWS_REGION = "ap-south-1"
FIREHOSE_ROLE_ARN = "arn:aws:iam::992382588438:role/airflow-role-firehose"
S3_BUCKET_ARN = "arn:aws:s3:::iot-anomaly-pipeline-parquet"

def create_kinesis(stream_name, region_name=AWS_REGION):
    client = boto3.client("kinesis", region_name=region_name)
    existing_streams = client.list_streams()["StreamNames"]
    if stream_name not in existing_streams:
        client.create_stream(StreamName=stream_name, ShardCount=1)
        print(f"Kinesis stream {stream_name} created")
    else:
        print(f"Kinesis stream {stream_name} already exists")

def create_firehose(stream_name, delivery_stream_name, region_name=AWS_REGION):
    client = boto3.client("firehose", region_name=region_name)
    existing_streams = client.list_delivery_streams()["DeliveryStreamNames"]
    if delivery_stream_name not in existing_streams:
        client.create_delivery_stream(
            DeliveryStreamName=delivery_stream_name,
            S3DestinationConfiguration={
                "RoleARN": FIREHOSE_ROLE_ARN,
                "BucketARN": S3_BUCKET_ARN,
                "Prefix": f"{stream_name}/"
            }
        )
        print(f"Firehose delivery stream {delivery_stream_name} created")
    else:
        print(f"Firehose delivery stream {delivery_stream_name} already exists")

with DAG(
    dag_id="IoT-sensor-anomaly-pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    wait_for_s3_files = S3KeySensor(
        task_id="wait_for_s3_files",
        bucket_name="iot-pipeline-raw",
        bucket_key="*.csv",
        wildcard_match=True,
        aws_conn_id="aws-airflow",
        poke_interval=60,
        timeout=600
    )

    kinesis_tasks = []
    firehose_tasks = []

    for stream in streams:
        # Task IDs must strictly match what Airflow expects
        k_task_id = f"create_kinesis_{stream.replace('-', '_')}"
        f_task_id = f"create_firehose_{stream.replace('-', '_')}"

        k_task = PythonOperator(
            task_id=k_task_id,
            python_callable=create_kinesis,
            op_args=[stream]
        )

        # Create Firehose Task in your DAG
        firehose_name = stream.replace("-stream", "-firehose")

        f_task = PythonOperator(
            task_id=f"create_firehose_{stream.replace('-', '_')}",
            python_callable=create_firehose,
            op_args=[stream, firehose_name]
        )

        k_task >> f_task
        kinesis_tasks.append(k_task)
        firehose_tasks.append(f_task)

    run_databricks = DatabricksRunNowOperator(
        task_id="run_databricks",
        databricks_conn_id="databricks-airflow",
        job_id=1096925561587618
    )

    wait_for_s3_files >> kinesis_tasks
    for f in firehose_tasks:
        f >> run_databricks