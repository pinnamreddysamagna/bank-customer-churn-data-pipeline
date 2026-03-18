from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id="bank_churn_pipeline",
    start_date=datetime(2026, 3, 15),
    schedule_interval="@daily",
    catchup=False,
    tags=["bank", "churn", "data-pipeline"]
) as dag:

    # Step 1: Wait for CSV file in S3 raw folder
    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name="bank-customer-churn-data",
        bucket_key="raw/test.csv",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600
    )

    # Step 2: Trigger Glue ETL Job
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="Bank_churn_Parquet_Etl",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        retries=2
    )

    # Step 3: Trigger Databricks Job
    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="run_databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id=998178900316865
    )

    wait_for_s3_file >> run_glue_job >> run_databricks_pipeline