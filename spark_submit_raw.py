from datetime import datetime

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='example_spark_operator',
    schedule_interval=None,
    start_date=datetime(2022, 5, 5),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_spark_submit]
    submit_job = SparkSubmitOperator(
        application="/opt/bitnami/spark/examples/src/main/python/pi.py", task_id="submit_job"
    )