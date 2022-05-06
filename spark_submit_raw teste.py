from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

pyspark_app_home=Variable.get("PYSPARK_APP_HOME")

#local_tz = pendulum.timezone("Ásia/Teerã")
default_args = {
    'owner': 'mtrix',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 6),
    'email': ['nematpour.ma@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True , 'retries'
    : 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='teste_executa_spark',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

flight_search_ingestion= SparkSubmitOperator(task_id='teste_servico',
conn_id='spark_local',#spark_default
application=f'{pyspark_app_home}/processar.py',
total_executor_cores=4,
#packages="io.delta:delta-core_2.12:0.7.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0",
executor_cores=2,
executor_memory='1g',
driver_memory='1g',
name='teste_servico',
execution_timeout=timedelta(minutes=10),
dag=dag
)

flight_search_ingestion