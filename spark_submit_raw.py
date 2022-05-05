import airflow
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args = {
    'owner': 'mtrix',
    'start_date': datetime(2022, 5, 5),
    'retries': 3,
	  'retry_delay': timedelta(minutes=1)
}
with airflow.DAG('dag_spark_submit_raw',
                  default_args=default_args) as dag:
    task_elt_documento_pagar = SparkSubmitOperator(
        task_id='py_spark',
        conn_id='spark_default',
        application="./app/processar.py",
        application_args=["teste"], #caso precise enviar dados da dag para o job airflow utilize esta propriedade
        total_executor_cores=2,
        executor_memory="1g"
    )