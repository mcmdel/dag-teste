# internal library
from datetime import datetime
import time
import random

from airflow.decorators import dag, task

############################################################
# Default DAG arguments
############################################################

default_args = {
    "owner": "mtrix",
    "depends_on_past": False,
    "email": "mmedeiros@mtrix.com.br", # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": True,
    "email_on_retry": False
}

@dag(
     dag_id= 'spark_csv_raw_dag',
     description= 'Execução do Spark JOB para carga de dados na Raw',
     start_date=datetime(2022, 4, 18),
     catchup=False,
     default_args= default_args,
     tags=['simulation'],
)
def spark_job_csv():
    """
    ### Execução de spark job
    """
    @task()
    def spark_csv_raw(ds=None, **kwargs):
        """
        #### Submit Job Spark CSV -> Raw
        """
        dag_run = kwargs.get('dag_run')
        message = dag_run.conf['message']

        print(message)

        # faixa = random.choice([2,5,7,20,30,60,75,80,100,240])
        # tempo = random.randint(1, faixa)

        # time.sleep(tempo)

        return True
    service_spark = spark_csv_raw()

spark_csv_raw_dag = spark_job_csv()