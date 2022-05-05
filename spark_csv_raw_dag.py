# internal library
from datetime import datetime
import time
import random
import psycopg2
import os

from airflow.decorators import dag, task
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.exceptions import AirflowException

############################################################
# Default DAG arguments
############################################################
DATABASE = os.getenv('PG_DATABASE', 'mtrix_metadata')
USER = os.getenv('PG_USER', 'postgres')
PASSWORD = os.getenv('PASSWORD_PG', 'dbagnx2010@')
HOST = os.getenv('PG_HOST', 'metadata-postgres.mtrix-postgres')
PORT = os.getenv('PG_PORT', '5432')

default_args = {
    "owner": "mtrix",
    "depends_on_past": False,
    "email": "mmedeiros@mtrix.com.br", # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": False,
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
    @task(task_id = 'spark_csv_raw')
    def spark_csv_raw(ds=None, **kwargs):
        """
        #### Submit Job Spark CSV -> Raw
        """
        dag_run = kwargs.get('dag_run')
        message = dag_run.conf['message']
        error_message = ""

        print("Parametro", dag_run.run_id)

        try:
            job = LivyOperator(task_id = 'spark_job_raw',file='local:/app/processar.py', args=[message], polling_interval=0) # "--args1",

            job.execute('spark_job_raw')
            status = 'success'
        except Exception as e:
            error_message = e
            status = 'failed'
        except AirflowException as e:
            error_message = e
            status = 'failed'
        finally:
            ts = datetime.now()

            if status == 'success':
                process = 'S'
            else:
                process = 'E'

            param = {
                    "status": status,
                    "process_date": str(ts),
                    "process": process,
                    "instance_name": dag_run.run_id,
                    "error_message": error_message
                    }

        return param

    @task(task_id = 'update_metadata')
    def update_metadata(param: dict):
        """
        #### Executa script no trino
        """
        conn = psycopg2.connect(database=DATABASE,user=USER,password=PASSWORD,host=HOST,port=PORT)
        with conn:
           cursor = conn.cursor()

           query = """UPDATE ctr_mensagem_kafka
                       SET ic_status = '{}'
                          ,dt_processamento = '{}'
                          ,ic_processado = '{}'
                          ,erro_airflow = '{}'
                     WHERE nome_instancia = '{}'""".format(param["status"],param["process_date"],param["process"],param["error_message"],param["instance_name"])
           cursor.execute(query)

    t1 = spark_csv_raw()
    update_metadata(t1)

spark_csv_raw_dag = spark_job_csv()