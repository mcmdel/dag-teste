# internal library
from datetime import datetime
import time
import random
import psycopg2
import os

from airflow.decorators import dag, task

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

        print("Pametro", dag_run.run_id)

        #Pametro <DagRun spark_csv_raw_dag @ 2022-05-02 21:45:08.432816+00:00: csv_to_raw-0-020220502T184508432630, externally triggered: True>

        # Lancar spark Job via spark-submit e verificar o retorno
        # Loop para verificar via Rest API Spark quando o Job concluir (success / Failt)
        # URL = http://<server-url>:18080/api/v1/applications/[app-id]/jobs?status=[active|complete|pending|failed]
        #

        # Loop para verificar via Rest API Spark quando o Job concluir (success / Failt)
        # URL = http://<server-url>:18080/api/v1/applications/[app-id]/jobs?status=[active|complete|pending|failed]
        #

        # Retornar resultado do processamento

        faixa = random.choice([1,1,1,1,1,2,2,2,2,2,3,3,3,3,3,4,4,4,4,4,5,5,5,5,5,5,5,5,6,7,20,30,60,75,80,100,240])
        tempo = random.randint(0, faixa)

        time.sleep(tempo)
        print(f'Parameter = {message}')

        param = {
                  "status": "success",
                  "process_date": str(datetime.now()),
                  "process": "S",
                  "instance_name": dag_run.run_id
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
                       SET ic_status = '{}',
                           dt_processamento = '{}',
                           ic_processado = '{}'
                     WHERE nome_instancia = '{}'""".format(param["status"],param["process_date"],param["process"],param["instance_name"])
           cursor.execute(query)

    t1 = spark_csv_raw()
    update_metadata(t1)

spark_csv_raw_dag = spark_job_csv()