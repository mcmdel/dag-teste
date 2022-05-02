# internal library
from datetime import datetime
import time
import random
import trino

from airflow.decorators import dag, task

############################################################
# Default DAG arguments
############################################################
conn = trino.dbapi.connect(
    host='trino.warehouse',
    port=8080,
    user='admin',
    catalog='metadata',
    schema='public',
)

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
    @task(task_id = 'spark_csv_raw')
    def spark_csv_raw(ds=None, **kwargs):
        """
        #### Submit Job Spark CSV -> Raw
        """
        dag_run = kwargs.get('dag_run')
        message = dag_run.conf['message']
        dg_run  = dag_run['dag_run_id']

        print("Pametro", dg_run)

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
                  "instance_name": dag_run.dag_run_id
                }
        return param

    @task(task_id = 'update_metadata',multiple_outputs=True)
    def update_metadata(param: dict):
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute("""UPDATE ctr_mensagem_kafka
                       SET ic_status = '{}',
                           dt_processamento = '{}',
                           ic_processado = '{}'
                     WHERE cd_geracao_arquivo = '{}'""".format(param["status"],param["process_date"],param["process"],param["instance_name"]))
        cur.fetchall()

    t1 = spark_csv_raw()
    update_metadata(t1)

spark_csv_raw_dag = spark_job_csv()