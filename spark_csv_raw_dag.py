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

        param = kwargs['param']
        param.xcom_push('status', 'success')
        param.xcom_push('process_date', str(datetime.datetime.now()))
        param.xcom_push('process', 'S')
        param.xcom_push('instance_name', dag_run.dag_run_id)

        return True

    @task(task_id = 'update_metadata')
    def update_metadata(ds=None, **kwargs):
        """
        #### Executa script no trino
        """
        param = kwargs['param']
        status = param.xcom_pull(task_ids='spark_csv_raw', key='status')
        process_date = param.xcom_pull(task_ids='spark_csv_raw', key='process_date')
        process = param.xcom_pull(task_ids='spark_csv_raw', key='process')
        instance_name = param.xcom_pull(task_ids='spark_csv_raw', key='instance_name')

        cur = conn.cursor()
        cur.execute("""UPDATE ctr_mensagem_kafka
                       SET ic_status = '{}',
                           dt_processamento = '{}',
                           ic_processado = '{}'
                     WHERE cd_geracao_arquivo = '{}'""".format(status,process_date,process,instance_name))
        cur.fetchall()

    spark_csv_raw() >> update_metadata


spark_csv_raw_dag = spark_job_csv()