# internal library
from datetime import datetime
import psycopg2
import os
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

############################################################
# Default DAG arguments
############################################################
DATABASE = os.getenv('PG_DATABASE', 'mtrix_metadata')
USER = os.getenv('PG_USER', 'postgres')
PASSWORD = os.getenv('PASSWORD_PG', 'dbagnx2010@')
HOST = os.getenv('PG_HOST', 'metadata-postgres.mtrix-postgres')
PORT = os.getenv('PG_PORT', '5432')

SPARK_REST_API = os.getenv('SPARK_REST_API', 'spark-cluster-master-svc.spark-cluster:6066')

default_args = {
    "owner": "mtrix",
    "depends_on_past": False,
    "email": "mmedeiros@mtrix.com.br", # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(
     dag_id= 'spark_csv_raw_dag_rest',
     description= 'Execução do Spark JOB para carga de dados na Raw',
     start_date=datetime(2022, 5, 9),
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
        start_date = datetime.now()

        dag_run = kwargs.get('dag_run')
        message = dag_run.conf['message']
        error_message = ""
        url = f"http://{SPARK_REST_API}/v1/submissions/create"

        message.replace("$","%24")

        print("Parametro", dag_run.run_id)

        try:
            payload = {
                       "appResource": "file:/app/processar.py",
                       "sparkProperties": {
                         "spark.app.name": "csv - raw",
                         "spark.submit.deployMode": "cluster"
                       },
                       "clientSparkVersion": "3.2.0",
                       "mainClass": "org.apache.spark.deploy.SparkSubmit",
                       "environmentVariables": {
                         "SPARK_ENV_LOADED": "1"
                       },
                       "action": "CreateSubmissionRequest",
                       "appArgs": [
                         "/app/processar.py",
                         message
                       ]
                      }

            response = requests.post(url, json=payload)

            json_response = response.json()

            success = str(json_response['success'])
            job_id = str(json_response['submissionId'])
            error_message = str(json_response)

            error_message = error_message.replace("{","")
            error_message = error_message.replace("}","")
            error_message = error_message.replace(":"," ")
            error_message = error_message.replace("\'","")
            error_message = error_message.replace("\"","")

        except Exception as e:
            error_message = e
            success = 'false'
            job_id = 'undefined'
        except AirflowException as e:
            error_message = e
            success = 'false'
            job_id = 'undefined'
        finally:
            if success == 'True':
                process = 'S'
            else:
                process = 'E'

            param = {
                        "status": 'submitted',
                        "process_date": str(start_date),
                        "process": process,
                        "instance_name": dag_run.run_id,
                        "job_id": job_id,
                        "error_message": error_message
                    }

        return param

    @task(task_id = 'update_metadata')
    def update_metadata(param: dict):
        """
        #### Update postgres
        """
        conn = psycopg2.connect(database=DATABASE,user=USER,password=PASSWORD,host=HOST,port=PORT)
        with conn:
           cursor = conn.cursor()

           query = """UPDATE ctr_mensagem_kafka
                         SET ic_status = '{}'
                            ,dt_processamento = '{}'
                            ,ic_processado = '{}'
                            ,erro_airflow = '{}'
                            ,job_id = '{}'
                       WHERE nome_instancia = '{}'""".format(param["status"],param["process_date"],param["process"],str(param["error_message"]),param["job_id"],param["instance_name"])
           cursor.execute(query)

    t1 = spark_csv_raw()
    update_metadata(t1)

spark_csv_raw_dag_rest = spark_job_csv()