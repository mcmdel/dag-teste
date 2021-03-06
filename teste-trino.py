# internal library
from datetime import datetime

from airflow.decorators import dag, task
#from airflow.providers.trino.hooks.trino import TrinoHook

import trino

############################################################
# Default DAG arguments
############################################################

default_args = {
    "owner": "mtrix",
    "depends_on_past": False,
    "email": "mmedeiros@mtrix.com.br", # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(
     dag_id= 'carga_trino_dag',
     description= 'Execução script no trino',
     start_date=datetime(2022, 4, 18),
     catchup=False,
     default_args= default_args,
     tags=['carga'],
)
def carga_trino():
    """
    ### Execução de spark job
    """
    @task()
    def trino_script():
        """
        #### Executa script no trino
        """
        conn = trino.dbapi.connect(
            host='trino.warehouse',
            port=8080,
            user='admin',
            catalog='iceberg',
            schema='raw',
        )
        cur = conn.cursor()
        cur.execute(""" CREATE TABLE IF NOT EXISTS iceberg.raw."teste" (id integer,name varchar(50)) """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.raw."teste" values (1,'teste') """)
        cur.fetchall()
        # ts = TrinoHook()
        # sql = """ CREATE TABLE IF NOT EXISTS iceberg.raw."teste" (id int,name string); """
        # ts.run(sql)

    task_trino = trino_script()

carga_trino_dag = carga_trino()