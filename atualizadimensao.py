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
     dag_id= 'carga_dimensao_Mtrix',
     description= 'Execução script no trino',
     start_date=datetime(2022, 4, 18),
     catchup=False,
     default_args= default_args,
     tags=['carga', 'Atualizacao', 'Dimensao'],
)
def carga_trino():
    """
    ### Execução de spark job
    """
    @task(task_id = 'Atualiza_PRODUTO_FORNECEDOR')
    def atualiza_prd_Fornecedor():
        """
        #### Executa script no trino
        """
        conn = trino.dbapi.connect(
            host='trino.warehouse',
            port=8080,
            user='admin',
            catalog='iceberg',
            schema='trusted',
        )
        cur = conn.cursor()
        cur.execute(""" delete table iceberg.trusted."produto_fornecedor" """)
        cur.fetchall()
        # cur.execute(""" insert into iceberg.raw."teste" values (1,'teste') """)
        # cur.fetchall()

    @task(task_id = 'produto_parceria')
    def atualiza_prd_AD():
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
        cur.execute(""" delete table produto_parceria """)
        cur.fetchall()
        # cur.execute(""" insert into iceberg.raw."teste" values (1,'teste') """)
        # cur.fetchall()

    atualiza_prd_Fornecedor() >> atualiza_prd_AD()

carga_trino_dag = carga_trino()



