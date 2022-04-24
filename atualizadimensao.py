# internal library
from datetime import datetime

from airflow.decorators import dag, task
#from airflow.providers.trino.hooks.trino import TrinoHook

import trino


conn = trino.dbapi.connect(
    host='trino.warehouse',
    port=8080,
    user='admin',
    catalog='iceberg',
    schema='trusted',
)
############################################################
# Default DAG arguments
############################################################

default_args = {
    "owner": "mtrix",
    "depends_on_past": False,
    "email": "mmedeiros@mtrix.com.br;cbarbosa@mtrix.com.br", # Make sure you create the "email_monitoring" variable in the Airflow interface
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(
     dag_id= 'carga_dimensao_Mtrix',
     description= 'Execução script no trino',
     start_date=datetime(2022, 4, 18),
     schedule_interval="@daily",
     catchup=False,
     default_args= default_args,
     tags=['carga', 'Atualizacao', 'Dimensao'],
)
def carga_trino():
    """
    ### Execução de spark job
    """
    @task(task_id = 'atualiza_prd_Fornecedor')
    def atualiza_prd_Fornecedor():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.trusted."produto_fornecedor" """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.trusted."produto_fornecedor" select * from solo_operacional.dbo.produto_fornecedor """)
        cur.fetchall()

    @task(task_id = 'atualiza_prd_AD')
    def atualiza_prd_AD():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.trusted.produto_parceria """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.trusted."produto_parceria" select * from solo_operacional.dbo.produto_parceria """)
        cur.fetchall()

    @task(task_id = 'atualiza_cnpj_fornecedor')
    def atualiza_cnpj_fornecedor():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.trusted.fornecedor """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.trusted."fornecedor" select * from solo_operacional.dbo.fornecedor """)
        cur.fetchall()

    @task(task_id = 'atualiza_cnpj_ad')
    def atualiza_cnpj_ad():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.trusted.distribuidor """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.trusted."distribuidor" select * from solo_operacional.dbo.distribuidor """)
        cur.fetchall()

    @task(task_id = 'envia_email')
    def envia_email():
        """
        #### Executa script no trino
        """
    print ("Sucesso")


    [atualiza_prd_Fornecedor(),atualiza_prd_AD(),atualiza_cnpj_fornecedor(), atualiza_cnpj_ad()] >> envia_email()

carga_trino_dag = carga_trino()



