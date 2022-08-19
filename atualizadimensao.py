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
     schedule_interval='00 14 * * *',
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
        cur.execute(""" delete from iceberg.dimension."produto_fornecedor" """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."produto_fornecedor" select * from solo_operacional.dbo.produto_fornecedor """)
        cur.fetchall()

    @task(task_id = 'atualiza_prd_AD')
    def atualiza_prd_AD():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.dimension.produto_parceria """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."produto_parceria" select * from solo_operacional.dbo.produto_parceria """)
        cur.fetchall()

    @task(task_id = 'atualiza_cnpj_fornecedor')
    def atualiza_cnpj_fornecedor():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.dimension.fornecedor """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."fornecedor" select * from solo_operacional.dbo.fornecedor """)
        cur.fetchall()

    @task(task_id = 'atualiza_cnpj_ad')
    def atualiza_cnpj_ad():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.dimension.distribuidor """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."distribuidor" select * from solo_operacional.dbo.distribuidor """)
        cur.fetchall()

    @task(task_id = 'atualiza_produto_kit')
    def atualiza_produto_kit():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.dimension.ctr_produto_kit """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."ctr_produto_kit" select * from solo_operacional.dbo.ctr_produto_kit """)
        cur.fetchall()


    @task(task_id = 'atualiza_produto_interno')
    def atualiza_produto_interno():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.dimension.ctr_produto_interno """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."ctr_produto_interno" select * from solo_operacional.dbo.ctr_produto_interno """)
        cur.fetchall()

    @task(task_id = 'atualiza_produto_ean')
    def atualiza_produto_ean():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" delete from iceberg.dimension.ctr_produto_ean """)
        cur.fetchall()
        cur.execute(""" insert into iceberg.dimension."ctr_produto_ean" select * from solo_operacional.dbo.ctr_produto_ean """)
        cur.fetchall()

    @task(task_id = 'envia_email')
    def envia_email():
        """
        #### Executa script no trino
        """
    print ("Sucesso")


    [atualiza_prd_Fornecedor(),atualiza_prd_AD(),atualiza_cnpj_fornecedor(), atualiza_cnpj_ad(), atualiza_produto_kit(),atualiza_produto_interno(), atualiza_produto_ean()] >> envia_email()

carga_trino_dag = carga_trino()



