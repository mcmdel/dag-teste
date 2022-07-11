import os
import psycopg2
import urllib.parse
# internal library
from datetime import datetime

from airflow.decorators import dag, task
#from airflow.providers.trino.hooks.trino import TrinoHook

import trino

DATABASE = os.getenv('PG_DATABASE', 'mtrix_metadata')
USER = os.getenv('PG_USER', 'postgres')
PASSWORD = os.getenv('PASSWORD_PG', 'dbagnx2010@')
HOST = os.getenv('PG_HOST', 'metadata-postgres.mtrix-postgres')
PORT = os.getenv('PG_PORT', '5432')
URL  = """postgresql+psycopg2://{}:{}@{}:{}/{}""".format(USER,urllib.parse.quote(PASSWORD),HOST,PORT,DATABASE)

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
     dag_id= 'carga_Staging_Venda',
     description= 'Execução script no trino',
     start_date=datetime(2022, 4, 18),
     schedule_interval='00 14 * * *',
     catchup=False,
     default_args= default_args,
     tags=['carga', 'Atualizacao', 'Dimensao'],
)



# cria dataframe com os arquivos
def captura_arq_envio():
    pass 

def atualiza_metadado():
    pass 


def carga_venda():
    """
    ### Execução de spark job
    """
    @task(task_id = 'atualiza_prd_Fornecedor')
    def atualiza_prd_Fornecedor():
        """
        #### Executa script no trino
        """
        cur = conn.cursor()
        cur.execute(""" insert into solo_stagingarea.dbo."stg_venda_dcp"
                        select *    
                            from iceberg.raw.stg_venda 
                        where cd_geracao_arquivo = {}""")
        cur.fetchall()

  
    print ("Sucesso")


    atualiza_prd_Fornecedor()

carga_trino_dag = carga_venda()



