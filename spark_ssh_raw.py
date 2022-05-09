import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


dag = DAG(dag_id = "spk",
          description='filer',
          start_date = datetime(2022, 4, 18),
          catchup=False,
          default_args= default_args,
          tags=['simulation'],
         )

templated_bash_command = """
            cd {{ params.project_source }}
            {{ params.spark_submit }} 
            """

t1 = SSHOperator(
       task_id="SSH_task",
       ssh_conn_id='spark_21',
       command=templated_bash_command,
       dag=dag
       )