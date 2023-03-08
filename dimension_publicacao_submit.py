#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

# """
# This is an example DAG which uses SparkKubernetesOperator and SparkKubernetesSensor.
# In this example, we create two tasks which execute sequentially.
# The first task is to submit sparkApplication on Kubernetes cluster(the example uses spark-pi application).
# and the second task is to check the final state of the sparkApplication that submitted in the first state.
# Spark-on-k8s operator is required to be already installed on Kubernetes
# https://github.com/GoogleCloudPlatform/spark-on-k8s-operator
# """

import os
from datetime import datetime, timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

# [END import_module]


# [START instantiate_dag]

DAG_ID = "stg_dimension_publicacao_submit"

with DAG(
    DAG_ID,
    default_args={"max_active_runs": 1},
    description="submit ingest Dimension Publicacao as sparkApplication on kubernetes",
    schedule_interval='30 3 * * *',
    start_date=datetime(2022, 12, 19),
    tags=['dimension'],
    catchup=False,
) as dag:
    # [START SparkKubernetesOperator_DAG]
    t1 = SparkKubernetesOperator(
        task_id="dimension_pub_submit",
        namespace="spark-jobs",
        application_file="/yaml_gcp/spark-dimensions-pub-cdl.yaml",
        #kubernetes_conn_id = "k8shomolog",
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(
        task_id="dimension_pub_monitor",
        namespace="spark-jobs",
        application_name="{{ task_instance.xcom_pull(task_ids='dimension_pub_submit')['metadata']['name'] }}",
        dag=dag,
    )
    t1 >> t2