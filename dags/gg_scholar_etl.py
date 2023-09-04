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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import (DbtCloudRunJobOperator)
from airflow.utils.task_group import TaskGroup
from dags.stage_gg_scholar_list import stage_gg_scholar_list
from dags.stage_gg_scholar_citations import stage_gg_scholar_citations
from dags.stage_gg_scholar_author_metrics import stage_gg_scholar_author_metrics
from dags.stage_gg_scholar_article import stage_gg_scholar_article
from dags.grant_privilage import grant_privilage_to_dbt_snow_user


#https://cloud.getdbt.com/#/accounts/{account_id}/projects/{project_id}/jobs/{job_id}/
#https://cloud.getdbt.com/deploy/194213/projects/283399/jobs/409890/

with DAG(
    dag_id="gg_scholar_etl",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": 194213},
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    with TaskGroup(group_id='load') as group_load:

        load_scholar_list = PythonOperator(
        task_id='load_scholar_list',
        python_callable=stage_gg_scholar_list,
        dag=dag,
        )

        load_scholar_citations = PythonOperator(
        task_id='load_scholar_citations',
        python_callable=stage_gg_scholar_citations,
        dag=dag,
        )

        load_author_metrics = PythonOperator(
        task_id='load_author_metrics',
        python_callable=stage_gg_scholar_author_metrics,
        dag=dag,
        )

        load_author_article = PythonOperator(
        task_id='load_author_article',
        python_callable=stage_gg_scholar_article,
        dag=dag,
        )

        grant_privilage = PythonOperator(
        task_id='grant_privilage',
        python_callable=grant_privilage_to_dbt_snow_user,
        dag=dag,
        )
        load_scholar_list >> load_scholar_citations >> load_author_metrics >> load_author_article >> grant_privilage

    dbt_cloud_transform_job = DbtCloudRunJobOperator(
        task_id="dbt_cloud_transform_job",
        job_id=409890,
        check_interval=10,
        timeout=300,
    )

    start >> group_load >> dbt_cloud_transform_job >> end
    #start >> group_load >> end

