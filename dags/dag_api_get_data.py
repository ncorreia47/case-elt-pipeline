import sys
import os
from airflow.sdk import dag, task, task_group
from datetime import datetime
from api.api_get_data import api_get_data
from api.enum_endpoints import Endpoints

@dag(
    dag_id = "dag_api_get_data",
    schedule = None,
    start_date = datetime(2025, 1, 1),
    catchup = False,
    tags = ['data-engineering', 'api', 'ingest']
)


def create_api_tasks_dag():

    # Parametro dinâmico para cargas incrementais e reprocessamentos
    #start_time_param = "{{ dag_run.conf['start_time'] if dag_run.conf and dag_run.conf.get('start_time') else None }}"

    @task_group(group_id="get_data_group")
    def get_data_group():

        # Iterar sobre todos os endpoints e criar uma task para cada um
        for endpoint in Endpoints:
            
            # Definir task_id dinamicamente com base no endpoint
            task_id = f"get_data_{endpoint.value}"

            @task(task_id=task_id)
            def get_data(endpoint: Endpoints):
                print(f"Iniciando a coleta de dados do endpoint {endpoint.value}")
                api_get_data(endpoint)
                return f"Dados coletados para o endpoint {endpoint.value} com sucesso!"

            get_data(endpoint)

    get_data_group()

dag_api_get_data = create_api_tasks_dag()
