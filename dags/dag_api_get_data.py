import sys
import os
from airflow.sdk import dag, task, task_group,  Asset, Metadata
from airflow.models.param import Param
from datetime import datetime
from api.api_get_data import api_get_data
from api.enum_endpoints import Endpoints

INGESTION_ASSET = Asset("api_raw_data")

@dag(
    dag_id = "dag_api_get_data",
    schedule = None,
    start_date = datetime(2025, 1, 1),
    catchup = False,
    params={
        "start_time": Param(
            default=None,
            type=["string", "null"],
            description="Informe uma data no padrao yyyy-mm-dd para definir uma data de corte na extracao dos dados da API"
        )
    },
    tags = ['data-engineering', 'api', 'ingest']
)


def create_api_tasks_dag():

    @task_group(group_id="get_data_group")
    def get_data_group():

        # Iterar sobre todos os endpoints e criar uma task para cada um
        for endpoint in Endpoints:
            
            # Definir task_id dinamicamente com base no endpoint
            task_id = f"get_data_{endpoint.value}"

            @task(task_id=task_id)
            def get_data(endpoint: Endpoints, params=None):
                print(f"Iniciando a coleta de dados do endpoint {endpoint.value}")
                
                start_time_value = params["start_time"]
                start_time_value = datetime.strptime(start_time_value, '%Y-%m-%d').isoformat() if start_time_value else None

                api_get_data(endpoint, start_time_value)
                return f"Dados coletados para o endpoint {endpoint.value} com sucesso!"

            get_data(endpoint)
    
    @task(outlets=[INGESTION_ASSET])
    def finish_ingestion(params=None):
        st = params.get("start_time")
        yield Metadata(INGESTION_ASSET, {"start_time": st})

    get_data_group() >> finish_ingestion()

dag_api_get_data = create_api_tasks_dag()
