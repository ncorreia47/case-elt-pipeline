from airflow.sdk import dag, task, task_group, Asset
from airflow.models.param import Param
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt/elt_data_pipeline"
AIRFLOW_ASSET = Asset('dbt_bronze_to_silver')
INGESTION_ASSET = Asset("api_raw_data")

@dag(
    dag_id="dag_bronze_to_silver",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "selector": Param(
            default="tag:silver",
            type="string",
            enum=[
                "tag:silver",
                "tag:tickets_silver",
                "tag:ticket_custom_fields_silver",
                "tag:ticket_metrics_silver",
                "tag:ticket_sla_events_silver",
                "tag:users_silver",
                "tag:organizations_silver",
                "tag:groups_silver"
            ],
            description="Selecione o conjunto de modelos dbt para executar"
        )
    },
    tags = ['data-engineering', 'dbt', 'silver']
)


def bronze_to_silver():

    @task.bash(cwd=DBT_PROJECT_DIR)
    def setup_dbt():
        return "dbt deps"

    @task_group(group_id="silver_layer")
    def silver_layer():
        
        @task.bash(cwd=DBT_PROJECT_DIR, outlets=[AIRFLOW_ASSET])
        def run_silver(params=None, triggering_asset_events=None):

            selected_tag = params['selector']
            dbt_vars = ""

            if triggering_asset_events and INGESTION_ASSET in triggering_asset_events:
               event = triggering_asset_events[INGESTION_ASSET]
               start_time = event.metadata.get("start_time")
               
               if start_time:
                   dbt_vars = f'--vars \'{{"manual_start_time": "{start_time}"}}\''
            return f"dbt run --select {selected_tag} {dbt_vars}"
        
        @task.bash(cwd=DBT_PROJECT_DIR)
        def test_silver(params=None):
            selected_tag = params['selector']
            return f"dbt test --select {selected_tag}"
        
        run_silver() >> test_silver()

    setup_dbt() >> silver_layer()

bronze_to_silver()