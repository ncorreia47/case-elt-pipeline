from airflow.sdk import dag, task, task_group, Asset
from airflow.models.param import Param
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt/elt_data_pipeline"
AIRFLOW_ASSET = Asset('dbt_bronze_to_silver')

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
        def run_silver(params=None):
            return f"dbt build --select {params['selector']}"
        
        run_silver()

    setup_dbt() >> silver_layer()

bronze_to_silver()