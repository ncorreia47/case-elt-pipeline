from airflow.sdk import dag, task, task_group
from airflow.models.param import Param
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt/elt_data_pipeline"

@dag(
    dag_id="dag_landing_to_bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "selector": Param(
            default="tag:bronze",
            type="string",
            enum=[
                "tag:bronze",
                "tag:tickets",
                "tag:ticket_custom_fields",
                "tag:ticket_metrics",
                "tag:ticket_sla_events",
                "tag:users",
                "tag:organizations"
            ],
            description="Selecione o conjunto de modelos dbt para executar"
        )
    },
    tags = ['data-engineering', 'dbt', 'bronze']
)


def landing_to_bronze():

    @task.bash(cwd=DBT_PROJECT_DIR)
    def setup_dbt():
        return "dbt deps"

    @task_group(group_id="bronze_layer")
    def bronze_layer():
        
        @task.bash(cwd=DBT_PROJECT_DIR)
        def run_bronze(params=None):
            return f"dbt build --select {params['selector']}"
        
        run_bronze()
        
        
    setup_dbt() >> bronze_layer()

landing_to_bronze()