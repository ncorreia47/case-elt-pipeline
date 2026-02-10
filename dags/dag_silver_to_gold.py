from airflow.sdk import dag, task, task_group, Asset
from airflow.models.param import Param
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt/elt_data_pipeline"
AIRFLOW_ASSET = Asset('dbt_bronze_to_silver')

@dag(
    dag_id="dag_silver_to_gold",
    start_date=datetime(2025, 1, 1),
    schedule=[AIRFLOW_ASSET],
    catchup=False,
    params={
        "selector": Param(
            default="tag:gold",
            type="string",
            enum=[
                "tag:gold",
                "tag:tickets_gold",
                "tag:users_gold",
                "tag:organizations_gold"
            ],
            description="Selecione o conjunto de modelos dbt para executar"
        )
    },
    tags = ['data-engineering', 'dbt', 'gold']
)


def bronze_to_silver():

    @task.bash(cwd=DBT_PROJECT_DIR)
    def setup_dbt():
        return "dbt deps"

    @task_group(group_id="silver_layer")
    def silver_layer():
        
        @task.bash(cwd=DBT_PROJECT_DIR)
        def run_silver(params=None):
            return f"dbt build --select {params['selector']}"
        
        run_silver()
        
        
    setup_dbt() >> silver_layer()

bronze_to_silver()