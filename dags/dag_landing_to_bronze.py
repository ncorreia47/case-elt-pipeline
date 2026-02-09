from airflow.sdk import dag, task, task_group
from datetime import datetime

DBT_PROJECT_DIR = "/opt/airflow/dbt/elt_data_pipeline"

@dag(
    dag_id="dag_landing_to_bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags = ['data-engineering', 'dbt', 'bronze']
)


def landing_to_bronze():

    @task.bash(cwd=DBT_PROJECT_DIR)
    def setup_dbt():
        return "dbt deps"

    @task_group(group_id="bronze_layer")
    def bronze_layer():
        
        @task.bash(cwd=DBT_PROJECT_DIR)
        def groups():
            return "dbt run --models bronze.groups"
        
        @task.bash(cwd=DBT_PROJECT_DIR)
        def users():
            return "dbt run --models bronze.users"
        
        @task.bash(cwd=DBT_PROJECT_DIR)
        def organizations():
            return "dbt run --models bronze.organizations"
        
        groups()
        users()
        organizations()

    setup_dbt() >> bronze_layer()

landing_to_bronze()