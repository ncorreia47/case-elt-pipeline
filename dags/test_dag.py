from airflow.sdk import dag, task 
from datetime import datetime 

@dag(
    dag_id = "airflow_installation_validator",
    schedule = None,
    start_date = datetime(2025, 1, 1),
    catchup = False,
    tags = ['data-engineering', 'tests']
)

def validator():
    
    @task
    def check_configurations():
        print('The configuration has been created successfully.')
        return 'Success'
    
    check_configurations()

validator()