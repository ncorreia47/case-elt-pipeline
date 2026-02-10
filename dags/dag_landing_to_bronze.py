from airflow.sdk import dag, task, task_group, Asset
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
                "tag:tickets_bronze",
                "tag:ticket_custom_fields_bronze",
                "tag:ticket_metrics_bronze",
                "tag:ticket_sla_events_bronze",
                "tag:users_bronze",
                "tag:organizations_bronze"
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
            selected_tag = params['selector']
            return f"dbt build --select {selected_tag}"
        
        run_bronze()
    
    @task
    def decide_next_tag(current_selector):
        # Dicionario que mapeia a entrada bronze para a saida silver
        mapping = {
            "tag:bronze": "tag:silver",
            "tag:tickets_bronze": "tag:tickets_silver",
            "tag:ticket_custom_fields_bronze": "tag:ticket_custom_fields_silver",
            "tag:ticket_metrics_bronze": "tag:ticket_metrics_silver",
            "tag:ticket_sla_events_bronze": "tag:ticket_sla_events_silver",
            "tag:users_bronze": "tag:users_silver",
            "tag:organizations_bronze": "tag:organizations_silver"
        }
        # Retorna o valor mapeado ou um padrao caso não encontre
        return mapping.get(current_selector, "tag:silver")

    next_tag = decide_next_tag("{{ params.selector }}")

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_dag",
        trigger_dag_id="dag_bronze_to_silver",
        conf={"selector": next_tag}, # Passa o resultado da função de mapeamento
    )

    setup_dbt() >> bronze_layer() >> next_tag >> trigger_silver

landing_to_bronze()