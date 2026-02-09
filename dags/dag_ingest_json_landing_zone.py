import os
import glob
import json
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parent.parent
BUCKET_DIR = f'{BASE_DIR}/bucket/'


@dag(
    dag_id="dag_ingest_json_landing_zone",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags = ['data-engineering', 'api', 'postgres', 'landing_zone']
)


def ingest_json_landing_zone():

    @task
    def create_landing_schema():

        hook = PostgresHook(postgres_conn_id="postgres")
        sql = """
              CREATE SCHEMA IF NOT EXISTS elt_data_pipeline_landing;
              
              CREATE TABLE IF NOT EXISTS elt_data_pipeline_landing.api_files_landing (
                  endpoint text,
                  file_path text,
                  json_file_datetime text,
                  payload jsonb
                );
              """
        hook.run(sql)
        return "Schema validado/criado."
    

    @task
    def list_bucket_files():

        search_pattern = os.path.join(BUCKET_DIR, "**/*.json")
        files = glob.glob(search_pattern, recursive=True)
        return files
    

    @task
    def load_json_to_postgres(file_path):

        hook = PostgresHook(postgres_conn_id="postgres") 
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Extracao dos parametros na estrutura de pastas sugerida: /endpoint/YYYY/MM/file.json
        path_obj = Path(file_path)
        relative_path = path_obj.relative_to(BUCKET_DIR)
        parts = relative_path.parts
        endpoint = parts[0]
        json_file_datetime = path_obj.stem.split("_")[-1]


        sql = """
            INSERT INTO elt_data_pipeline_landing.api_files_landing
            (endpoint, file_path, json_file_datetime, payload)
            values (%s, %s, %s, %s)
        """

        hook.run(sql, parameters=(endpoint, file_path, json_file_datetime, json.dumps(data)))
        return f"Arquivo {file_path} processado com sucesso."
    
    create_landing_schema = create_landing_schema()
    files = list_bucket_files()
    create_landing_schema >> load_json_to_postgres.expand(file_path=files)

ingest_json_landing_zone()