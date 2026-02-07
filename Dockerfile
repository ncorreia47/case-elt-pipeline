FROM apache/airflow:3.0.0-python3.11 

USER airflow 

# Instala o dbt-core e o adaptador do PostgreSQL
RUN pip install --no-cache-dir dbt-core dbt-postgres 

# Configura o airflow para rodar em modo standalone
ENTRYPOINT [ "airflow", "standalone" ]