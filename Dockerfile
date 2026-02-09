FROM apache/airflow:3.0.0-python3.11 

USER root 

# Instala pacotes adicionais para habilitar dbt_deps e depois limpa o cache do apt na imagem docker
RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \ 
    && rm -rf /var/lib/apt/lists/*

USER airflow 

# Instala o dbt-core e o adaptador do PostgreSQL
RUN pip install --no-cache-dir dbt-core dbt-postgres

RUN mkdir -p /home/airflow/.dbt
COPY profiles.yml /home/airflow/.dbt/profiles.yml

ENV DBT_PROFILES_DIR=/home/airflow/.dbt

# Configura o airflow para rodar em modo standalone
ENTRYPOINT [ "airflow", "standalone" ]