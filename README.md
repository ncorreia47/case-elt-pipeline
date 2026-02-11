# Case ELT Pipeline

Um caso de uso de pipeline ELT (Extract, Load, Transform)** integrando **Apache Airflow** e **dbt (Data Build Tool)** para orquestração e transformação de dados.

> Este projeto demonstra como estruturar e rodar uma pipeline de dados com ferramentas modernas de engenharia de dados.


##  Sobre o Projeto

Este repositório contém um exemplo de fluxo ELT com:

- **Airflow** → orquestra os jobs e DAGs;
- **dbt** → realiza transformações de dados (modelagem SQL);
- **Docker / Docker-Compose** → ambiente containerizado;
- **Python** → lógica principal de orquestração e DAGs.

## Pré-requisitos

Antes de começar, instale:

- Docker & Docker-Compose
- Python 3.9 ou superior para testes locais   
- Git

Obs: Todo o projeto foi executado e buildado em uma distro Linux (Ubuntu v.20.04). Para execuções em ambiente Linux, veja a documentação oficial da Docker: https://docs.docker.com/


## Rodando o Projeto (Local com Docker)

1. Clone o projeto:

```bash
   git clone https://github.com/ncorreia47/case-elt-pipeline.git
   cd case-elt-pipeline
```

2. Na pasta onde encontra-se o arquivo Dockerfile, execute o seguinte comando:
   
```bash
   sudo docker compose up -d
```

3. Verifique se os serviços foram criados corretamente:
   
```bash
    sudo docker exec -it <nome_do_container> sh
    dbt debug
```

4. (Opcional) Substitua <nome_do_container> pelo nome real do container que o Docker Compose criou.
   Dependendo da configuração local, o Airflow pode precisar de permissões em pastas para salvar os arquivos que simulam um bucket.
   No ambiente Linux, a abordagem utilizada foi conceder acesso ao user airflow (50000) via acl (não utilizei chmod). Isso garante que seu usuário local não perca permissões.
    
```bash
    sudo setfacl -R -m u:50000:rwx nome_pasta_para_dar_grant_de_acesso. Ex dags/
```

5. Crie um usuário do banco de dados na UI do Airflow (para ser utilizado via parametros). Na UI do Airflow, vá em: Admin -> Connections -> Add Connection e adicione a seguinte conexão:
```bash
    host=datawarehouse
    login=postgres
    password=admin
    port=5432
    database=datawarehouse
```


## Fluxo de Dados (ELT)

Extract: Coleta os dados de origem (API), salva os dados em json simulando um comportamento de um bucket.

Load: Insere os dados brutos em uma camada de landing zone no banco Postgres.

Transform: Usa dbt para transformar e modelar dados em camadas bronze (dados brutos com tratamentos mínimos), silver (entidades de negócios) e gold (tabelas analíticas).


## Fluxo de Orquestração da arquitetura:

O Airflow dispara DAGs que executam essas etapas de forma programada (a partir da bronze -> gold, através de Eventos (Assets). Não foi agendado nenhum schedule, mas em ambientes de produção poderíamos ajustar o fluxo para ser automãtico desde a extração.

A primeira etapa do processo seria a representação da camada Raw (dados crus, sem nenhum tipo de tratamento).
Para simular o mesmo comportamento do ambiente de desenvolvimento para criação da camada Raw, execute os dags nessa ordem no console do Airflow:
1. dag_api_get_data: dag responsável por consumir os dados da API e simular o salvamento em um bucket
2. dag_ingest_json_landing_zone: dag responsável por salvar os dados desnormalizados em uma landing_zone e acionar uma tarefa de limpeza do bucket, se as tarefas forem concluídas sem erros
3. dag_landing_to_bronze: dag responsável por normalizar os dados e enviá-los para a camada bronze (bruta). A partir desse dag, os demais são acionados através de eventos (Assets).


## Fluxo da Modelagem no dbt:

No dbt, foram aplicadas as práticas para modelagem das camadas bronze, silver e gold:

1. Construção correta de cada camada, respeitando suas finalidades
2. Criação de testes com dbt tests, garantindo a integridade dos dados entre camadas
3. Utilização das funcionalidades do Jinja para macros e aplicações de cargas incrementais (utilizando parametros do Airflow no dbt)
4. Padronização das consultas, nomes e outros métodos, com o objetivo de facilitar o entendimento de novos usuários
5. Todas as queries de negócios (ad-hoc) foram disponibilizadas na pasta analyses

O que não foi implementado nesse projeto mas que fica como um backlog de melhorias:
1. Contratos de dados (desde a ingestão até a camada analítica)
2. Snapshots de dados
3. Atualizações com os métodos nativos do dbt (como freshness)
4. Testes de idempotencia nos dags
5. Validações mais criteriosas no consumo da API (time limit, payload, retry etc).

