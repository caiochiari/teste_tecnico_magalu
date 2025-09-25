from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="people_analytics_pipeline",
    # Data de início: O pipeline tentará rodar para todas as datas a partir daqui.
    # Usamos uma data no passado para poder acionar um "backfill" ou um run manual para testes.
    start_date=pendulum.datetime(2025, 9, 24, tz="America/Sao_Paulo"),
    
    # Programação: "0 0 * * *" significa "à meia-noite, todos os dias".
    schedule_interval="0 0 * * *",
    
    # Evita que a DAG rode para todos os intervalos passados desde a start_date ao ser ativada.
    catchup=False,
    
    # Tags para facilitar a organização na UI do Airflow.
    tags=["people-analytics", "datalake"],
    
    doc_md="""
    ### Pipeline de Dados de People Analytics

    Esta DAG orquestra o pipeline completo de dados para a área de People Analytics.
    
    **Arquitetura Medalhão:**
    - **Bronze:** Ingestão de dados brutos das fontes (API e CSV) para o Data Lake no MinIO.
    - **Prata (TODO):** Limpeza, padronização e enriquecimento dos dados.
    - **Ouro (TODO):** Agregação e criação de modelos de dados para consumo analítico.
    """,
) as dag:

    # Task 1: Ingestão para a Camada Bronze
    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command="python /opt/airflow/scripts/ingest_to_bronze.py {{ ds }}",
        doc_md="""
        #### Tarefa de Ingestão para a Camada Bronze
        
        Executa o script `ingest_to_bronze.py` para:
        1. Consumir dados da API de movimentações.
        2. Ler dados do CSV de colaboradores.
        3. Salvar os dados brutos, em formato Parquet e particionados por data de ingestão, no bucket do MinIO.
        
        O parâmetro `{{ ds }}` (template Jinja do Airflow) passa a data de execução lógica 
        da DAG para o script, garantindo a idempotência e o processamento incremental.
        """,
    )

    # Ex: ingest_to_bronze >> bronze_to_silver >> ...
