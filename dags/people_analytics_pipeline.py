from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="people_analytics_pipeline",
    start_date=pendulum.datetime(2025, 9, 24, tz="America/Sao_Paulo"),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["people-analytics", "datalake", "producao"],
    doc_md="""
    ### Pipeline de Dados de People Analytics

    Esta DAG orquestra o pipeline completo de dados para a área de People Analytics.
    
    **Arquitetura Medalhão:**
    - **Bronze:** Ingestão de dados brutos das fontes (API e CSV) para o Data Lake no MinIO.
    - **Prata:** Limpeza, padronização e enriquecimento dos dados.
    - **Ouro (TODO):** Agregação e criação de modelos de dados para consumo analítico no PostgreSQL.
    """,
) as dag:

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command="python /opt/airflow/scripts/ingest_to_bronze.py {{ ds }}",
        doc_md="""
        #### Tarefa de Ingestão para a Camada Bronze
        
        Executa o script `ingest_to_bronze.py` para consumir fontes e salvar
        os dados brutos, em formato Parquet e particionados por data, no Data Lake (MinIO).
        """,
    )

    process_bronze_to_silver = SparkSubmitOperator(
        task_id="process_bronze_to_silver",
        conn_id="spark_default", 
        application="/opt/airflow/scripts/bronze_to_silver.py",
        application_args=["{{ ds }}"],
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf={
            "spark.driver.extraJavaOptions": "-Dlog4j.configuration=file:///opt/spark/conf/log4j.properties",
        },
        doc_md="""
        #### Tarefa de Transformação Bronze para Prata
        
        Submete um job PySpark para:
        1. Ler os dados brutos da última partição da camada Bronze.
        2. Aplicar limpeza de dados, conversão de tipos e remoção de duplicatas.
        3. Salvar os dados transformados na camada Prata do Data Lake (MinIO).
        """,
    )

    # Definindo a ordem de execução das tarefas
    ingest_to_bronze >> process_bronze_to_silver