from botocore.exceptions import NoCredentialsError
from datetime import datetime
import pandas as pd
import requests
import logging
import boto3
import sys
import os

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
MINIO_BUCKET = 'people-analytics'

API_URL = 'http://fake-api:5000/movimentacoes'
CSV_PATH = '/opt/airflow/data/colaboradores.csv'

def get_s3_client():
    """Cria e retorna um cliente S3 configurado para o MinIO."""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=boto3.session.Config(signature_version='s3v4')
        )
        # Testa a conexão
        s3_client.list_buckets()
        logging.info("Conexão com o MinIO estabelecida com sucesso.")
        return s3_client
    except Exception as e:
        logging.error(f"Não foi possível conectar ao MinIO: {e}")
        sys.exit(1)

def ingest_api_data(s3_client, dt_ingestao):
    """
    Ingere dados da API de movimentações e os salva na camada Bronze.
    """
    try:
        logging.info(f"Buscando dados da API: {API_URL}")
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)

        df['dt_ingestao'] = dt_ingestao
        
        # Converte o DataFrame para Parquet em memória
        parquet_buffer = df.to_parquet(index=False, engine='pyarrow')

        # Define o caminho do objeto no MinIO (camada Bronze)
        object_name = f'bronze/movimentacoes/dt_ingestao={dt_ingestao}/movimentacoes.parquet'
        
        logging.info(f"Enviando dados de movimentações para o MinIO: {object_name}")
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=object_name, Body=parquet_buffer)
        logging.info("Dados de movimentações ingeridos com sucesso.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao acessar a API: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Erro inesperado na ingestão de dados da API: {e}")
        sys.exit(1)

def ingest_csv_data(s3_client, dt_ingestao):
    """
    Ingere dados do CSV de colaboradores e os salva na camada Bronze.
    """
    try:
        logging.info(f"Lendo dados do CSV: {CSV_PATH}")
        if not os.path.exists(CSV_PATH):
            logging.error(f"Arquivo CSV não encontrado em: {CSV_PATH}")
            sys.exit(1)

        df = pd.read_csv(CSV_PATH)
        
        df['dt_ingestao'] = dt_ingestao

        parquet_buffer = df.to_parquet(index=False, engine='pyarrow')

        object_name = f'bronze/colaboradores/dt_ingestao={dt_ingestao}/colaboradores.parquet'

        logging.info(f"Enviando dados de colaboradores para o MinIO: {object_name}")
        s3_client.put_object(Bucket=MINIO_BUCKET, Key=object_name, Body=parquet_buffer)
        logging.info("Dados de colaboradores ingeridos com sucesso.")

    except FileNotFoundError:
        logging.error(f"Arquivo CSV não encontrado no caminho especificado: {CSV_PATH}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Erro inesperado na ingestão de dados do CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # O Airflow passará a data de execução como o primeiro argumento
    if len(sys.argv) > 1:
        execution_date_str = sys.argv[1]
        dt_ingestao = datetime.strptime(execution_date_str, '%Y-%m-%d').strftime('%Y-%m-%d')
    else:
        # Fallback para execução local/manual
        dt_ingestao = datetime.now().strftime('%Y-%m-%d')
    
    logging.info(f"Iniciando ingestão para a data: {dt_ingestao}")
    
    s3 = get_s3_client()
    ingest_api_data(s3, dt_ingestao)
    ingest_csv_data(s3, dt_ingestao)
    
    logging.info("Processo de ingestão para a camada Bronze concluído.")