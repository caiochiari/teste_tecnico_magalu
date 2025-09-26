from pyspark.sql.functions import col, trim, to_date
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
import logging
import sys
import os

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
MINIO_BUCKET = 'people-analytics'
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'local[*]')

def create_spark_session():
    """Cria e configura uma sessão Spark para interagir com o MinIO."""
    logging.info("Criando a sessão Spark.")
    try:
        spark = (
            SparkSession.builder
            .appName("BronzeToSilverTransformation")
            .master(SPARK_MASTER_URL)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        logging.info("Sessão Spark criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a sessão Spark: {e}")
        sys.exit(1)

def process_colaboradores(spark, execution_date):
    """Processa os dados de colaboradores da camada Bronze para a Prata."""
    logging.info(f"Processando dados de colaboradores para a data: {execution_date}")
    try:
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/colaboradores/dt_ingestao={execution_date}/"
        df_bronze = spark.read.parquet(bronze_path)

        df_silver = (
            df_bronze
            .withColumn("id", col("id").cast(IntegerType()))
            .withColumn("idade", col("idade").cast(IntegerType()))
            .withColumn("nome", trim(col("nome")))
            .withColumn("cargo", trim(col("cargo")))
            .withColumn("area", trim(col("area")))
            .withColumn("data_de_admissao", to_date(col("data_de_admissao"), "yyyy-MM-dd"))
            .dropDuplicates(["id"])
        )

        silver_path = f"s3a://{MINIO_BUCKET}/silver/colaboradores/"
        logging.info(f"Salvando dados de colaboradores na camada Prata: {silver_path}")
        df_silver.write.mode("overwrite").parquet(silver_path)
        logging.info("Dados de colaboradores processados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao processar dados de colaboradores: {e}")
        raise

def process_movimentacoes(spark, execution_date):
    """Processa os dados de movimentações da camada Bronze para a Prata."""
    logging.info(f"Processando dados de movimentações para a data: {execution_date}")
    try:
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/movimentacoes/dt_ingestao={execution_date}/"
        df_bronze = spark.read.parquet(bronze_path)

        df_silver = (
            df_bronze
            .withColumn("id", col("colaborador_id").cast(IntegerType()))
            .withColumnRenamed("colaborador_id", "id_antigo")
            .withColumn("data_movimentacao", to_date(col("data_movimentacao"), "yyyy-MM-dd"))
            .select("id", "tipo_movimentacao", "data_movimentacao", "detalhes")
            .dropDuplicates()
        )

        silver_path = f"s3a://{MINIO_BUCKET}/silver/movimentacoes/"
        logging.info(f"Salvando dados de movimentações na camada Prata: {silver_path}")
        df_silver.write.mode("append").partitionBy("data_movimentacao").parquet(silver_path)
        logging.info("Dados de movimentações processados com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao processar dados de movimentações: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Erro: Data de execução não fornecida.")
        sys.exit(1)
    
    execution_date_str = sys.argv[1]
    spark_session = create_spark_session()
    
    process_colaboradores(spark_session, execution_date_str)
    process_movimentacoes(spark_session, execution_date_str)
    
    spark_session.stop()
    logging.info("Processo Bronze para Prata concluído com sucesso.")