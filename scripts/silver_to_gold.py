from pyspark.sql.functions import col, row_number, first, count, lit, rand, when, datediff, current_date
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType
import logging
import sys
import os

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
MINIO_BUCKET = 'people-analytics'
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'local[*]')

# Configurações do banco de dados PostgreSQL da aplicação
POSTGRES_HOST = "postgres-application-db"
POSTGRES_PORT = "5432"
POSTGRES_DB = os.getenv("POSTGRES_APP_DB")
POSTGRES_USER = os.getenv("POSTGRES_APP_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_APP_PASSWORD")
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def create_spark_session():
    """Cria e configura uma sessão Spark para interagir com MinIO e PostgreSQL."""
    logging.info("Criando a sessão Spark para a camada Ouro.")
    try:
        spark = (
            SparkSession.builder
            .appName("SilverToGoldTransformation")
            .master(SPARK_MASTER_URL)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.3.1")
            .getOrCreate()
        )
        logging.info("Sessão Spark para a camada Ouro criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a sessão Spark: {e}")
        sys.exit(1)

def create_gold_table(spark):
    """Lê os dados da camada Prata, aplica a lógica de negócio e cria a tabela Ouro."""
    try:
        logging.info("Lendo dados da camada Prata.")
        colaboradores_silver_path = f"s3a://{MINIO_BUCKET}/silver/colaboradores/"
        movimentacoes_silver_path = f"s3a://{MINIO_BUCKET}/silver/movimentacoes/"

        df_colaboradores = spark.read.parquet(colaboradores_silver_path)
        df_movimentacoes = spark.read.parquet(movimentacoes_silver_path)

        window_spec = Window.partitionBy("id").orderBy(col("data_movimentacao").desc())
        df_ultima_movimentacao = (
            df_movimentacoes
            .withColumn("rank", row_number().over(window_spec))
            .filter(col("rank") == 1)
            .select(
                col("id"),
                col("tipo_movimentacao").alias("ultima_movimentacao"),
                col("data_movimentacao").alias("data_ultima_movimentacao")
            )
        )

        df_contagem_movimentacoes = df_movimentacoes.groupBy("id").agg(count("*").alias("total_movimentacoes"))

        df_fato_colaborador = (
            df_colaboradores
            .join(df_ultima_movimentacao, "id", "left")
            .join(df_contagem_movimentacoes, "id", "left")
            .fillna(0, subset=["total_movimentacoes"])
        )

        df_fato_colaborador = df_fato_colaborador.withColumn(
            "logins_simulados", (rand() * 100).cast(IntegerType())
        ).withColumn(
            "tempo_ultima_movimentacao_anos", 
            datediff(current_date(), col("data_ultima_movimentacao")) / 365
        )

        df_fato_colaborador = df_fato_colaborador.withColumn(
            "risco_saida",
            when(
                (col("tempo_ultima_movimentacao_anos") > 3) & (col("total_movimentacoes") > 0), "Alto"
            ).when(
                (col("tempo_ultima_movimentacao_anos") > 1.5), "Médio"
            ).otherwise("Baixo")
        )
        
        return df_fato_colaborador

    except Exception as e:
        logging.error(f"Erro na transformação para a camada Ouro: {e}")
        raise

def write_to_postgres(df, table_name):
    """Escreve o DataFrame final na tabela do PostgreSQL."""
    logging.info(f"Escrevendo dados na tabela '{table_name}' do PostgreSQL.")
    try:
        (df.write
           .format("jdbc")
           .option("url", POSTGRES_URL)
           .option("dbtable", f"gold.{table_name}")
           .option("user", POSTGRES_USER)
           .option("password", POSTGRES_PASSWORD)
           .option("driver", POSTGRES_DRIVER)
           .mode("overwrite")
           .save())
        logging.info("Dados escritos no PostgreSQL com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao escrever no PostgreSQL: {e}")
        raise

def main():
    spark_session = create_spark_session()
    
    df_gold = create_gold_table(spark_session)
    
    logging.info("Schema final da tabela Ouro:")
    df_gold.printSchema()
    
    logging.info("Amostra dos dados da tabela Ouro:")
    df_gold.show(5)

    write_to_postgres(df_gold, "fato_colaborador")
    
    spark_session.stop()
    logging.info("Processo Prata para Ouro concluído com sucesso.")

if __name__ == "__main__":
    main()
