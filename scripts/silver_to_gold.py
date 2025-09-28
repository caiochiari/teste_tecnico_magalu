from pyspark.sql.functions import col, row_number, count, when, lit, coalesce, datediff, current_date, round as spark_round
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType
import logging
import sys
import os

# --- CONFIGURAÇÕES ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ROOT_USER')
MINIO_SECRET_KEY = os.getenv('MINIO_ROOT_PASSWORD')
MINIO_BUCKET = os.getenv('MINIO_BUCKET')
SPARK_MASTER_URL = os.getenv('SPARK_MASTER_URL', 'local[*]')

# Configurações do banco de dados PostgreSQL da aplicação
POSTGRES_HOST = os.getenv('POSTGRES_APP_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_APP_PORT')
POSTGRES_DB = os.getenv("POSTGRES_APP_DB")
POSTGRES_USER = os.getenv("POSTGRES_APP_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_APP_PASSWORD")
POSTGRES_DRIVER = "org.postgresql.Driver"
POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def create_spark_session():
    """
    Cria e configura uma sessão Spark para interagir com o MinIO e PostgreSQL.
    """
    logging.info("Criando a sessão Spark.")
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
        logging.info("Sessão Spark criada com sucesso.")
        return spark
    except Exception as e:
        logging.error(f"Erro ao criar a sessão Spark: {e}")
        sys.exit(1)

def write_to_postgres(df, table_name):
    """
    Escreve um DataFrame Spark em uma tabela do PostgreSQL.
    """
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
        logging.info(f"Tabela '{table_name}' escrita com sucesso no PostgreSQL.")
    except Exception as e:
        logging.error(f"Erro ao escrever no PostgreSQL: {e}")
        raise

def main():
    """
    Orquestra a transformação de dados da camada Prata para a Ouro.
    """
    spark_session = create_spark_session()

    colaboradores_path = f"s3a://{MINIO_BUCKET}/silver/colaboradores/"
    movimentacoes_path = f"s3a://{MINIO_BUCKET}/silver/movimentacoes/"

    df_colaboradores = spark_session.read.parquet(colaboradores_path)
    df_movimentacoes = spark_session.read.parquet(movimentacoes_path)

    # Encontra a última movimentação de cada colaborador
    window_spec = Window.partitionBy("id").orderBy(col("data_movimentacao").desc())
    df_ultima_movimentacao = (
        df_movimentacoes
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .select(
            col("id").alias("ult_mov_id"),
            col("data_movimentacao").alias("data_ultima_movimentacao")
        )
    )

    # Conta o total de movimentações por colaborador
    df_total_movimentacoes = (
        df_movimentacoes
        .groupBy("id")
        .agg(count("*").alias("total_movimentacoes"))
    )
    
    # Cria uma flag para colaboradores desligados
    df_desligados = (
        df_movimentacoes
        .filter(col("tipo_movimentacao") == "Desligamento")
        .select(col("id").alias("desligado_id"), lit(1).alias("desligado_flag"))
        .distinct()
    )

    # Uni todos os dfs para construir a tabela fato
    df_gold = (
        df_colaboradores
        .join(df_ultima_movimentacao, df_colaboradores.id == df_ultima_movimentacao.ult_mov_id, "left")
        .join(df_total_movimentacoes, df_colaboradores.id == df_total_movimentacoes.id, "left")
        .join(df_desligados, df_colaboradores.id == df_desligados.desligado_id, "left")
        .select(
            df_colaboradores.id,
            df_colaboradores.nome,
            df_colaboradores.idade,
            df_colaboradores.cargo,
            df_colaboradores.area,
            coalesce(col("data_ultima_movimentacao"), col("data_de_admissao")).alias("data_ultima_movimentacao"),
            coalesce(col("total_movimentacoes"), lit(0)).alias("total_movimentacoes"),
            col("desligado_flag")
        )
        .withColumn("logins_simulados", (spark_round(col("idade") / 10) * 100).cast(IntegerType()))
        .withColumn(
            "status_colaborador",
            when(col("desligado_flag") == 1, "Desligado").otherwise("Ativo")
        )
    )

    # Calculaa a métrica de negócio de Risco de Saída dos funcionarios
    df_gold_com_metricas = (
        df_gold
        .withColumn("tempo_no_cargo_em_anos", datediff(current_date(), col("data_ultima_movimentacao")) / 365)
        .withColumn(
            "risco_saida",
            when(
                col("status_colaborador") == "Desligado",
                "N/A (Desligado)"
            ).when(
                (col("tempo_no_cargo_em_anos") > 3) & (col("total_movimentacoes") == 0),
                "Alto"
            ).when(
                (col("tempo_no_cargo_em_anos") > 2) & (col("total_movimentacoes") <= 1),
                "Médio"
            ).otherwise("Baixo")
        )
        .select(
            "id", "nome", "idade", "cargo", "area", "data_ultima_movimentacao",
            "total_movimentacoes", "logins_simulados", "status_colaborador", "risco_saida"
        )
    )

    write_to_postgres(df_gold_com_metricas, "fato_colaborador")
    
    spark_session.stop()
    logging.info("Processo Prata para Ouro concluído com sucesso.")

if __name__ == "__main__":
    main()