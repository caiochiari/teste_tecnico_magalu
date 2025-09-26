# Usa a imagem oficial do Airflow como base
FROM apache/airflow:2.8.1-python3.9

# Muda para o usuário root para instalar pacotes do sistema
USER root
# O SparkSubmitOperator precisa do Java (JRE) para funcionar
RUN apt-get update && apt-get install -y default-jre
# Volta para o usuário airflow
USER airflow

# Instala o provedor do Apache Spark.
# É uma boa prática fixar a versão para garantir a reprodutibilidade.
RUN pip install apache-airflow-providers-apache-spark==3.0.0

# Instala o PySpark.
RUN pip install pyspark==3.3.0