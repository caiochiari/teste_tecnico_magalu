# **Desafio Técnico \- Engenheiro(a) de Dados (People Analytics)**

## **Visão Geral**

Este projeto implementa um pipeline de dados ponta a ponta para a área de People Analytics, seguindo as melhores práticas de engenharia de dados. A solução ingere dados de colaboradores de múltiplas fontes (uma API simulada e um arquivo CSV), processa-os utilizando uma arquitetura escalável e os disponibiliza em um Data Mart para consumo por um agente de Inteligência Artificial.

O objetivo é demonstrar proficiência em orquestração de workflows, processamento de dados em larga escala, modelagem de dados e a criação de produtos de dados inteligentes.

## **Arquitetura da Solução**

O pipeline foi desenhado utilizando a **Arquitetura Medalhão** sobre uma stack de tecnologias containerizadas com Docker.

\+----------------+      \+---------------------+      \+------------------------+      \+-------------------------+      \+-----------------------+  
|                |      |                     |      |                        |      |                         |      |                       |  
|   Fontes de    |-----\>|     Ingestão        |-----\>|   Data Lake (MinIO)    |-----\>|   Processamento (Spark) |-----\>|  Data Mart (Postgres) |  
|      Dados     |      |  (Python/Pandas)    |      | (Armazenamento Parquet)|      |  (Lógica de Negócio)    |      |   (Camada Ouro)       |  
| (API \+ CSV)    |      |                     |      |                        |      |                         |      |                       |  
\+----------------+      \+---------------------+      \+-----+------------------+      \+------------+------------+      \+-----------+-----------+  
                                                           | Camada Bronze (Bruto)                       |                          |  
                                                           | Camada Prata (Limpo)                        |                          |  
                                                           \+------------------+                          |                          |  
                                                                                                         |                          |  
                                                                                                         |           Consulta via SQL  
                                                                                                         |                          |  
                                                                                                         |                          |  
                                                                                         \+---------------+--------------+  
                                                                                         |                              |  
                                                                                         | Agente de IA (Google ADK)    |  
                                                                                         |    (Consumidor Final)        |  
                                                                                         \+------------------------------+

### **Diagrama de Infraestrutura**

A infraestrutura é composta por um ecossistema de microsserviços que se comunicam através de uma rede Docker customizada (data-network).

\+-------------------------------------------------------------------------+  
| Rede Docker: data-network                                               |  
|                                                                         |  
|  \+-----------------------+      \+-----------------------+               |  
|  |     Airflow Cluster   |      |     Spark Cluster     |               |  
|  | (Scheduler, Worker,  |      |  (Master, Worker)     |               |  
|  |  Webserver, Redis,    |      |                       |               |  
|  |  Postgres Metadados)  |      |                       |               |  
|  \+-----------------------+      \+-----------------------+               |  
|           ^ |                             ^ |                           |  
|           | | Orquestra                   | | Processa                  |  
|           | v                             | v                           |  
|  \+-----------------------+      \+-----------------------+               |  
|  |     Scripts (Python)  |      |   Data Lake (MinIO)   |               |  
|  |  \- ingest\_to\_bronze.py| \<--\> |   \- Bucket:           |               |  
|  |  \- bronze\_to\_silver.py|      |     people-analytics  |               |  
|  |  \- silver\_to\_gold.py  |      |                       |               |  
|  \+-----------------------+      \+-----------------------+               |  
|                                             |                           |  
|      Leitura (S3A) |------------------------+------------------| Escrita (JDBC) |  
|                    v                                            v                 |  
|  \+-----------------------+      \+-----------------------+      \+-----------------------+  
|  |     Fonte: API Fake   |      |   Agente de IA (Web)  |      |  Data Mart (Postgres) |  
|  |        (Flask)        | \<--\> |  (Streamlit \+ Gemini) | \<--\> |   \- DB: people\_db     |  
|  \+-----------------------+      \+-----------------------+      |   \- Schema: gold      |  
|                                                                \+-----------------------+  
\+-------------------------------------------------------------------------+

## **Como Rodar o Projeto**

### **Pré-requisitos**

* Docker  
* Docker Compose  

### **1\. Preparação do Ambiente**

Clone este repositório para a sua máquina local. Antes de iniciar, é necessário criar um arquivo .env na raiz do projeto. Você pode renomear o arquivo env.example para .env se ele existir.

Este arquivo centraliza todas as senhas e configurações. **Nenhuma alteração nos valores padrão é necessária para rodar o projeto, exceto pela chave da API do Google (passo 3).**

### **2\. Iniciando a Infraestrutura**

Com o Docker em execução, suba todos os serviços com um único comando:

docker-compose up \-d \--build

Este comando irá construir as imagens customizadas (Airflow, API, Agente de IA) e iniciar todos os contêineres em segundo plano. Pode levar alguns minutos na primeira vez.

**Serviços disponíveis:**

* **Airflow UI:** http://localhost:8080 (login: admin, senha: admin)  
* **Spark Master UI:** http://localhost:8081  
* **MinIO Console (Data Lake):** http://localhost:9001 (acesse as credenciais no arquivo .env)
* **Chat Agente de IA:** http://localhost:8501

### **3\. Configurar o Agente de IA (Bônus)**

Para utilizar o chat interativo, você precisa de uma chave de API do Google.

1. Acesse o [**Google AI Studio**](https://aistudio.google.com/).  
2. Obtenha sua chave de API (Get API key).  
3. Abra o arquivo .env e cole a sua chave na variável GOOGLE\_API\_KEY.

\# .env  
GOOGLE\_API\_KEY="SUA\_CHAVE\_API\_VEM\_AQUI"

### **4\. Executando o Pipeline**

1. Acesse a interface do Airflow (http://localhost:8080).  
2. Na lista de DAGs, encontre people\_analytics\_pipeline e ative-a no botão de toggle.  
3. Para iniciar uma execução imediatamente, clique no botão "Play" (▶️) ao lado do nome da DAG e selecione "Trigger DAG".  
4. Acompanhe a execução na aba "Grid". As tarefas ficarão verdes à medida que forem concluídas com sucesso.

### **5\. Interagindo com o Agente de IA (Chat Web Bônus)**

Após o pipeline ser executado com sucesso, a interface de chat do agente de IA estará disponível no seu navegador.

1. Acesse o seguinte endereço:  
   http://localhost:8501  
2. Aguarde a página carregar. Você verá uma interface de chat.  
3. Digite sua pergunta na caixa de texto na parte inferior e pressione Enter.  
   Exemplo: Quais são os colaboradores com risco de saída alto?

## **Estrutura de Pastas**

.  
├── ai\_agent/         \# Contém a aplicação do agente de IA com Streamlit (Bônus).  
├── api/              \# Contém a API Fake em Flask para simular uma fonte de dados.  
├── dags/             \# Definições das DAGs do Airflow.  
├── data/             \# Arquivos de dados de origem (CSV, JSON).  
├── postgres-init/    \# Scripts SQL de inicialização para o banco de dados da aplicação.  
├── scripts/          \# Scripts Python e PySpark executados pelas tarefas do Airflow.  
├── .env              \# Arquivo de configuração com todas as variáveis de ambiente (local).  
├── Dockerfile        \# Dockerfile para customizar a imagem do Airflow.  
└── docker-compose.yml\# Arquivo principal que define e orquestra toda a infraestrutura.

## **Escolhas Técnicas e Justificativas**

* **Docker e Docker Compose:** Permitem criar um ambiente de desenvolvimento completo, isolado e 100% reproduzível, simplificando o setup e garantindo que o projeto funcione em qualquer máquina.  
* **Airflow:** Escolhido como orquestrador pela sua robustez, flexibilidade e por ser o padrão de mercado para a criação de workflows de dados complexos.  
* **Arquitetura Medalhão:** A separação dos dados em camadas **Bronze** (brutos), **Prata** (limpos) e **Ouro** (agregados para negócio) é uma prática moderna que garante governança e qualidade.  
  * **Camada Prata \- Qualidade de Dados:** Nesta camada, são aplicadas regras explícitas de qualidade: remoção de duplicatas, padronização de tipos de dados e tratamento de valores nulos. Linhas com identificadores críticos nulos são descartadas, enquanto campos não-críticos são preenchidos com valores padrão (ex: "Não Informado"), garantindo a integridade e a usabilidade dos dados.  
  * **Camada Ouro \- Lógica de Negócio:** Esta camada consolida os dados e aplica as regras de negócio. Um exemplo é a métrica de *risco de saída*, que é calculada condicionalmente apenas para colaboradores com status 'Ativo', excluindo corretamente funcionários já desligados da análise e tornando o insight mais preciso.  
* **MinIO como Data Lake:** O MinIO simula um Object Storage como o Amazon S3. É a escolha ideal para armazenar grandes volumes de dados de forma escalável e com baixo custo. O uso do formato **Parquet** otimiza o armazenamento e a performance das leituras pelo Spark.  
* **Apache Spark:** Selecionado como a ferramenta de processamento por sua capacidade de lidar com grandes volumes de dados de forma distribuída e performática. Todas as transformações pesadas (Bronze \-\> Prata \-\> Ouro) são realizadas com Spark, demonstrando uma mentalidade de escalabilidade.  
* **PostgreSQL como Data Mart:** Enquanto o MinIO armazena os dados brutos e intermediários, o PostgreSQL serve a camada Ouro. Ele é ideal para disponibilizar dados já modelados e agregados para aplicações finais que exigem baixa latência, como dashboards e o nosso agente de IA.  
* **Agente de IA com Streamlit e Google ADK:** Para a etapa bônus, foi utilizado o Google Agent Development Kit (ADK), implementado através do Google AI SDK (para o modelo Gemini), para a lógica de IA e o **Streamlit** para criar uma interface de usuário web interativa. Esta abordagem demonstra não apenas a capacidade de consumir os dados da camada Ouro, mas também de entregar um produto de dados final polido e fácil de usar.

## **Possíveis Melhorias**

* **Testes e Qualidade de Dados:** Implementar testes unitários para as transformações, garantindo a funcionalidade dos scripts
* **CI/CD:** Criar um pipeline de integração e entrega contínua (ex: com Jenkins) para automatizar os testes e o deploy de novas versões do projeto.
* **Monitoramento e Alertas:** Integrar o Airflow com ferramentas de monitoramento (ex: Prometheus, Grafana) e configurar alertas (ex: via , e-mail) em caso de falhas no pipeline.  
* **Segurança:** Utilizar um backend de segredos do Airflow (como o HashiCorp Vault) para gerenciar senhas e chaves de API, em vez de armazená-las em texto plano no arquivo .env. 
* **Evolução para um Lakehouse com Delta Lake:** Substituir o formato Parquet pelo Delta Lake na camada Prata. Isso traria transações ACID ao Data Lake, permitindo operações de MERGE (upsert) muito mais eficientes e seguras, além de habilitar versionamento de dados ("Time Travel").
* **Governança de Dados:** Implementar uma ferramenta de catálogo de dados para documentar schemas, linhagem de dados e proprietários, tornando a plataforma mais confiável e auditável para a empresa.
* **Robustez do Agente de IA:** Incrementar o agente de IA com um tratamento de erros mais sofisticado, como validar o SQL gerado pelo LLM antes da execução e fornecer respostas amigáveis ao usuário caso a pergunta não possa ser respondida.
* **Maior Granularidade e Paralelismo nas Tarefas:** Separar as tarefas de ingestão e processamento por fonte de dados (uma para colaboradores, outra para movimentações). Isso permitiria a re-execução independente em caso de falha de uma única fonte e habilitaria o processamento em paralelo das fontes independentes, tornando o pipeline mais rápido e resiliente.