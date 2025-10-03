import os
import json
from sqlalchemy import create_engine, text
from google.adk.agents import Agent

# --- Definição da Ferramenta (Tool) ---
def query_database(sql_query: str) -> str:
    """
    Executa uma consulta SQL SOMENTE LEITURA no banco de dados de People Analytics
    e retorna o resultado em formato JSON. Use esta ferramenta para responder a
    perguntas sobre colaboradores, cargos, áreas e risco de saída.
    """
    try:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            return "Erro: A variável de ambiente DATABASE_URL não foi definida."
        
        engine = create_engine(db_url)
        with engine.connect() as connection:
            result_proxy = connection.execute(text(sql_query))
            results = [dict(row) for row in result_proxy.mappings()]
            return json.dumps(results)
            
    except Exception as e:
        return f"Erro ao executar a consulta SQL: {e}"

# --- Definição do Agente ---
# O schema da tabela é passado como parte das instruções para o agente.
db_schema = """
Tabela: gold.fato_colaborador
Colunas:
- id (INTEGER): Identificador único do colaborador.
- nome (TEXT): Nome do colaborador.
- idade (INTEGER): Idade do colaborador.
- cargo (TEXT): Cargo atual do colaborador.
- area (TEXT): Área de atuação do colaborador.
- data_ultima_movimentacao (DATE): Data da última promoção ou transferência.
- total_movimentacoes (BIGINT): Número total de movimentações na carreira.
- logins_simulados (BIGINT): Número simulado de logins no sistema.
- status_colaborador (TEXT): Flag que define se o funcionario está ativo na empresa ('Ativo', 'Desligado')
- risco_saida (TEXT): Nível de risco de saída ('Alto', 'Médio', 'Baixo', 'N/A (Desligado)').
"""

# Instancia o agente principal, exatamente como na documentação do Google ADK.
root_agent = Agent(
    name="people_analytics_agent",
    model="gemini-2.5-flash",
    description="Agente para responder perguntas sobre os dados de colaboradores do Magalu.",
    instruction=(
        f"""Você é um assistente de People Analytics. Com base no esquema de banco de dados abaixo, 
        sua tarefa é gerar e executar consultas SQL usando a ferramenta 'query_database' para responder 
        às perguntas do usuário. Responda sempre em português de forma clara e amigável.
        **Esquema do Banco de Dados:**{db_schema}"""
    ),
    tools=[query_database],
)