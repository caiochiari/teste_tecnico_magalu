import os
import streamlit as st
import google.generativeai as genai
from sqlalchemy import create_engine, text

# --- Configuração da Página do Streamlit ---
st.set_page_config(
    page_title="Magalu People Analytics IA",
    page_icon="🤖",
    layout="centered"
)

# Usei o cache do Streamlit para evitar reconectar a cada interação
@st.cache_resource
def get_db_connection():
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL.
    """
    try:
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            st.error("A variável de ambiente DATABASE_URL não foi definida.")
            return None
        engine = create_engine(db_url)
        connection = engine.connect()
        print("Conexão com o PostgreSQL estabelecida com sucesso.")
        return connection
    except Exception as e:
        st.error(f"Erro ao conectar com o banco de dados: {e}")
        return None

@st.cache_resource
def configure_ai():
    """
    Configura e retorna o modelo generativo do Google.
    """
    try:
        api_key = os.getenv("GOOGLE_API_KEY")
        if not api_key or "SUA_CHAVE_API_AQUI" in api_key:
            st.error("A chave de API do Google (GOOGLE_API_KEY) não foi definida no arquivo .env.")
            return None
        genai.configure(api_key=api_key)
        print("SDK do Google Generative AI configurado com sucesso.")
        return genai.GenerativeModel('gemini-2.5-flash')
    except Exception as e:
        st.error(f"Erro ao configurar o SDK de IA: {e}")
        return None

def generate_sql_from_question(model, question, schema):
    """
    Gera uma consulta SQL a partir de uma pergunta.
    """
    prompt = f"""
    Com base no esquema da tabela PostgreSQL abaixo, sua tarefa é gerar uma consulta SQL que responda à pergunta do usuário.
    Responda APENAS com o código SQL. Não adicione texto extra, explicações ou formatação de markdown.

    
    **Esquema da Tabela:**
    {schema}

    **Pergunta do Usuário:**
    "{question}"

    **SQL Gerado:**
    """
    try:
        response = model.generate_content(prompt)
        sql_query = response.text.strip().replace("```sql", "").replace("```", "")
        # Salva para possivel exibição
        st.session_state.last_sql_query = sql_query
        return sql_query
    except Exception as e:
        st.error(f"Erro ao gerar SQL: {e}")
        return None

def execute_sql_query(connection, query):
    """
    Executa a consulta SQL e retorna os resultados.
    """
    try:
        result_proxy = connection.execute(text(query))
        results = [dict(row) for row in result_proxy.mappings()]
        return results
    except Exception as e:
        st.error(f"Erro ao executar a consulta SQL: {e}")
        return None

def generate_answer_from_result(model, question, results):
    """
    Gera uma resposta em linguagem natural a partir dos resultados.
    """
    prompt = f"""
    Sua tarefa é atuar como um assistente de People Analytics.
    Com base na pergunta original do usuário e nos dados retornados do banco de dados, formule uma resposta clara, concisa e amigável em português.

    **Pergunta Original:**
    "{question}"

    **Dados do Banco de Dados (em formato JSON):**
    {results}

    **Resposta Amigável:**
    """
    try:
        response = model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        st.error(f"Erro ao gerar a resposta final: {e}")
        return "Desculpe, não consegui gerar uma resposta final."

# --- Interface do Chat ---
st.title("🤖 Assistente de People Analytics do Magalu")
st.caption("Faça perguntas sobre os dados de colaboradores e obtenha insights.")

# Inicializa o histórico do chat na sessão
if "messages" not in st.session_state:
    st.session_state.messages = []

# Exibe as mensagens do histórico
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message:
            with st.expander("Ver consulta SQL"):
                st.code(message["sql"], language="sql")

# Obtém a pergunta do usuário
if prompt := st.chat_input("Quais colaboradores estão em risco de saída?"):
    # Adiciona e exibe a mensagem do usuário
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Gera e exibe a resposta do assistente
    with st.chat_message("assistant"):
        with st.spinner("Pensando..."):
            model = configure_ai()
            connection = get_db_connection()
            
            if model and connection:
                schema = """
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
                
                sql_query = generate_sql_from_question(model, prompt, schema)
                
                if sql_query:
                    results = execute_sql_query(connection, sql_query)
                    if results is not None:
                        response = generate_answer_from_result(model, prompt, results)
                        st.markdown(response)
                        # Adiciona a resposta completa ao histórico
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": response, 
                            "sql": st.session_state.get('last_sql_query', '')
                        })

