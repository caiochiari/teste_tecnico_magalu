# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o arquivo de dependências primeiro
COPY requirements.txt .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código do agente
COPY . .

# Comando para executar o script do agente quando o contêiner iniciar
CMD ["streamlit", "run", "agent.py", "--server.port=8501", "--server.address=0.0.0.0"]
