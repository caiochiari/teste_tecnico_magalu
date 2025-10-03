# Usar uma imagem base oficial do Python
FROM python:3.10-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o arquivo de dependências primeiro
COPY requirements.txt .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código do agente
COPY . .

# Comando para executar o script do agente quando o contêiner iniciar
CMD ["adk", "web", "--host", "0.0.0.0", "--port", "8501", "."]