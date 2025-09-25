# Usar uma imagem base oficial do Python
FROM python:3.9-slim

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o arquivo de dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código da aplicação
COPY . .

# Expor a porta que a aplicação Flask vai rodar
EXPOSE 5000

# Comando para iniciar a aplicação
# O host 0.0.0.0 é essencial para que a API seja acessível por outros contêineres
CMD ["flask", "run", "--host=0.0.0.0"]
