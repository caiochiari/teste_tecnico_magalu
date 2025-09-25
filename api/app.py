import json
from flask import Flask, jsonify, abort

# Inicializa a aplicação Flask
app = Flask(__name__)

# Define o caminho para o arquivo de dados
# O volume montado no docker-compose fará este arquivo estar disponível
DATA_FILE = '/app/data/movimentacoes.json'

def carregar_movimentacoes():
    """
    Função auxiliar para carregar os dados do arquivo JSON.
    """
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        # Aborta com um erro 500 se o arquivo de dados não for encontrado
        abort(500, description="Arquivo de dados (movimentacoes.json) não encontrado.")
    except json.JSONDecodeError:
        # Aborta se o arquivo JSON estiver mal formatado
        abort(500, description="Erro ao decodificar o arquivo JSON.")

@app.route('/movimentacoes', methods=['GET'])
def get_movimentacoes():
    """
    Endpoint principal que serve a lista de movimentações de colaboradores.
    """
    movimentacoes_data = carregar_movimentacoes()
    return jsonify(movimentacoes_data)

@app.route('/', methods=['GET'])
def health_check():
    """
    Endpoint de health check para verificar se a API está funcionando.
    """
    return jsonify({"status": "ok"}), 200

if __name__ == '__main__':
    # Roda a aplicação, tornando-a acessível por outros contêineres na porta 5000
    app.run(host='0.0.0.0', port=5000, debug=False)
