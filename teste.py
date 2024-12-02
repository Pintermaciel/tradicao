import requests
import json  # Importando a biblioteca json

# URL da API com a data especificada
url = "https://xysxl4d0mc.execute-api.sa-east-1.amazonaws.com/v1/vendas?token=46723379751&data_venda=2024-06-06,2024-06-06"

# Fazendo a requisição GET
response = requests.get(url)

# Verificando o status da resposta
if response.status_code == 200:
    try:
        # Processando os dados JSON com json.loads
        vendas = json.loads(response.text)  # Usando json.loads aqui

        # Exibindo todas as vendas
        for venda in vendas:
            print(venda)  # Exibindo cada venda individualmente

    except ValueError as e:
        print("Erro ao decodificar JSON:", e)
else:
    print(f"Erro ao obter dados: {response.status_code} - {response.text}")
