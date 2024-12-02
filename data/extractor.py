import requests
import os
import time
from datetime import datetime, timedelta
from services.database_service import DatabaseService
from models.cliente import Cliente
from models.venda import Venda
from models.item_venda import ItemVenda
from models.pagamento import Pagamento

# Cria a pasta 'json' se não existir
os.makedirs("data/json", exist_ok=True)


# Função para gerar datas diárias dos últimos 5 anos em ordem decrescente
def generate_daily_intervals(years=5):
    today = datetime.today()
    intervals = []
    start_date = today

    for _ in range(years * 365):  # Aproximadamente 5 anos de dias
        end_date = start_date
        intervals.append(
            (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        )
        start_date -= timedelta(days=1)  # Retrocede um dia

    return intervals  # Retorna em ordem decrescente


# Gera as datas e faz as requisições GET com tentativa em caso de erro
def fetch_sales_data():
    token = "46723379751"
    base_url = "https://xysxl4d0mc.execute-api.sa-east-1.amazonaws.com/v1/vendas"
    intervals = generate_daily_intervals()
    database_service = DatabaseService(
        url="your_supabase_url", key="your_supabase_key"
    )  # Substitua com suas credenciais

    for start, end in intervals:
        max_retries = 3  # Número máximo de tentativas
        attempts = 0
        success = False

        while attempts < max_retries and not success:
            print(f"Requisição para: {base_url}?token={token}&data_venda={start},{end}")
            response = requests.get(
                base_url, params={"token": token, "data_venda": f"{start},{end}"}
            )
            attempts += 1

            if response.status_code == 200:
                # Tenta decodificar a resposta JSON
                try:
                    data = response.json()

                    # Verifica se a mensagem indica que não há registros
                    if (
                        "message" in data
                        and data["message"] == "Nenhum registro encontrado!"
                    ):
                        print(f"Nenhuma venda encontrada para o dia {start}.")
                        success = True  # Define sucesso para interromper o loop
                        break  # Interrompe as tentativas se não houver vendas

                    # Processa os dados
                    for venda in data:
                        if "Venda" in venda:
                            venda_details = venda["Venda"]
                            # Criação de instâncias de Venda, Cliente, ItemVenda e Pagamento
                            cliente = Cliente(
                                id_cliente=venda_details["id_cliente"],
                                nome_cliente=venda_details.get("nome_cliente", ""),
                                fone1=venda_details.get("fone1", ""),
                                fone2=venda_details.get("fone2", ""),
                                cpf_cnpj=venda_details.get("cpf_cnpj", ""),
                                endereco=venda_details.get("endereco", ""),
                                data_nascimento=venda_details.get(
                                    "data_nascimento", ""
                                ),
                                cidade=venda_details.get("cidade", ""),
                                uf=venda_details.get("uf", ""),
                                bairro=venda_details.get("bairro", ""),
                                cep=venda_details.get("cep", ""),
                            )

                            # Inserir ou atualizar o cliente no banco de dados
                            database_service.inserir_cliente(cliente)

                            venda_obj = Venda(
                                id_venda=venda_details["id_venda"],
                                qtdade_itens=venda_details["qtdade_itens"],
                                tipo=venda_details["tipo"],
                                id_cliente=venda_details["id_cliente"],
                                dt_venda=venda_details["dt_venda"],
                                hr_venda=venda_details["hr_venda"],
                                situacao=venda_details["situacao"],
                                sub_total=venda_details["sub_total"],
                                vlr_desc_acresc_geral=venda_details[
                                    "vlr_desc_acresc_geral"
                                ],
                                outros_valores=venda_details["outros_valores"],
                                total_liquido=venda_details["total_liquido"],
                                troca=venda_details["troca"],
                                status=venda_details.get("status", ""),
                            )

                            # Inserir ou atualizar a venda no banco de dados
                            database_service.inserir_venda(venda_obj)

                            # Processar os itens de venda
                            for item in venda_details.get("itens", []):
                                item_venda = ItemVenda(
                                    id_venda_item=item["id_venda_item"],
                                    id_produto=item["id_produto"],
                                    descricao_produto=item["descricao_produto"],
                                    valor_unitario=item["valor_unitario"],
                                    qtdade=item["qtdade"],
                                    id_venda=item["id_venda"],
                                )
                                # Inserir ou atualizar o item de venda
                                database_service.inserir_item_venda(item_venda)

                            # Processar os pagamentos
                            for pagamento in venda_details.get("pagamentos", []):
                                pagamento_obj = Pagamento(
                                    forma_pagto_ecf=pagamento["forma_pagto_ecf"],
                                    valor=pagamento["valor"],
                                    id_venda=pagamento["id_venda"],
                                )
                                # Inserir ou atualizar o pagamento
                                database_service.inserir_pagamento(pagamento_obj)

                    success = True  # Define sucesso se os dados forem processados corretamente
                    break  # Interrompe o loop se a resposta for válida
                except ValueError as e:
                    print(f"Erro ao decodificar JSON: {e}")
                    print(
                        "Texto da resposta:", response.text
                    )  # Imprime o texto da resposta para depuração
            else:
                print(
                    f"Erro na requisição para o dia {start} (Tentativa {attempts}/{max_retries}): {response.status_code}"
                )
                print(f"Resposta da API: {response.text}")  # Verifica a resposta
                time.sleep(2)  # Pausa entre as tentativas

        if not success:
            print(
                f"Falha ao obter dados para o dia {start} após {max_retries} tentativas."
            )
