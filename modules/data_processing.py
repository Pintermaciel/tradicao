import logging
from datetime import datetime, timedelta
import pandas as pd
from models.cliente import Cliente
from models.item_venda import ItemVenda
from models.pagamento import Pagamento
from models.venda import Venda
import json

# Configuração de logging
logging.basicConfig(level=logging.INFO)


# Função para gerar intervalos diários dos últimos 5 anos
def generate_daily_intervals(years=1):
    today = datetime.today()
    intervals = []
    start_date = today

    for _ in range(years * 365):
        end_date = start_date
        intervals.append(
            (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        )
        start_date -= timedelta(days=1)

    return intervals


def process_sales_data(raw_data):
    vendas = []  # Lista para armazenar as vendas processadas
    clientes = []  # Lista para armazenar os clientes processados
    itens_venda = []  # Lista para armazenar os itens de venda processados
    pagamentos = []  # Lista para armazenar os pagamentos processados
    sale_counter = 0  # Contador para as vendas processadas

    for response_text in raw_data:
        try:
            data = json.loads(response_text)
            num_objetos = len(data)
            print(f"Número de objetos no JSON: {num_objetos}")

            # Contagem de vendas no request
            vendas_no_request = 0  # Contador para vendas no request atual
            for venda in data:
                # Processar a venda
                id_venda = venda.get('id_venda')
                qtdade_itens = venda.get('qtdade_itens')
                tipo = venda.get('tipo')
                id_cliente = venda.get('id_cliente')
                nome_cliente = venda.get('nome_cliente', "")
                fone1 = venda.get('fone1', "")
                fone2 = venda.get('fone2', "")
                cpf_cnpj = venda.get('cpf_cnpj', "")
                endereco = venda.get('endereco', "")
                cidade = venda.get('cidade', "")
                uf = venda.get('uf', "")
                bairro = venda.get('bairro', "")
                cep = venda.get('cep', "     -")
                dt_venda = venda.get('dt_venda')
                hr_venda = venda.get('hr_venda')
                situacao = venda.get('situacao')
                sub_total = venda.get('sub_total', 0)
                vlr_desc_acresc_geral = venda.get('vlr_desc_acresc_geral', 0)
                outros_valores = venda.get('outros_valores', 0)
                total_liquido = venda.get('total_liquido', 0)
                troca = venda.get('troca', 0)
                status = venda.get('status', "")

                data_formatada = datetime.strptime(dt_venda, '%d/%m/%Y').strftime('%Y-%m-%d')

                # Criar objeto Cliente
                cliente = Cliente(
                    id_cliente=id_cliente,
                    nome_cliente=nome_cliente,
                    fone1=fone1,
                    fone2=fone2,
                    cpf_cnpj=cpf_cnpj,
                    endereco=endereco,
                    cidade=cidade,
                    uf=uf,
                    bairro=bairro,
                    cep=cep,
                    data_nascimento=venda.get('dt_nascto', "OO/OO/OOOO"),
                )

                if cliente.id_cliente != 0 and cliente.id_cliente != 670:
                    clientes.append(cliente)

                # Processar itens da venda
                for item in venda.get('itens', []):
                    id_venda_item = item.get('id_venda_item')
                    id_produto = item.get('id_produto')
                    descricao_produto = item.get('descricao_produto')
                    valor_unitario = item.get('valor_unitario', 0.0)
                    qtdade = item.get('qtdade', 0.0)

                    # Criar o objeto ItemVenda
                    item_venda = ItemVenda(
                        id_venda_item=id_venda_item,
                        id_produto=id_produto,
                        descricao_produto=descricao_produto,
                        valor_unitario=valor_unitario,
                        qtdade=qtdade,
                        id_venda=id_venda
                    )

                    itens_venda.append(item_venda)

                # Processar pagamentos
                for pagamento in venda.get('pagamentos', []):
                    forma_pagto_ecf = pagamento.get('forma_pagto_ecf', "")
                    valor = pagamento.get('valor', 0.0)

                    # Criar o objeto Pagamento
                    pagamento_obj = Pagamento(
                        forma_pagto_ecf=forma_pagto_ecf,
                        valor=valor,
                        id_venda=id_venda
                    )

                    pagamentos.append(pagamento_obj)

                # Criar objeto Venda
                venda_obj = Venda(
                    id_venda=id_venda,
                    qtdade_itens=qtdade_itens,
                    tipo=tipo,
                    id_cliente=id_cliente,
                    dt_venda=data_formatada,
                    hr_venda=hr_venda,
                    situacao=situacao,
                    sub_total=sub_total,
                    vlr_desc_acresc_geral=vlr_desc_acresc_geral,
                    outros_valores=outros_valores,
                    total_liquido=total_liquido,
                    troca=troca,
                    status=status,
                )

                vendas.append(venda_obj)
                vendas_no_request += 1  # Incrementa a contagem de vendas para o request atual
                sale_counter += 1

            # Log com o número de vendas processadas para o request
            logging.info(f"Vendas processadas neste request: {vendas_no_request}")

        except json.JSONDecodeError as e:
            logging.warning(f"Erro ao decodificar JSON: {e}")
            logging.warning(f"Texto da venda que causou o erro: {response_text}")
        except Exception as e:
            logging.warning(f"Erro ao processar a venda: {e}")

        except Exception as e:
            logging.error(f"Erro ao processar os dados: {e}")

    logging.info(f"Total de vendas processadas: {sale_counter}")
    return vendas, clientes, itens_venda, pagamentos


def save_to_dataframe(
    vendas, clientes, itens_venda, pagamentos, file_name="dados_processados.xlsx"
):
    # Convertendo as listas de objetos para DataFrames
    df_vendas = pd.DataFrame([venda.dict() for venda in vendas])
    df_clientes = pd.DataFrame([cliente.dict() for cliente in clientes])
    df_itens_venda = pd.DataFrame([item.dict() for item in itens_venda])
    df_pagamentos = pd.DataFrame([pagamento.dict() for pagamento in pagamentos])

    # Exibindo os DataFrames para verificação
    logging.info(f"DataFrame de Vendas:\n{df_vendas.head()}")
    logging.info(f"DataFrame de Clientes:\n{df_clientes.head()}")
    logging.info(f"DataFrame de Itens de Venda:\n{df_itens_venda.head()}")
    logging.info(f"DataFrame de Pagamentos:\n{df_pagamentos.head()}")

    # Salvar os DataFrames em arquivos Excel
    with pd.ExcelWriter(file_name) as writer:
        df_vendas.to_excel(writer, sheet_name="Vendas", index=False)
        df_clientes.to_excel(writer, sheet_name="Clientes", index=False)
        df_itens_venda.to_excel(writer, sheet_name="Itens de Venda", index=False)
        df_pagamentos.to_excel(writer, sheet_name="Pagamentos", index=False)

    # Confirmar que o arquivo foi salvo
    logging.info(f"Dados salvos no arquivo Excel: {file_name}")

    return df_vendas, df_clientes, df_itens_venda, df_pagamentos
