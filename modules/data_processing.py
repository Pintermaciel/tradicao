import logging
from datetime import datetime, timedelta
import pandas as pd
from models.cliente import Cliente
from models.item_venda import ItemVenda
from models.pagamento import Pagamento
from models.venda import Venda
import re

# Configuração de logging
logging.basicConfig(level=logging.INFO)

# Função para gerar intervalos diários dos últimos 5 anos
def generate_daily_intervals(years=5):
    today = datetime.today()
    intervals = []
    start_date = today

    for _ in range(years * 365):
        end_date = start_date
        intervals.append((start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        start_date -= timedelta(days=1)

    return intervals

def process_sales_data(raw_data):
    vendas = []  # Lista para armazenar as vendas processadas
    clientes = []  # Lista para armazenar os clientes processados
    itens_venda = []  # Lista para armazenar os itens de venda processados
    pagamentos = []  # Lista para armazenar os pagamentos processados
    sale_counter = 0 # Contador para as vendas processadas

    for response_text in raw_data:
        try:
            # Verifica se o item é uma string escapada e tenta processá-la
            if isinstance(response_text, str):
                # Remove as barras invertidas para lidar com a string escapada
                response_text = response_text.replace("\\", "")  # Remove as barras invertidas
                
                # Usando regex para contar as ocorrências de "Venda"
                num_vendas = len(re.findall(r'"Venda":', response_text))
                logging.info(f"Quantidade de vendas nesta string: {num_vendas}, string {sale_counter}")
                
                # Imprime o conteúdo do response_text para ajudar no diagnóstico
                logging.info(f"Conteúdo do response_text:\n{response_text[:500]}...")  # Exibe os primeiros 500 caracteres

                # Verifica se a string parece ser um JSON válido
                try:
                    pattern = r'"Venda":\s*({.*?})'

                    # Encontrar todas as ocorrências de "Venda" no texto
                    vendas_data = re.findall(pattern, response_text, re.DOTALL)

                    # Verifica se as vendas foram encontradas
                    if vendas_data:
                        logging.info(f"Vendas encontradas: {len(vendas_data)}, string {sale_counter}")
                    else:
                        logging.warning("Nenhuma venda encontrada com a regex.")

                    vendas_dict = {}
                    for venda in vendas_data:
                        # Regex para capturar o id_venda dentro de cada venda
                        id_venda_match = re.search(r'"id_venda":\s*(\d+)', venda)
                        if id_venda_match:
                            id_venda = int(id_venda_match.group(1))  # Extrai o id_venda
                            vendas_dict[id_venda] = venda  # Usa id_venda como chave no dicionário

                            # Extrair dados de cliente da venda
                            nome_cliente = re.search(r'"nome_cliente":\s*"([^"]*)"', venda)
                            fone1 = re.search(r'"fone1":\s*"([^"]*)"', venda)
                            fone2 = re.search(r'"fone2":\s*"([^"]*)"', venda)
                            cpf_cnpj = re.search(r'"cpf_cnpj":\s*"([^"]*)"', venda)
                            endereco = re.search(r'"endereco":\s*"([^"]*)"', venda)
                            cidade = re.search(r'"cidade":\s*"([^"]*)"', venda)
                            uf = re.search(r'"uf":\s*"([^"]*)"', venda)
                            bairro = re.search(r'"bairro":\s*"([^"]*)"', venda)
                            cep = re.search(r'"cep":\s*"([^"]*)"', venda)
                            data_nascimento = re.search(r'"dt_nascto":\s*"([^"]*)"', venda)

                            # Criar um objeto Cliente com as informações extraídas
                            cliente = Cliente(
                                id_cliente=re.search(r'"id_cliente":\s*"([^"]*)"', venda).group(1),
                                nome_cliente=nome_cliente.group(1) if nome_cliente else "",
                                fone1=fone1.group(1) if fone1 else "",
                                fone2=fone2.group(1) if fone2 else "",
                                cpf_cnpj=cpf_cnpj.group(1) if cpf_cnpj else "",
                                endereco=endereco.group(1) if endereco else "",
                                cidade=cidade.group(1) if cidade else "",
                                uf=uf.group(1) if uf else "",
                                bairro=bairro.group(1) if bairro else "",
                                cep=cep.group(1) if cep else "",
                                data_nascimento=data_nascimento.group(1) if data_nascimento else ""
                            )

                            # Adicionar o cliente à lista de clientes
                            clientes.append(cliente)

                            # Extrair os itens da venda (caso exista)
                            itens_venda_data = re.findall(r'"ItemVenda":\s*\{(.*?)\}', venda, re.DOTALL)
                            for item in itens_venda_data:
                                id_venda_item = re.search(r'"id_venda_item":\s*(\d+)', item)
                                id_produto = re.search(r'"id_produto":\s*(\d+)', item)
                                descricao_produto = re.search(r'"descricao_produto":\s*"([^"]*)"', item)
                                valor_unitario = re.search(r'"valor_unitario":\s*([\d\.]+)', item)
                                qtdade = re.search(r'"qtdade":\s*([\d\.]+)', item)

                                # Criar o objeto ItemVenda
                                item_venda = ItemVenda(
                                    id_venda_item=int(id_venda_item.group(1)) if id_venda_item else 0,
                                    id_produto=int(id_produto.group(1)) if id_produto else 0,
                                    descricao_produto=descricao_produto.group(1) if descricao_produto else "",
                                    valor_unitario=float(valor_unitario.group(1)) if valor_unitario else 0.0,
                                    qtdade=float(qtdade.group(1)) if qtdade else 0.0,
                                    id_venda=id_venda
                                )

                                # Adicionar à lista de itens de venda
                                itens_venda.append(item_venda)

                            # Extrair os dados de pagamento
                            pagamentos_data = re.findall(r'"Pagamento":\s*\{(.*?)\}', venda, re.DOTALL)
                            for pagamento in pagamentos_data:
                                id_venda=id_venda,
                                forma_pagto_ecf = re.search(r'"forma_pagto_ecf":\s*"([^"]*)"', pagamento)
                                valor = re.search(r'"valor":\s*([\d\.]+)', pagamento)

                                # Criar o objeto Pagamento
                                pagamento_obj = Pagamento(
                                    forma_pagto_ecf=forma_pagto_ecf.group(1) if forma_pagto_ecf else "",
                                    valor=float(valor.group(1)) if valor else 0.0,
                                    id_venda=id_venda
                                )

                                # Adicionar à lista de pagamentos
                                pagamentos.append(pagamento_obj)

                            # Agora, criar um objeto Venda com as informações da venda
                            venda_obj = Venda(
                                id_venda=id_venda,
                                qtdade_itens=len(itens_venda_data),
                                tipo=re.search(r'"tipo":\s*"([^"]*)"', venda).group(1),
                                id_cliente=re.search(r'"id_cliente":\s*"([^"]*)"', venda).group(1),
                                dt_venda=re.search(r'"dt_venda":\s*"([^"]*)"', venda).group(1),
                                hr_venda=re.search(r'"hr_venda":\s*"([^"]*)"', venda).group(1),
                                situacao=re.search(r'"situacao":\s*"([^"]*)"', venda).group(1),
                                sub_total=float(re.search(r'"sub_total":\s*([\d\.]+)', venda).group(1)),
                                vlr_desc_acresc_geral=float(re.search(r'"vlr_desc_acresc_geral":\s*([\d\.]+)', venda).group(1)),
                                outros_valores=float(re.search(r'"outros_valores":\s*([\d\.]+)', venda).group(1)),
                                total_liquido=float(re.search(r'"total_liquido":\s*([\d\.]+)', venda).group(1)),
                                troca=int(re.search(r'"troca":\s*(\d+)', venda).group(1)),
                                status=re.search(r'"status":\s*"([^"]*)"', venda).group(1)
                            )

                            # Adicionar a venda à lista de vendas
                            vendas.append(venda_obj)
                            
                            sale_counter += 1 

                except Exception as e:
                    logging.warning(f"Erro ao processar a venda: {e}")
                    logging.warning(f"Texto da venda que causou o erro: {response_text}")
        except Exception as e:
            logging.error(f"Erro ao processar os dados: {e}")

    return vendas, clientes, itens_venda, pagamentos

def save_to_dataframe(vendas, clientes, itens_venda, pagamentos, file_name="dados_processados.xlsx"):
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
        df_vendas.to_excel(writer, sheet_name='Vendas', index=False)
        df_clientes.to_excel(writer, sheet_name='Clientes', index=False)
        df_itens_venda.to_excel(writer, sheet_name='Itens de Venda', index=False)
        df_pagamentos.to_excel(writer, sheet_name='Pagamentos', index=False)
    
    # Confirmar que o arquivo foi salvo
    logging.info(f"Dados salvos no arquivo Excel: {file_name}")

    return df_vendas, df_clientes, df_itens_venda, df_pagamentos