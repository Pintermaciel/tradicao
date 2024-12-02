# services/data_processor.py
import json
import requests
import logging
from pathlib import Path
from models.cliente import Cliente
from models.venda import Venda
from models.item_venda import ItemVenda
from models.pagamento import Pagamento

# Configuração do logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class DataProcessor:
    def __init__(self, pasta_json):
        self.pasta_json = pasta_json
        self.api_whatsapp = (
            "http://5.161.106.90:8080/chat/whatsappNumbers/teste-pega-contato"
        )

    def processar_arquivos(self):
        todas_vendas = []
        todos_pagamentos = []
        todos_itens = []
        clientes = {}

        vendas_processadas = 0
        vendas_ignoradas = 0

        for arquivo in Path(self.pasta_json).glob("*.json"):
            try:
                with open(arquivo, "r", encoding="utf-8") as f:
                    conteudo = f.read().strip()

                logging.debug(
                    f"Conteúdo do arquivo {arquivo.name}: {conteudo[:100]}..."
                )  # Log dos primeiros 100 caracteres

                # Se o arquivo começa com "{", é um objeto com a chave "nivel"
                if conteudo.startswith("{"):
                    dados = json.loads(conteudo)
                    if "Venda" in dados:
                        vendas = dados["nivel"]  # Acessa a chave "nivel"
                        logging.info(
                            f"Arquivo {arquivo.name} contém {len(vendas)} vendas (formato nível)."
                        )

                        # Processa cada venda da lista
                        for venda in vendas:
                            if "Venda" in venda:
                                try:
                                    vendas_processadas, vendas_ignoradas = (
                                        self.processar_dados(
                                            venda["Venda"],
                                            todas_vendas,
                                            todos_pagamentos,
                                            todos_itens,
                                            clientes,
                                            vendas_processadas,
                                            vendas_ignoradas,
                                        )
                                    )
                                except Exception as e:
                                    logging.error(
                                        f"Erro ao processar venda da lista: {str(e)}"
                                    )
                                    vendas_ignoradas += 1
                    else:
                        logging.warning(
                            f"A chave 'nivel' não encontrada no arquivo {arquivo.name}."
                        )
                else:
                    # Se não é um objeto, tentamos processar como uma lista de vendas
                    try:
                        vendas = json.loads(conteudo)
                        logging.info(
                            f"Arquivo {arquivo.name} contém {len(vendas)} vendas (formato lista)."
                        )

                        # Processa cada venda da lista
                        for venda in vendas:
                            if "Venda" in venda:
                                try:
                                    vendas_processadas, vendas_ignoradas = (
                                        self.processar_dados(
                                            venda["Venda"],
                                            todas_vendas,
                                            todos_pagamentos,
                                            todos_itens,
                                            clientes,
                                            vendas_processadas,
                                            vendas_ignoradas,
                                        )
                                    )
                                except Exception as e:
                                    logging.error(
                                        f"Erro ao processar venda da lista: {str(e)}"
                                    )
                                    vendas_ignoradas += 1
                    except json.JSONDecodeError as e:
                        logging.error(
                            f"Erro ao decodificar o arquivo {arquivo.name}: {e}"
                        )
                        vendas_ignoradas += 1
                    except Exception as e:
                        logging.error(
                            f"Erro ao processar o arquivo {arquivo.name}: {e}"
                        )
                        vendas_ignoradas += 1

            except Exception as e:
                logging.error(f"Erro ao processar o arquivo {arquivo.name}: {e}")

        logging.info(f"Total de vendas processadas: {vendas_processadas}")
        logging.info(f"Total de vendas ignoradas: {vendas_ignoradas}")

        return todas_vendas, todos_pagamentos, todos_itens, clientes

    def processar_dados(
        self,
        dados,
        todas_vendas,
        todos_pagamentos,
        todos_itens,
        clientes,
        vendas_processadas,
        vendas_ignoradas,
    ):
        try:
            if not isinstance(dados, dict):
                logging.error(f"Formato de dados inválido: {type(dados)}")
                vendas_ignoradas += 1
                return vendas_processadas, vendas_ignoradas

            id_venda = dados.get("id_venda")

            logging.debug(f"Processando venda ID: {id_venda}")

            # Verificações para garantir que os campos necessários estão presentes
            qtdade_itens = dados.get("qtdade_itens")
            sub_total = dados.get("sub_total")
            total_liquido = dados.get("total_liquido")

            if qtdade_itens is None or sub_total is None or total_liquido is None:
                logging.warning(
                    f"Venda com id {id_venda} tem campos obrigatórios faltando."
                )
                vendas_ignoradas += 1
                return vendas_processadas, vendas_ignoradas

            venda_obj = Venda(**dados)
            todas_vendas.append(venda_obj)
            vendas_processadas += 1
            logging.info(f"Venda processada com sucesso: ID {id_venda}")

            # Processar pagamentos
            if "pagamentos" in dados:
                for pagamento in dados["pagamentos"]:
                    pagamento_obj = Pagamento(**pagamento, id_venda=id_venda)
                    todos_pagamentos.append(pagamento_obj)
            else:
                logging.warning(f"A venda com id {id_venda} não contém pagamentos.")

            # Processar itens
            if "itens" in dados:
                for item in dados["itens"]:
                    item["qtdade"] = float(item["qtdade"])  # Convertendo para float
                    item_obj = ItemVenda(**item, id_venda=id_venda)
                    todos_itens.append(item_obj)
            else:
                logging.warning(f"A venda com id {id_venda} não contém itens.")

            # Processar cliente
            cliente_id = dados["id_cliente"]
            nome_cliente = dados.get("nome_cliente", "")
            fone1 = self.formatar_telefone(dados.get("fone1", ""))
            fone2 = self.formatar_telefone(dados.get("fone2", ""))

            # Verificar se os números de WhatsApp são válidos
            whatsapp_valido_fone1 = self.verificar_whatsapp(fone1, id_venda)
            whatsapp_valido_fone2 = self.verificar_whatsapp(fone2, id_venda)

            cliente = Cliente(
                id_cliente=cliente_id,
                nome_cliente=nome_cliente,
                fone1=fone1 if whatsapp_valido_fone1 else "",
                fone2=fone2 if whatsapp_valido_fone2 else "",
                cpf_cnpj=dados["cpf_cnpj"],
                endereco=dados["endereco"],
                cidade=dados["cidade"],
                uf=dados["uf"],
                bairro=dados["bairro"],
                cep=dados["cep"],
            )
            clientes[cliente_id] = cliente

        except Exception as e:
            logging.error(f"Erro ao processar venda: {str(e)}")
            vendas_ignoradas += 1

        return vendas_processadas, vendas_ignoradas

    def formatar_telefone(self, telefone):
        telefone = "".join(filter(str.isdigit, telefone))
        if telefone:
            return "55" + telefone  # Adiciona o código do Brasil
        return ""

    def verificar_whatsapp(self, telefone, id_venda):
        if telefone:
            headers = {
                "apiKey": "9e093ded665f5b49"  # Adicionando o cabeçalho com a chave da API
            }
            response = requests.post(
                self.api_whatsapp, json={"numbers": [telefone]}, headers=headers
            )
            if response.status_code == 200:
                resultado = response.json()
                # Verifica se o número existe
                if (
                    resultado
                    and isinstance(resultado, list)
                    and len(resultado) > 0
                    and resultado[0].get("exists")
                ):
                    logging.info(
                        f"Número de WhatsApp válido: {telefone} na venda ID {id_venda}."
                    )
                    return True  # Número válido
                else:
                    logging.warning(
                        f"Número de WhatsApp não encontrado: {telefone} na venda ID {id_venda}."
                    )
                    return False  # Número inválido
            else:
                # Lê o corpo da resposta para entender o erro
                erro_corpo = response.text  # ou response.json() se a resposta for JSON
                logging.error(
                    f"Erro ao verificar WhatsApp para {telefone} na venda ID {id_venda}: {response.status_code}, corpo: {erro_corpo}"
                )
                return False  # Número inválido
        return False  # Telefone vazio
