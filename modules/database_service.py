from supabase import create_client, Client
import logging
import pandas as pd

class DatabaseService:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)

    async def get_racao_purchase_data(self):
        # Consulta SQL ajustada para o Supabase, se necessário
        sql = """
        SELECT * FROM racao_purchase_data
        """

        try:
            # Use async methods for Supabase client
            response = await self.supabase.table("racao_purchase_data").select("*").execute()
        except Exception as e:
            logging.error(f"Erro ao executar a consulta: {e}")
            return None

        # Criar um DataFrame com os resultados
        df = pd.DataFrame(response.data)
        df.to_csv('verificar.csv', index=False)
        return df

    def inserir_ou_atualizar_em_lote(self, tabela: str, dados: list[dict], chave: str):
        try:
            if not dados:
                logging.warning(f"Dados vazios para a tabela '{tabela}', operação ignorada.")
                return

            if not all(isinstance(item, dict) for item in dados):
                raise ValueError("Os dados devem ser uma lista de dicionários.")

            response = self.supabase.table(tabela).upsert(dados, on_conflict=[chave]).execute()
            if response.data is not None:
                logging.info(f"Dados inseridos/atualizados na tabela '{tabela}' com sucesso.")
            else:
                logging.error(f"Erro ao inserir/atualizar na tabela '{tabela}': {response.error_message}")
        except Exception as e:
            logging.error(f"Erro ao inserir/atualizar em lote na tabela '{tabela}': {str(e)}")


    def inserir_clientes(self, clientes: list[dict]):
        clientes = self.remover_duplicatas_por_chave(clientes, "id_cliente")
        self.inserir_ou_atualizar_em_lote("clientes", clientes, "id_cliente")
        
    def remover_duplicatas_por_chave(self,dados, chave):
        seen = set()
        return [item for item in dados if item[chave] not in seen and not seen.add(item[chave])]


    def inserir_vendas(self, vendas: list[dict]):
        self.inserir_ou_atualizar_em_lote("vendas", vendas, "id_venda")

    def inserir_itens_venda(self, itens: list[dict]):
        self.inserir_ou_atualizar_em_lote("itens_venda", itens, "id_venda_item")

    def inserir_pagamentos(self, pagamentos: list[dict]):
        self.inserir_ou_atualizar_em_lote("pagamentos", pagamentos, "id_venda")

def insert_data_to_supabase(
    vendas, clientes, itens_venda, pagamentos, supabase_url, supabase_key
):
    db_service = DatabaseService(supabase_url, supabase_key)

    # Converter dados para dicionários, se necessário
    clientes = [cliente.to_dict() if hasattr(cliente, "to_dict") else cliente for cliente in clientes]
    vendas = [venda.to_dict() if hasattr(venda, "to_dict") else venda for venda in vendas]
    itens_venda = [item.to_dict() if hasattr(item, "to_dict") else item for item in itens_venda]
    pagamentos = [pagamento.to_dict() if hasattr(pagamento, "to_dict") else pagamento for pagamento in pagamentos]

    # Inserir/atualizar em lote
    db_service.inserir_clientes(clientes)
    db_service.inserir_vendas(vendas)
    db_service.inserir_itens_venda(itens_venda)
    #db_service.inserir_pagamentos(pagamentos)
