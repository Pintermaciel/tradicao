from supabase import create_client, Client
from models.cliente import Cliente
from models.venda import Venda
from models.item_venda import ItemVenda
from models.pagamento import Pagamento
import logging
from datetime import datetime
import time


class DatabaseService:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)

    def inserir_cliente(self, cliente: Cliente):
        # Verificar se o cliente já existe
        if self.cliente_existe(cliente.id_cliente):
            logging.info(
                f"O cliente com ID {cliente.id_cliente} já existe. Atualizando os dados."
            )
            # Atualizar os dados do cliente
            self.supabase.table("clientes").update(cliente.dict()).eq(
                "id_cliente", cliente.id_cliente
            ).execute()
            logging.info(f"Cliente com ID {cliente.id_cliente} atualizado com sucesso.")
        else:
            # Inserir o cliente no banco de dados
            self.supabase.table("clientes").insert(cliente.dict()).execute()
            logging.info(f"Cliente com ID {cliente.id_cliente} inserido com sucesso.")

    def inserir_venda(self, venda: Venda):
        # Verificar se os atributos de data estão presentes
        if hasattr(venda, "dt_venda"):
            logging.info(f"Data de venda antes da formatação: {venda.dt_venda}")
            venda.dt_venda = self.formatar_data(venda.dt_venda)
            logging.info(f"Data de venda após a formatação: {venda.dt_venda}")

        if hasattr(venda, "dt_nascto"):
            logging.info(f"Data de nascimento antes da formatação: {venda.dt_nascto}")
            venda.dt_nascto = self.formatar_data(venda.dt_nascto)
            logging.info(f"Data de nascimento após a formatação: {venda.dt_nascto}")

        # Verificar se a venda já existe
        if self.venda_existe(venda.id_venda):
            logging.info(
                f"A venda com ID {venda.id_venda} já existe. Atualizando os dados."
            )
            # Atualizar os dados da venda
            self.supabase.table("vendas").update(venda.dict()).eq(
                "id_venda", venda.id_venda
            ).execute()
            logging.info(f"Venda com ID {venda.id_venda} atualizada com sucesso.")
        else:
            # Inserir a venda no banco de dados
            self.supabase.table("vendas").insert(venda.dict()).execute()
            logging.info(f"Venda com ID {venda.id_venda} inserida com sucesso.")

    def inserir_item_venda(self, item: ItemVenda):
        # Verifica se o item já existe
        existing_item = (
            self.supabase.table("itens_venda")
            .select("id_venda_item")
            .eq("id_venda_item", item.id_venda_item)
            .execute()
        )

        if existing_item.data:
            logging.info(
                f"Item com id_venda_item {item.id_venda_item} já existe. Atualizando o item."
            )
            # Atualiza o item existente
            update_response = (
                self.supabase.table("itens_venda")
                .update(item.dict())
                .eq("id_venda_item", item.id_venda_item)
                .execute()
            )

            if update_response.data:  # Verifica se a atualização foi bem-sucedida
                logging.info(
                    f"Item com id_venda_item {item.id_venda_item} atualizado com sucesso."
                )
            else:
                logging.error(
                    f"Erro ao atualizar o item com id_venda_item {item.id_venda_item}: {update_response.data}"
                )
        else:
            # Insere o novo item
            insert_response = (
                self.supabase.table("itens_venda").insert(item.dict()).execute()
            )

            if insert_response.data:  # Verifica se a inserção foi bem-sucedida
                logging.info(
                    f"Item com id_venda_item {item.id_venda_item} inserido com sucesso."
                )
            else:
                logging.error(
                    f"Erro ao inserir o item com id_venda_item {item.id_venda_item}: {insert_response.data}"
                )

    def inserir_pagamento(self, pagamento: Pagamento):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Verifica se o pagamento já existe
                existing_payment = (
                    self.supabase.table("pagamentos")
                    .select("id_venda")
                    .eq("id_venda", pagamento.id_venda)
                    .execute()
                )

                if existing_payment.data:
                    logging.info(
                        f"Pagamento para id_venda {pagamento.id_venda} já existe. Atualizando o pagamento."
                    )
                    # Atualiza o pagamento existente
                    update_response = (
                        self.supabase.table("pagamentos")
                        .update(pagamento.dict())
                        .eq("id_venda", pagamento.id_venda)
                        .execute()
                    )

                    if (
                        update_response.data
                    ):  # Verifica se a atualização foi bem-sucedida
                        logging.info(
                            f"Pagamento para id_venda {pagamento.id_venda} atualizado com sucesso."
                        )
                    else:
                        logging.error(
                            f"Erro ao atualizar o pagamento para id_venda {pagamento.id_venda}: {update_response.data}"
                        )
                else:
                    # Insere o novo pagamento
                    insert_response = (
                        self.supabase.table("pagamentos")
                        .insert(pagamento.dict())
                        .execute()
                    )

                    if insert_response.data:  # Verifica se a inserção foi bem-sucedida
                        logging.info(
                            f"Pagamento para id_venda {pagamento.id_venda} inserido com sucesso."
                        )
                    else:
                        logging.error(
                            f"Erro ao inserir o pagamento para id_venda {pagamento.id_venda}: {insert_response.data}"
                        )
                break  # Se a operação foi bem-sucedida, sai do loop
            except Exception as e:
                logging.error(f"Erro ao inserir/atualizar pagamento: {e}")
                if attempt < max_retries - 1:
                    logging.info(
                        f"Tentando novamente em 2 segundos... (Tentativa {attempt + 1})"
                    )
                    time.sleep(2)  # Espera antes de tentar novamente
                else:
                    logging.error(
                        "Máximo de tentativas atingido. Não foi possível inserir/atualizar o pagamento."
                    )

    def dados_diferentes(self, id_venda, novos_dados):
        # Obter os dados existentes
        resultado = (
            self.supabase.table("vendas").select("*").eq("id_venda", id_venda).execute()
        )

        if resultado.data:
            dados_existentes = resultado.data[
                0
            ]  # Assume que o ID é único e retorna apenas um registro

            # Comparar os dados
            for key, value in novos_dados.items():
                if dados_existentes.get(key) != value:
                    return True  # Retorna True se houver diferenças
        return False  # Retorna False se os dados forem iguais

    def venda_existe(self, id_venda):
        resultado = (
            self.supabase.table("vendas")
            .select("id_venda")
            .eq("id_venda", id_venda)
            .execute()
        )
        return len(resultado.data) > 0  # Retorna True se a venda existir

    def cliente_existe(self, id_cliente):
        resultado = (
            self.supabase.table("clientes")
            .select("id_cliente")
            .eq("id_cliente", id_cliente)
            .execute()
        )
        return len(resultado.data) > 0  # Retorna True se o cliente existir

    def formatar_data(self, data_str: str):
        try:
            # Converter a string de data do formato DD/MM/YYYY para YYYY-MM-DD
            data = datetime.strptime(data_str, "%d/%m/%Y")
            return data.strftime("%Y-%m-%d")  # Retorna no formato YYYY-MM-DD
        except ValueError as e:
            logging.error(f"Erro ao formatar a data: {e}")
            return None  # Ou lance uma exceção, dependendo do seu caso de uso
