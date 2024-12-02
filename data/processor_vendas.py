# data/processor_vendas.py
from data.extractor import fetch_sales_data
from services.data_processor import DataProcessor


def processar_vendas(pasta_json, db_service):
    # Extraindo dados
    fetch_sales_data()

    # Processando dados
    processor = DataProcessor(pasta_json)
    todas_vendas, todos_pagamentos, todos_itens, clientes = (
        processor.processar_arquivos()
    )


"""    # Persistindo dados
    for cliente in clientes.values():
        db_service.inserir_cliente(cliente)

    for venda in todas_vendas:
        db_service.inserir_venda(venda)

    for item in todos_itens:
        db_service.inserir_item_venda(item)

    for pagamento in todos_pagamentos:
        db_service.inserir_pagamento(pagamento)"""
