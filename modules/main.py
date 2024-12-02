import asyncio
import logging
from api_requests import fetch_all_sales_data
from database_service import insert_data_to_supabase, DatabaseService
from data_processing import generate_daily_intervals, process_sales_data

# Configurações de autenticação e URL
TOKEN = "46723379751"
BASE_URL = "https://xysxl4d0mc.execute-api.sa-east-1.amazonaws.com/v1/vendas"
SUPABASE_URL = "https://ktetsgeljrqfbvbqpdlg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt0ZXRzZ2VsanJxZmJ2YnFwZGxnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMDgzNDc5MSwiZXhwIjoyMDQ2NDEwNzkxfQ.uLSXQoj8VhaCA8AnLk9RRVE7cgvufM8BhZ5jlzDQ5dg"

# Configuração de logging
logging.basicConfig(level=logging.INFO)

async def fetch_and_process_data(token, base_url, interval, supabase_url, supabase_key):
    try:
        raw_data = await fetch_all_sales_data(token, base_url, interval)

        # Processamento dos dados (exemplo)
        vendas, clientes, itens_venda, pagamentos = process_sales_data(raw_data)
        #save_to_dataframe(vendas, clientes, itens_venda, pagamentos)  # Salvar em DataFrame (opcional)

        # Inserção dos dados no Supabase
        insert_data_to_supabase(vendas, clientes, itens_venda, pagamentos, supabase_url, supabase_key)

        logging.info(f"Dados do intervalo {interval} processados e inseridos com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao processar intervalo {interval}: {e}")

async def process_and_insert_data():
    MAX_CONCURRENT_TASKS = 10
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    intervals = generate_daily_intervals()

    tasks = []
    for interval in intervals:
        async with semaphore:
            tasks.append(
                asyncio.create_task(
                    fetch_and_process_data(TOKEN, BASE_URL, interval, SUPABASE_URL, SUPABASE_KEY)
                )
            )

    await asyncio.gather(*tasks)

async def main():
    await process_and_insert_data()
    
if __name__ == "__main__":
    asyncio.run(main())