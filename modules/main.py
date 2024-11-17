import asyncio
import logging
from api_requests import fetch_all_sales_data
from database_service import insert_data_to_supabase
from data_processing import generate_daily_intervals
from data_processing import process_sales_data, save_to_dataframe

# Configurações de autenticação e URL
TOKEN = "46723379751"
BASE_URL = "https://xysxl4d0mc.execute-api.sa-east-1.amazonaws.com/v1/vendas"
SUPABASE_URL = "https://ktetsgeljrqfbvbqpdlg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt0ZXRzZ2VsanJxZmJ2YnFwZGxnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMDgzNDc5MSwiZXhwIjoyMDQ2NDEwNzkxfQ.uLSXQoj8VhaCA8AnLk9RRVE7cgvufM8BhZ5jlzDQ5dg"

# Configuração de logging
logging.basicConfig(level=logging.INFO)

# Função principal de execução
async def main():
    intervals = generate_daily_intervals()
    raw_data = await fetch_all_sales_data(TOKEN, BASE_URL, intervals)
    
    if raw_data:
        vendas, clientes, itens_venda, pagamentos = process_sales_data(raw_data)
        save_to_dataframe(vendas, clientes, itens_venda, pagamentos)
        #insert_data_to_supabase(vendas, clientes, itens_venda, pagamentos, SUPABASE_URL, SUPABASE_KEY)


# Executa o script
if __name__ == "__main__":
    asyncio.run(main())
