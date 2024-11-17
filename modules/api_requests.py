import asyncio
import aiohttp
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO)

# Função assíncrona para fazer a requisição à API
async def fetch_sales_data(session, token, base_url, start_date, end_date):
    try:
        params = {"token": token, "data_venda": f"{start_date},{end_date}"}
        async with session.get(base_url, params=params) as response:
            if response.status == 200:
                # Recebe o conteúdo como texto para processamento posterior
                raw_data = await response.text()
                logging.info(f"Dados brutos recebidos para o período {start_date} a {end_date}")
                return raw_data  # Retorna o texto bruto para ser tratado em outro módulo
            else:
                logging.error(f"Erro na requisição para {start_date} a {end_date}: {response.status}")
                return None
    except Exception as e:
        logging.error(f"Erro ao fazer requisição: {e}")
        return None

# Função para orquestrar todas as requisições de forma assíncrona
async def fetch_all_sales_data(token, base_url, intervals):
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_sales_data(session, token, base_url, start, end)
            for start, end in intervals
        ]
        results = await asyncio.gather(*tasks)
        
        # Filtrando apenas resultados válidos e calculando o total de registros
        raw_responses = [result for result in results if result]
        logging.info(f"Total de respostas brutas retornadas: {len(raw_responses)}")
        
        return raw_responses  # Retorna a lista de respostas brutas para tratamento externo
