import asyncio
import aiohttp
import logging
import random

# Configuração do logging
logging.basicConfig(level=logging.INFO)


# Função assíncrona para fazer a requisição à API
async def fetch_sales_data(session, token, base_url, start_date, end_date,
                        max_retries=5, base_backoff=2, jitter=0.5):

    for attempt in range(max_retries):
        try:
            params = {"token": token, "data_venda": f"{start_date},{end_date}"}
            async with session.get(base_url, params=params) as response:
                if response.status == 200:
                    raw_data = await response.text()
                    logging.info(f"Dados brutos recebidos para o período {start_date} a {end_date}")
                    return raw_data
                else:
                    logging.error(f"Erro na requisição para {start_date} a {end_date}: {response.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.error(f"Erro de rede na requisição (tentativa {attempt+1}): {e}")
        except Exception as e:
            logging.error(f"Erro inesperado na requisição (tentativa {attempt+1}): {e}")

        wait_time = base_backoff ** attempt * (1 + random.uniform(0, jitter))
        await asyncio.sleep(wait_time)

    return None, None


# Função para orquestrar todas as requisições de forma assíncrona
async def fetch_all_sales_data(token, base_url, interval):
    async with aiohttp.ClientSession() as session:
        start_date, end_date = interval
        tasks = [
            fetch_sales_data(session, token, base_url, start_date, end_date)
        ]
        results = await asyncio.gather(*tasks)

        # Filtrando apenas resultados válidos e calculando o total de registros
        raw_responses = [result for result in results if result]
        logging.info(f"Total de respostas brutas retornadas: {len(raw_responses)}")

        return (
            raw_responses  # Retorna a lista de respostas brutas para tratamento externo
        )
