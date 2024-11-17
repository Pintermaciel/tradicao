from supabase import create_client
import logging

# Conexão com o Supabase
def get_supabase_client(url, key):
    return create_client(url, key)

# Função para inserir ou atualizar dados no Supabase
def insert_data_to_supabase(data, supabase_url, supabase_key):
    supabase = get_supabase_client(supabase_url, supabase_key)
    
    for item in data:
        existing_data = supabase.table("vendas").select("*").eq("id_venda", item["id_venda"]).execute()
        if existing_data.data:
            supabase.table("vendas").update(item).eq("id_venda", item["id_venda"]).execute()
            logging.info(f"Venda com ID {item['id_venda']} atualizada com sucesso.")
        else:
            supabase.table("vendas").insert(item).execute()
            logging.info(f"Venda com ID {item['id_venda']} inserida com sucesso.")
