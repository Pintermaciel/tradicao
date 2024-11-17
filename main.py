# main.py
from services.database_service import DatabaseService
from data.processor_vendas import processar_vendas

def main():
    url = "https://ktetsgeljrqfbvbqpdlg.supabase.co"
    key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imt0ZXRzZ2VsanJxZmJ2YnFwZGxnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczMDgzNDc5MSwiZXhwIjoyMDQ2NDEwNzkxfQ.uLSXQoj8VhaCA8AnLk9RRVE7cgvufM8BhZ5jlzDQ5dg"
    
    db_service = DatabaseService(url, key)
    pasta_json = 'data/json'  # Ajuste o caminho conforme necessário
    
    processar_vendas(pasta_json, db_service)

if __name__ == "__main__":
    main()