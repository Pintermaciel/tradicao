# models/item_venda.py
from pydantic import BaseModel


class ItemVenda(BaseModel):
    id_venda_item: int
    id_produto: int
    descricao_produto: str
    valor_unitario: float
    qtdade: float
    id_venda: int
    
    def to_dict(self) -> dict:
        """
        Converte a instância do modelo para um dicionário.
        """
        return self.dict()
