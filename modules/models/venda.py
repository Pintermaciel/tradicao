# models/venda.py
from pydantic import BaseModel, Field
from typing import Optional


class Venda(BaseModel):
    id_venda: int
    qtdade_itens: int
    tipo: str
    id_cliente: Optional[int] = Field(default="0") 
    dt_venda: str  # Pode ser ajustado para datetime se necessário
    hr_venda: str
    situacao: str
    sub_total: float
    vlr_desc_acresc_geral: float
    outros_valores: float
    total_liquido: float
    troca: int
    status: str = Field(default="")
    
    def to_dict(self) -> dict:
        """
        Converte a instância do modelo para um dicionário.
        """
        return self.dict()
