# models/pagamento.py
from pydantic import BaseModel

class Pagamento(BaseModel):
    forma_pagto_ecf: str
    valor: float
    id_venda: int