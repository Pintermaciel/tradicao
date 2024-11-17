# models/cliente.py
from pydantic import BaseModel, Field

class Cliente(BaseModel):
    id_cliente: str = Field(min_length=1)  # ID do cliente deve ter pelo menos 1 caractere
    nome_cliente: str = Field(default="")
    fone1: str = Field(default="")
    fone2: str = Field(default="")
    cpf_cnpj: str = Field(default="")
    endereco: str = Field(default="")
    data_nascimento: str = Field(default="")
    cidade: str = Field(default="")
    uf: str = Field(default="")
    bairro: str = Field(default="")
    cep: str = Field(default="")

    class Config:
        orm_mode = True  # Permite que o Pydantic funcione com ORM