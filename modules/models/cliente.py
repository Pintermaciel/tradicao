from pydantic import BaseModel, Field, model_validator
from typing import Optional

class Cliente(BaseModel):
    id_cliente: Optional[int] = Field(default="0")  # Tipo string e valor default "0"
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

    @model_validator(mode='before')
    def set_default_id_cliente(cls, values):
        # Se id_cliente for None ou null, define como "0"
        if values.get('id_cliente') is None:
            values['id_cliente'] = "0"
        return values
    
    def to_dict(self) -> dict:
        """
        Converte a instância do modelo para um dicionário.
        """
        return self.dict()

    class Config:
        from_attributes = True  # Permite que o Pydantic funcione com ORM
