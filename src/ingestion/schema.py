# Habilita avaliação adiada de anotações de tipo (PEP 563)
from __future__ import annotations

# Classe padrão para manipulação e validação de timestamps ISO 8601
from datetime import datetime

# Restringe valores de string a um conjunto finito de estados permitidos
from typing import Literal

# Componentes nucleares do Pydantic v2 para modelagem e validação de campos
from pydantic import BaseModel, Field, field_validator


# Classe de contrato de dados que atua como barreira de entrada (Schema Enforcement) na camada Bronze
class TransactionRecord(BaseModel):
    # Identificador único da transação logística (compatível com UUIDv4)
    transaction_id: str = Field(..., min_length=8, max_length=36)

    # Data e hora do evento; converte strings ISO válidas para objetos datetime
    timestamp: datetime

    # Código do centro de distribuição de origem (2 a 10 caracteres)
    origin_hub: str = Field(..., min_length=2, max_length=10)

    # Código do centro de distribuição de destino (2 a 10 caracteres)
    destination_hub: str = Field(..., min_length=2, max_length=10)

    # Identificador da transportadora validado por expressão regular (ex: CARR-001)
    carrier_id: str = Field(..., pattern=r"^CARR-[0-9]{3}$")

    # Peso da carga em kg: estritamente maior que 0.0 e menor ou igual a 50.000
    weight_kg: float = Field(..., gt=0.0, le=50000.0)

    # Valor financeiro do frete: impede registros zerados ou negativos
    freight_value: float = Field(..., gt=0.0)

    # Tempo total de entrega em horas: intervalo fechado entre 0.5h e 2400h
    delivery_time_hours: float = Field(..., ge=0.5, le=2400.0)

    # Status operacional restrito aos 4 estados de negócio listados
    status: Literal["DELIVERED", "IN_TRANSIT", "DELAYED", "CANCELLED"]

    # Aplica higienização nos campos de hub antes da persistência
    @field_validator("origin_hub", "destination_hub")
    @classmethod
    def normalize_hub_code(cls, value: str) -> str:
        # Remove espaços nas extremidades e converte para caixa alta
        return value.strip().upper()