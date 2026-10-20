# Habilita suporte moderno de tipagem estática
from __future__ import annotations

# Emissão de logs estruturados no console
import logging

# Geração de números pseudoaleatórios controlados por seed
import random

# Geração de identificadores únicos universais (UUID)
import uuid

# Manipulação de datas, intervalos de tempo e fusos horários (UTC)
from datetime import datetime, timedelta, timezone

# Manipulação segura de caminhos de sistema de arquivos entre diferentes SOs
from pathlib import Path

# Tipo genérico para anotação de dicionários de configuração e payloads dinâmicos
from typing import Any

# Motor de DataFrame colunar para serialização em Parquet
import polars as pl

# Leitura do arquivo de parâmetros config/config.yaml
import yaml

# Exceção disparada pelo Pydantic quando um registro viola o contrato de dados
from pydantic import ValidationError

# Importa o contrato de dados definido no módulo vizinho
from src.ingestion.schema import TransactionRecord

# Configura o formato global de logging (Timestamp | Nível | Módulo | Mensagem)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
# Instancia o logger específico para rastrear eventos da camada Bronze
logger = logging.getLogger("bronze_extractor")


def load_config(config_path: str = "config/config.yaml") -> dict[str, Any]:
    """Carrega os parâmetros de execução a partir de um arquivo YAML externo."""
    # Abre o arquivo em modo leitura com codificação UTF-8 explícita
    with open(config_path, "r", encoding="utf-8") as file:
        # Converte o YAML em um dicionário Python
        return yaml.safe_load(file)


def generate_raw_batch(batch_size: int, seed: int) -> list[dict[str, Any]]:
    """Simula a extração de um lote de eventos transacionais de uma API."""
    # Fixa a semente aleatória para reprodutibilidade exata dos dados
    random.seed(seed)
    # Lista de centros logísticos operacionais
    hubs = ["SP-01", "SP-02", "RJ-01", "PR-01", "MG-01", "SC-01"]
    # Gera códigos padronizados de transportadoras de CARR-001 até CARR-014
    carriers = [f"CARR-{i:03d}" for i in range(1, 15)]
    # Estados possíveis do ciclo de vida do pedido
    statuses = ["DELIVERED", "IN_TRANSIT", "DELAYED", "CANCELLED"]
    # Define o marco temporal inicial em UTC
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    # Inicializa a lista que armazenará os dicionários brutos
    records: list[dict[str, Any]] = []
    # Itera até atingir o volume de lote parametrizado no config.yaml
    for i in range(batch_size):
        # Seleciona aleatoriamente o hub de origem
        origin = random.choice(hubs)
        # Seleciona o hub de destino garantindo que seja diferente da origem
        destination = random.choice([h for h in hubs if h != origin])
        # Sorteia o peso da carga em kg com duas casas decimais
        weight = round(random.uniform(1.5, 1200.0), 2)
        # Calcula o valor do frete como função linear do peso mais taxa base
        freight = round(weight * random.uniform(2.5, 8.0) + random.uniform(15.0, 120.0), 2)
        # Adiciona o payload bruto na lista de registros
        records.append(
            {
                # Gera um UUID determinístico derivado da seed
                "transaction_id": str(uuid.UUID(int=random.getrandbits(128), version=4)),
                # Incrementa o timestamp em intervalos de 5 minutos por registro
                "timestamp": (base_time + timedelta(minutes=i * 5)).isoformat(),
                "origin_hub": origin,
                "destination_hub": destination,
                "carrier_id": random.choice(carriers),
                "weight_kg": weight,
                "freight_value": freight,
                "delivery_time_hours": round(random.uniform(2.0, 168.0), 2),
                # Sorteia o status aplicando pesos de probabilidade realistas
                "status": random.choices(statuses, weights=[0.70, 0.15, 0.10, 0.05])[0],
            }
        )
    return records


def validate_and_persist_bronze(raw_records: list[dict[str, Any]], output_path: Path) -> int:
    """Valida lote bruto contra o contrato Pydantic e persiste em formato Parquet."""
    # Lista para acumular apenas os registros aprovados na validação
    valid_rows: list[dict[str, Any]] = []
    # Contador de auditoria para registros rejeitados
    rejected_count = 0

    # Valida registro por registro
    for item in raw_records:
        try:
            # Instancia o modelo Pydantic; dispara ValidationError se inválido
            validated = TransactionRecord(**item)
            # Converte o objeto validado para dicionário serializável
            valid_rows.append(validated.model_dump())
        except ValidationError as err:
            # Contabiliza rejeição sem interromper o lote (Dead Letter lógica)
            rejected_count += 1
            logger.debug("Registro rejeitado na camada Bronze: %s", err)

    # Garante que o diretório físico data/bronze/ exista antes da escrita
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Converte a lista de dicionários validados em um DataFrame colunar Polars
    df_bronze = pl.DataFrame(valid_rows)
    # Adiciona coluna de metadados registrando o instante da ingestão em UTC
    df_bronze = df_bronze.with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("ingested_at_utc")
    )
    # Persiste o DataFrame em formato Apache Parquet com compressão Snappy
    df_bronze.write_parquet(output_path, compression="snappy")

    # Registra métricas operacionais da execução no console
    logger.info(
        "Camada Bronze concluída | Válidos: %d | Rejeitados: %d | Arquivo: %s",
        len(valid_rows),
        rejected_count,
        output_path,
    )
    # Retorna o total de linhas gravadas para verificação em testes
    return len(valid_rows)


def main() -> None:
    """Ponto de entrada principal para execução via linha de comando."""
    # Lê as configurações do arquivo YAML
    config = load_config()
    # Extrai parâmetros específicos do dicionário de configuração
    batch_size = config["pipeline"]["batch_size"]
    seed = config["pipeline"]["random_seed"]
    bronze_path = Path(config["storage"]["bronze_path"])

    # Executa extração e persistência
    raw_batch = generate_raw_batch(batch_size=batch_size, seed=seed)
    validate_and_persist_bronze(raw_records=raw_batch, output_path=bronze_path)


if __name__ == "__main__":
    main()