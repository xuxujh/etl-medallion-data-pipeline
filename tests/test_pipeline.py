# Habilita suporte a anotações modernas de tipo
from __future__ import annotations

# Manipulação de caminhos de diretórios temporários injetados pelo pytest
from pathlib import Path

# Cliente DuckDB para validar se a tabela física foi criada corretamente na camada Gold
import duckdb

# Motor Polars para inspeção de asserções nos testes
import polars as pl

# Framework de testes para captura de exceções esperadas (pytest.raises)
import pytest

# Exceção de validação disparada pelo contrato Pydantic
from pydantic import ValidationError

# Importa as funções de transformação das camadas Silver e Gold
from src.etl.processor import (
    aggregate_silver_to_gold,
    transform_bronze_to_silver,
    validate_silver_quality,
)

# Importa as funções de geração e persistência da camada Bronze
from src.ingestion.extractor import generate_raw_batch, validate_and_persist_bronze

# Importa o modelo de contrato de dados da camada Bronze
from src.ingestion.schema import TransactionRecord


def test_transaction_record_normalizes_hub_codes() -> None:
    """Verifica se o contrato Pydantic higieniza espaços e converte hubs para maiúsculas."""
    payload = {
        "transaction_id": "tx-1234567890",
        "timestamp": "2026-01-01T10:00:00+00:00",
        "origin_hub": "  sp-01 ",
        "destination_hub": "rj-01  ",
        "carrier_id": "CARR-005",
        "weight_kg": 100.0,
        "freight_value": 500.0,
        "delivery_time_hours": 24.0,
        "status": "DELIVERED",
    }
    record = TransactionRecord(**payload)
    # Assegura que o validador customizado removeu espaços e aplicou uppercase
    assert record.origin_hub == "SP-01"
    assert record.destination_hub == "RJ-01"


def test_transaction_record_rejects_invalid_data() -> None:
    """Garante que o schema bloqueie códigos de transportadora fora do padrão ou peso negativo."""
    invalid_payload = {
        "transaction_id": "tx-1234567890",
        "timestamp": "2026-01-01T10:00:00+00:00",
        "origin_hub": "SP-01",
        "destination_hub": "RJ-01",
        "carrier_id": "INVALID-99",  # Viola regex ^CARR-[0-9]{3}$
        "weight_kg": -10.0,          # Viola restrição gt=0.0
        "freight_value": 250.0,
        "delivery_time_hours": 12.0,
        "status": "DELIVERED",
    }
    # O teste passa somente se o Pydantic disparar ValidationError
    with pytest.raises(ValidationError):
        TransactionRecord(**invalid_payload)


def test_bronze_ingestion_filters_corrupted_rows(tmp_path: Path) -> None:
    """Valida se o extrator Bronze descarta registros corrompidos e persiste apenas os válidos."""
    # Gera 10 registros válidos determinísticos
    raw_batch = generate_raw_batch(batch_size=10, seed=99)
    # Injeta 1 registro corrompido (frete negativo) no lote
    corrupted_row = dict(raw_batch[0])
    corrupted_row["freight_value"] = -50.0
    raw_batch.append(corrupted_row)

    bronze_file = tmp_path / "bronze" / "test_raw.parquet"
    saved_count = validate_and_persist_bronze(raw_records=raw_batch, output_path=bronze_file)

    # Dos 11 registros enviados, exatamente 10 devem ser persistidos
    assert saved_count == 10
    assert bronze_file.exists()
    df_saved = pl.read_parquet(bronze_file)
    assert df_saved.height == 10
    assert "ingested_at_utc" in df_saved.columns


def test_silver_quality_raises_on_excessive_nulls() -> None:
    """Confirma que a barreira de Data Quality interrompe o pipeline se houver excesso de nulos."""
    df_with_nulls = pl.DataFrame({"col_a": [1, 2, None, None], "col_b": [10, 20, 30, 40]})
    # 50% de nulos em col_a deve disparar ValueError quando o limite é 5% (0.05)
    with pytest.raises(ValueError, match="Violação de Data Quality"):
        validate_silver_quality(df_with_nulls, max_null_ratio=0.05)


def test_silver_and_gold_end_to_end_pipeline(tmp_path: Path) -> None:
    """Testa a transformação Silver (deduplicação/filtro/enriquecimento) e a agregação Gold."""
    bronze_file = tmp_path / "bronze" / "raw.parquet"
    silver_file = tmp_path / "silver" / "clean.parquet"
    gold_file = tmp_path / "gold" / "agg.parquet"
    duckdb_file = tmp_path / "gold" / "warehouse.duckdb"

    # Cria lote controlado com 1 duplicata de ID, 1 registro CANCELLED e 2 válidos distintos
    controlled_records = [
        {
            "transaction_id": "uuid-duplicate-001",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "origin_hub": "SP-01",
            "destination_hub": "RJ-01",
            "carrier_id": "CARR-001",
            "weight_kg": 100.0,
            "freight_value": 300.0,
            "delivery_time_hours": 20.0,
            "status": "IN_TRANSIT",
        },
        {
            # Mesmo ID com timestamp mais recente e status atualizado para DELIVERED
            "transaction_id": "uuid-duplicate-001",
            "timestamp": "2026-01-01T12:00:00+00:00",
            "origin_hub": "SP-01",
            "destination_hub": "RJ-01",
            "carrier_id": "CARR-001",
            "weight_kg": 100.0,
            "freight_value": 300.0,
            "delivery_time_hours": 24.0,
            "status": "DELIVERED",
        },
        {
            # Registro com atraso (> 72h) na mesma transportadora e origem
            "transaction_id": "uuid-valid-002",
            "timestamp": "2026-01-01T14:00:00+00:00",
            "origin_hub": "SP-01",
            "destination_hub": "PR-01",
            "carrier_id": "CARR-001",
            "weight_kg": 50.0,
            "freight_value": 200.0,
            "delivery_time_hours": 80.0,
            "status": "DELIVERED",
        },
        {
            # Registro cancelado que deve ser removido na camada Silver
            "transaction_id": "uuid-cancelled-003",
            "timestamp": "2026-01-01T15:00:00+00:00",
            "origin_hub": "SP-01",
            "destination_hub": "MG-01",
            "carrier_id": "CARR-001",
            "weight_kg": 20.0,
            "freight_value": 100.0,
            "delivery_time_hours": 10.0,
            "status": "CANCELLED",
        },
    ]

    # 1. Executa persistência Bronze
    validate_and_persist_bronze(raw_records=controlled_records, output_path=bronze_file)

    # 2. Executa transformação Silver
    df_silver = transform_bronze_to_silver(bronze_path=bronze_file, silver_path=silver_file)

    # Devem restar apenas 2 registros (1 deduplicado + 1 válido; o CANCELLED foi removido)
    assert df_silver.height == 2
    assert "route_code" in df_silver.columns
    assert "cost_per_kg" in df_silver.columns
    assert "is_delayed" in df_silver.columns

    # Verifica o cálculo exato de cost_per_kg (300.0 / 100.0 = 3.0) para o registro deduplicado
    row_dedup = df_silver.filter(pl.col("transaction_id") == "uuid-duplicate-001").row(0, named=True)
    assert row_dedup["status"] == "DELIVERED"
    assert row_dedup["cost_per_kg"] == 3.0
    assert row_dedup["route_code"] == "SP-01->RJ-01"
    assert row_dedup["is_delayed"] is False

    # 3. Executa agregação Gold no DuckDB
    df_gold = aggregate_silver_to_gold(
        silver_path=silver_file,
        gold_path=gold_file,
        duckdb_path=duckdb_file,
    )

    # Ambos os registros válidos pertencem a CARR-001 e SP-01, resultando em 1 linha agregada
    assert df_gold.height == 1
    gold_row = df_gold.row(0, named=True)
    assert gold_row["total_shipments"] == 2
    assert gold_row["total_weight_kg"] == 150.0
    assert gold_row["total_revenue"] == 500.0
    assert gold_row["delay_rate_pct"] == 50.0  # 1 atrasado (>72h) em 2 envios = 50%

    # Verifica se a tabela foi persistida fisicamente dentro do banco DuckDB
    with duckdb.connect(str(duckdb_file)) as conn:
        count_db = conn.execute("SELECT COUNT(*) FROM gold_carrier_metrics").fetchone()[0]
        assert count_db == 1