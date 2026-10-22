# Habilita avaliação adiada de anotações de tipo (PEP 563)
from __future__ import annotations

# Emissão de logs operacionais estruturados
import logging

# Manipulação de carimbos de tempo em UTC para auditoria de processamento
from datetime import UTC, datetime

# Manipulação orientada a objetos de caminhos de diretórios e arquivos
from pathlib import Path

# Anotação de tipo para dicionários de configuração
from typing import Any

# Motor de banco de dados OLAP in-process para execução de SQL analítico na camada Gold
import duckdb

# Motor de DataFrames colunar vetorizado para transformações na camada Silver
import polars as pl

# Reutiliza o carregador de configurações YAML do módulo de ingestão
from src.ingestion.extractor import load_config

# Configuração padrão de logging do módulo de transformação
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("medallion_processor")


def validate_silver_quality(df: pl.DataFrame, max_null_ratio: float) -> None:
    """Executa auditoria de qualidade verificando limites de nulos no DataFrame."""
    # Verifica se o DataFrame de entrada possui registros antes de calcular proporções
    if df.is_empty():
        raise ValueError("Falha de Qualidade: O DataFrame de entrada está vazio.")

    # Calcula a taxa de valores nulos para cada coluna do DataFrame
    total_rows = df.height
    for col_name in df.columns:
        null_ratio = df[col_name].null_count() / total_rows
        # Interrompe o pipeline se qualquer coluna exceder o limite tolerado no config.yaml
        if null_ratio > max_null_ratio:
            raise ValueError(
                f" Violação de Data Quality na coluna '{col_name}': "
                f"taxa de nulos ({null_ratio:.2%}) excede o limite ({max_null_ratio:.2%})."
            )


def transform_bronze_to_silver(
    bronze_path: Path,
    silver_path: Path,
    min_freight_value: float = 0.01,
    max_null_ratio: float = 0.05,
) -> pl.DataFrame:
    """Limpa, deduplica e enriquece os dados da camada Bronze, salvando na camada Silver."""
    # Carrega o arquivo Parquet bruto da camada Bronze em memória colunar
    df_bronze = pl.read_parquet(bronze_path)

    # Executa a barreira de qualidade de dados sobre o lote lido
    validate_silver_quality(df_bronze, max_null_ratio=max_null_ratio)

    # Inicia o encadeamento de transformações determinísticas do Polars
    df_silver = (
        df_bronze
        # Ordena por timestamp decrescente para manter o evento mais recente na deduplicação
        .sort("timestamp", descending=True)
        # Remove duplicatas de chave primária (transaction_id), preservando a ocorrência mais atual
        .unique(subset=["transaction_id"], keep="first")
        # Filtra transações canceladas e garante valor mínimo de frete parametrizado
        .filter(
            (pl.col("status") != "CANCELLED")
            & (pl.col("freight_value") >= min_freight_value)
        )
        # Engenharia de atributos: cria métricas derivadas e metadados de auditoria
        .with_columns(
            [
                # Concatena origem e destino formando o identificador da rota logística
                (pl.col("origin_hub") + pl.lit("->") + pl.col("destination_hub")).alias(
                    "route_code"
                ),
                # Calcula o custo logístico por quilograma transportado arredondado em 4 casas
                (pl.col("freight_value") / pl.col("weight_kg")).round(4).alias("cost_per_kg"),
                # Cria flag booleana de atraso (status DELAYED ou entrega acima de 72 horas)
                (
                    (pl.col("status") == "DELAYED")
                    | (pl.col("delivery_time_hours") > 72.0)
                ).alias("is_delayed"),
                # Registra o instante exato do processamento da camada Silver em UTC
                pl.lit(datetime.now(UTC)).alias("processed_at_utc"),
            ]
        )
        # Reordena pelo carimbo de tempo original para otimizar compressão por blocos no Parquet
        .sort("timestamp")
    )

    # Garante a existência do diretório data/silver/ e persiste o artefato limpo
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    df_silver.write_parquet(silver_path, compression="snappy")

    logger.info(
        "Camada Silver concluída | Linhas Entrada: %d | Linhas Saída: %d | Arquivo: %s",
        df_bronze.height,
        df_silver.height,
        silver_path,
    )
    return df_silver


def aggregate_silver_to_gold(
    silver_path: Path,
    gold_path: Path,
    duckdb_path: Path,
) -> pl.DataFrame:
    """Executa agregação analítica SQL via DuckDB sobre a camada Silver e persiste na Gold."""
    # Garante que o diretório data/gold/ exista antes de inicializar o banco DuckDB
    gold_path.parent.mkdir(parents=True, exist_ok=True)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    # Converte o caminho do Parquet para formato POSIX (barras '/') exigido pelo motor SQL DuckDB
    silver_posix = silver_path.as_posix()

    # Define a consulta analítica SQL utilizando funções de agregação e percentil contínuo
    query = f"""
    CREATE OR REPLACE TABLE gold_carrier_metrics AS
    SELECT
        carrier_id,
        origin_hub,
        COUNT(transaction_id) AS total_shipments,
        ROUND(SUM(weight_kg), 2) AS total_weight_kg,
        ROUND(SUM(freight_value), 2) AS total_revenue,
        ROUND(AVG(cost_per_kg), 4) AS avg_cost_per_kg,
        ROUND(AVG(delivery_time_hours), 2) AS avg_delivery_hours,
        ROUND(QUANTILE_CONT(delivery_time_hours, 0.95), 2) AS p95_delivery_hours,
        ROUND(AVG(CASE WHEN is_delayed THEN 1.0 ELSE 0.0 END) * 100.0, 2) AS delay_rate_pct
    FROM read_parquet('{silver_posix}')
    GROUP BY carrier_id, origin_hub
    ORDER BY total_revenue DESC;
    """

    # Abre conexão persistente com o arquivo DuckDB, materializa a tabela Gold e extrai para Polars
    with duckdb.connect(str(duckdb_path)) as conn:
        conn.execute(query)
        df_gold = conn.execute("SELECT * FROM gold_carrier_metrics").pl()

    # Exporta a tabela agregada também em formato Parquet para leitura direta por ferramentas de BI
    df_gold.write_parquet(gold_path, compression="snappy")

    logger.info(
        "Camada Gold concluída | Grupos Agregados: %d | Parquet: %s | DuckDB: %s",
        df_gold.height,
        gold_path,
        duckdb_path,
    )
    return df_gold


def main() -> None:
    """Orquestra a transformação sequencial Bronze -> Silver -> Gold."""
    # Carrega dicionário de parâmetros do config.yaml
    config: dict[str, Any] = load_config()
    storage_cfg = config["storage"]
    val_cfg = config["validation"]

    bronze_path = Path(storage_cfg["bronze_path"])
    silver_path = Path(storage_cfg["silver_path"])
    gold_path = Path(storage_cfg["gold_path"])
    duckdb_path = Path(storage_cfg["duckdb_database"])

    # Executa a transformação Silver
    transform_bronze_to_silver(
        bronze_path=bronze_path,
        silver_path=silver_path,
        min_freight_value=val_cfg["min_freight_value"],
        max_null_ratio=val_cfg["max_null_ratio"],
    )

    # Executa a agregação Gold e persistência no DuckDB
    aggregate_silver_to_gold(
        silver_path=silver_path,
        gold_path=gold_path,
        duckdb_path=duckdb_path,
    )


if __name__ == "__main__":
    main()