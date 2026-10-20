# Utiliza imagem oficial enxuta do Python 3.11 baseada em Debian Bookworm
FROM python:3.11-slim

# Impede o Python de gravar arquivos .pyc em disco e força saída de logs em tempo real (unbuffered)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Define o diretório de trabalho principal dentro do container
WORKDIR /app

# Cria usuário de sistema não-privilegiado (boas práticas de segurança em produção)
RUN groupadd -r etlgroup && useradd -r -g etlgroup appuser

# Copia os arquivos de definição de dependências e metadados primeiro (otimiza cache de camadas do Docker)
COPY pyproject.toml README.md Makefile ./

# Copia o código-fonte, configurações e suíte de testes
COPY config/ ./config/
COPY src/ ./src/
COPY tests/ ./tests/

# Cria os diretórios físicos para persistência das camadas Medallion e ajusta permissões
RUN mkdir -p data/bronze data/silver data/gold && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e ".[dev]" && \
    chown -R appuser:etlgroup /app

# Alterna para o usuário não-root antes de iniciar a execução
USER appuser

# Comando padrão ao iniciar o container: executa a ingestão Bronze seguida do processamento Silver/Gold
CMD ["sh", "-c", "python -m src.ingestion.extractor && python -m src.etl.processor"]