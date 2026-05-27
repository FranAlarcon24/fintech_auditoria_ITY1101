# =============================================================================
# FinTech Auditoría — Dockerfile multi-stage
# Cada stage genera una imagen mínima para el servicio correspondiente.
# =============================================================================

# Imagen base compartida
ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim AS base

WORKDIR /app

# Dependencias del sistema comunes (psycopg2, curl para healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .


# =============================================================================
# Stage: ingesta (Kafka producer)
# =============================================================================
FROM base AS ingesta

RUN pip install \
    confluent-kafka==2.5.3 \
    python-dotenv==1.0.1

COPY src/ingesta/ ./src/ingesta/

CMD ["python", "-m", "src.ingesta.kafka_producer"]


# =============================================================================
# Stage: batch (Spark + Delta Lake)
# Requiere JDK para PySpark
# =============================================================================
FROM python:${PYTHON_VERSION:-3.11}-slim AS batch

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jdk-headless \
        libpq-dev \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install \
    pyspark==3.5.3 \
    delta-spark==3.2.0 \
    confluent-kafka==2.5.3 \
    python-dotenv==1.0.1

COPY src/batch/ ./src/batch/

CMD ["python", "-m", "src.batch.spark_transform"]


# =============================================================================
# Stage: api (FastAPI + asyncpg)
# =============================================================================
FROM base AS api

RUN pip install \
    fastapi==0.115.5 \
    "uvicorn[standard]==0.32.1" \
    asyncpg==0.30.0 \
    pydantic==2.10.3 \
    python-dotenv==1.0.1

COPY src/servicio/ ./src/servicio/

EXPOSE 8000
CMD ["uvicorn", "src.servicio.api_gateway:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]


# =============================================================================
# Stage: reportes (generador regulatorio)
# =============================================================================
FROM base AS reportes

RUN pip install \
    psycopg2-binary==2.9.10 \
    python-dotenv==1.0.1

COPY src/reportes/ ./src/reportes/

CMD ["python", "-m", "src.reportes.reporte_regulatorio"]
