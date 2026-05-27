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

# Stage: ingesta (Kafka producer)
FROM base AS ingesta

RUN pip install \
    confluent-kafka==2.5.3 \
    python-dotenv==1.0.1

COPY src/ingesta/ ./src/ingesta/

CMD ["python", "-m", "src.ingesta.kafka_producer"]


# Stage: batch (Spark + Delta Lake + Kafka connector)
# Requiere JDK para PySpark
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

# ── Conector Spark-Kafka (Structured Streaming) ──────────────────────────────
# Los JARs se instalan en $SPARK_HOME/jars para que sean cargados
# automáticamente sin necesidad de --packages ni acceso a Maven en runtime.
# Versiones alineadas con pyspark==3.5.3 / Scala 2.12 / kafka-clients 3.4.1
ARG SPARK_VERSION=3.5.3
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

RUN SPARK_JARS="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && MVN="https://repo1.maven.org/maven2" \
    # 1. Conector principal Spark ↔ Kafka
    && curl -fsSL "${MVN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
            -o "${SPARK_JARS}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    # 2. Proveedor de tokens Kafka (requerido por spark-sql-kafka)
    && curl -fsSL "${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
            -o "${SPARK_JARS}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar" \
    # 3. Cliente Java de Kafka
    && curl -fsSL "${MVN}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
            -o "${SPARK_JARS}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
    # 4. Pool de conexiones (dependencia transitiva de kafka-clients)
    && curl -fsSL "${MVN}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar" \
            -o "${SPARK_JARS}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"

COPY src/batch/ ./src/batch/

CMD ["python", "-m", "src.batch.spark_transform"]


# Stage: api (FastAPI + asyncpg)
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


# Stage: reportes (generador regulatorio)
FROM base AS reportes

RUN pip install \
    psycopg2-binary==2.9.10 \
    python-dotenv==1.0.1

COPY src/reportes/ ./src/reportes/

CMD ["python", "-m", "src.reportes.reporte_regulatorio"]
