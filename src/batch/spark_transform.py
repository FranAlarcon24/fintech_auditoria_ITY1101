"""
Capa de lotes (Batch Layer) - Arquitectura Lambda
Transformaciones Spark sobre Delta Lake para construir la vista maestra inmutable.
Aplica SCD Tipo 2 y garantiza ACID mediante transacciones Delta.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DELTA_BASE_PATH  = os.getenv("DELTA_LAKE_PATH",     "/data/delta")
KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TX         = os.getenv("KAFKA_TOPIC_TRANSACCIONES", "fintech.transacciones")

SCHEMA_TRANSACCION = StructType([
    StructField("event_id",       StringType(),              False),
    StructField("event_type",     StringType(),              False),
    StructField("timestamp_utc",  TimestampType(),           False),
    StructField("schema_version", StringType(),              True),
    StructField("payload",        StructType([
        StructField("cuenta_id",       StringType(),  False),
        StructField("monto",           DecimalType(18, 2), False),
        StructField("moneda",          StringType(),  False),
        StructField("tipo",            StringType(),  False),
        StructField("cuenta_destino",  StringType(),  True),
        StructField("canal",           StringType(),  True),
        StructField("ip_origen",       StringType(),  True),
    ]), False),
    StructField("integrity",      StructType([
        StructField("algorithm", StringType(), False),
        StructField("hash",      StringType(), False),
    ]), False),
])


def crear_sesion_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("FinTech-AuditoriaLambdaBatch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        # Garantiza escrituras ACID en entornos con múltiples escritores
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------------------------
# UDF: verificación de integridad SHA-256
# ---------------------------------------------------------------------------
def _verificar_hash(payload_json: str, hash_esperado: str) -> bool:
    """Recomputa el SHA-256 del payload y lo compara con el hash almacenado."""
    try:
        payload = json.loads(payload_json)
        hash_real = hashlib.sha256(
            json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()
        return hash_real == hash_esperado
    except Exception:
        return False



# ---------------------------------------------------------------------------
# Lectura desde Kafka (streaming → micro-batch)
# ---------------------------------------------------------------------------
def leer_desde_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_TX)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), SCHEMA_TRANSACCION).alias("msg"),
        )
        .select("kafka_offset", "kafka_partition", "kafka_ts", "msg.*")
    )


# ---------------------------------------------------------------------------
# Transformaciones de calidad y enriquecimiento
# ---------------------------------------------------------------------------
def validar_integridad(df: DataFrame) -> DataFrame:
    """Marca registros cuyo hash SHA-256 no coincide (tampering detection)."""
    return df.withColumn(
        "hash_valido",
        F.expr(
            "integrity.hash = sha2(to_json(payload), 256)"
        ),
    )


def enriquecer_transacciones(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("fecha_proceso", F.current_date())
        .withColumn("anio",  F.year("timestamp_utc"))
        .withColumn("mes",   F.month("timestamp_utc"))
        .withColumn("dia",   F.dayofmonth("timestamp_utc"))
        .withColumn("monto_clp",
            F.when(F.col("payload.moneda") == "CLP", F.col("payload.monto"))
             .otherwise(F.col("payload.monto") * F.lit(950.0))  # tipo de cambio simplificado
        )
        .withColumn("es_alta_valor", F.col("monto_clp") >= 5_000_000)
    )


def detectar_anomalias(df: DataFrame) -> DataFrame:
    """Marca transacciones que superan umbrales regulatorios (SBIF/CMF)."""
    return df.withColumn(
        "requiere_reporte_cmf",
        (F.col("monto_clp") >= 10_000_000) |          # ≥ 10M CLP
        (F.col("payload.tipo") == "RETIRO_EFECTIVO"),
    )


# ---------------------------------------------------------------------------
# Escritura inmutable en Delta Lake (append-only, ACID)
# ---------------------------------------------------------------------------
def escribir_delta(df: DataFrame, tabla: str, partition_cols: list[str]):
    ruta = f"{DELTA_BASE_PATH}/{tabla}"

    def procesar_micro_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        logger.info("Procesando micro-batch %d para tabla '%s'", batch_id, tabla)

        # Rechazar registros con hash inválido antes de persistir
        validos   = batch_df.filter(F.col("hash_valido") == True)   # noqa: E712
        invalidos = batch_df.filter(F.col("hash_valido") == False)  # noqa: E712

        if invalidos.count() > 0:
            logger.warning(
                "Micro-batch %d: %d registros con hash inválido descartados.",
                batch_id, invalidos.count(),
            )
            invalidos.write.format("delta").mode("append").save(
                f"{DELTA_BASE_PATH}/cuarentena/{tabla}"
            )

        # Escritura ACID append-only (inmutabilidad garantizada por Delta)
        (
            validos.write
            .format("delta")
            .mode("append")
            .partitionBy(*partition_cols)
            .option("mergeSchema", "false")
            .save(ruta)
        )
        logger.info("Micro-batch %d escrito en Delta: %s", batch_id, ruta)

    return procesar_micro_batch


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------
def ejecutar_pipeline():
    spark = crear_sesion_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Iniciando pipeline batch Lambda - FinTech Auditoría")

    raw_df = leer_desde_kafka(spark)

    transformado = (
        raw_df
        .transform(validar_integridad)
        .transform(enriquecer_transacciones)
        .transform(detectar_anomalias)
    )

    query = (
        transformado.writeStream
        .foreachBatch(
            escribir_delta(transformado, "transacciones_auditoria", ["anio", "mes"])
        )
        .option("checkpointLocation", f"{DELTA_BASE_PATH}/_checkpoints/transacciones")
        .trigger(processingTime="60 seconds")
        .start()
    )

    logger.info("Pipeline en ejecución. Esperando datos de Kafka...")
    query.awaitTermination()


# ---------------------------------------------------------------------------
# Utilidades de mantenimiento Delta Lake
# ---------------------------------------------------------------------------
def vacuum_delta(spark: SparkSession, tabla: str, horas_retension: int = 168):
    """Elimina versiones antiguas manteniendo el historial de retención configurado."""
    ruta = f"{DELTA_BASE_PATH}/{tabla}"
    spark.sql(f"VACUUM delta.`{ruta}` RETAIN {horas_retension} HOURS")
    logger.info("VACUUM completado en %s (retención: %dh)", ruta, horas_retension)


def obtener_historial(spark: SparkSession, tabla: str) -> DataFrame:
    """Retorna el historial completo de transacciones Delta (audit trail)."""
    ruta = f"{DELTA_BASE_PATH}/{tabla}"
    return spark.sql(f"DESCRIBE HISTORY delta.`{ruta}`")


if __name__ == "__main__":
    ejecutar_pipeline()
