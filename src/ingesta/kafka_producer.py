"""
Capa de velocidad (Speed Layer) - Arquitectura Lambda
Productor Kafka para ingesta de eventos financieros en tiempo real.
Cada mensaje incluye hash SHA-256 para garantizar inmutabilidad.
"""

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TRANSACCIONES = os.getenv("KAFKA_TOPIC_TRANSACCIONES", "fintech.transacciones")
TOPIC_AUDITORIA = os.getenv("KAFKA_TOPIC_AUDITORIA", "fintech.auditoria")
TOPIC_ALERTAS = os.getenv("KAFKA_TOPIC_ALERTAS", "fintech.alertas")

KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",                  # Confirmación de todos los réplicas (durabilidad máxima)
    "retries": 5,
    "retry.backoff.ms": 300,
    "enable.idempotence": True,     # Exactamente una entrega (no duplicados)
    "compression.type": "snappy",
    "linger.ms": 5,
    "batch.size": 65536,
}


def calcular_hash_evento(payload: dict) -> str:
    """Genera SHA-256 del payload para garantizar integridad e inmutabilidad."""
    contenido = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(contenido.encode("utf-8")).hexdigest()


def construir_sobre(payload: dict, tipo_evento: str) -> dict:
    """
    Envuelve el payload en un sobre de auditoría con metadatos de trazabilidad.
    El hash cubre únicamente el payload para que sea verificable de forma independiente.
    """
    hash_payload = calcular_hash_evento(payload)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": tipo_evento,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "payload": payload,
        "integrity": {
            "algorithm": "SHA-256",
            "hash": hash_payload,
        },
    }


def delivery_callback(err, msg):
    if err:
        logger.error("Error al entregar mensaje [topic=%s]: %s", msg.topic(), err)
    else:
        logger.info(
            "Mensaje entregado [topic=%s, partition=%d, offset=%d]",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


class ProductorAuditoria:
    """
    Productor Kafka especializado para eventos de auditoría financiera.
    Garantiza exactamente una entrega (idempotencia) y trazabilidad completa.
    """

    def __init__(self):
        self._producer = Producer(KAFKA_CONFIG)
        self._admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        self._asegurar_topics()

    def _asegurar_topics(self):
        topics_requeridos = [
            NewTopic(TOPIC_TRANSACCIONES, num_partitions=6, replication_factor=3),
            NewTopic(TOPIC_AUDITORIA,     num_partitions=3, replication_factor=3),
            NewTopic(TOPIC_ALERTAS,       num_partitions=3, replication_factor=3),
        ]
        resultados = self._admin.create_topics(topics_requeridos)
        for topic, future in resultados.items():
            try:
                future.result()
                logger.info("Topic creado: %s", topic)
            except Exception as exc:
                # Ignorar si el topic ya existe
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    logger.warning("Topic %s: %s", topic, exc)

    def publicar_transaccion(self, transaccion: dict[str, Any]) -> str:
        """
        Publica una transacción financiera en Kafka.
        Retorna el event_id para trazabilidad.
        """
        sobre = construir_sobre(transaccion, "TRANSACCION_FINANCIERA")
        self._producer.produce(
            topic=TOPIC_TRANSACCIONES,
            key=transaccion.get("cuenta_id", "sin_cuenta").encode("utf-8"),
            value=json.dumps(sobre, ensure_ascii=False).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        self._producer.poll(0)
        logger.info(
            "Transacción publicada [event_id=%s, hash=%s]",
            sobre["event_id"],
            sobre["integrity"]["hash"][:16] + "...",
        )
        return sobre["event_id"]

    def publicar_evento_auditoria(self, evento: dict[str, Any]) -> str:
        """Publica un evento de auditoría (accesos, cambios de configuración, etc.)."""
        sobre = construir_sobre(evento, "EVENTO_AUDITORIA")
        self._producer.produce(
            topic=TOPIC_AUDITORIA,
            key=evento.get("usuario_id", "sistema").encode("utf-8"),
            value=json.dumps(sobre, ensure_ascii=False).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        self._producer.poll(0)
        return sobre["event_id"]

    def publicar_alerta(self, alerta: dict[str, Any]) -> str:
        """Publica una alerta regulatoria o de riesgo."""
        sobre = construir_sobre(alerta, "ALERTA_REGULATORIA")
        self._producer.produce(
            topic=TOPIC_ALERTAS,
            key=alerta.get("tipo_alerta", "general").encode("utf-8"),
            value=json.dumps(sobre, ensure_ascii=False).encode("utf-8"),
            on_delivery=delivery_callback,
        )
        self._producer.poll(0)
        return sobre["event_id"]

    def flush(self, timeout: float = 30.0):
        """Espera a que todos los mensajes pendientes sean entregados."""
        pendientes = self._producer.flush(timeout)
        if pendientes > 0:
            logger.warning("%d mensajes no confirmados tras flush.", pendientes)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.flush()


# ---------------------------------------------------------------------------
# Ejemplo de uso / smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    transaccion_ejemplo = {
        "cuenta_id": "CTA-001234",
        "monto": 1500000.00,
        "moneda": "CLP",
        "tipo": "TRANSFERENCIA",
        "cuenta_destino": "CTA-005678",
        "canal": "APP_MOVIL",
        "ip_origen": "192.168.1.10",
        "metadata": {"referencia": "PAG-2026-05-001"},
    }

    with ProductorAuditoria() as productor:
        event_id = productor.publicar_transaccion(transaccion_ejemplo)
        print(f"Transacción publicada con event_id: {event_id}")
