"""
Generador de Reportes Regulatorios Mensuales - Arquitectura Lambda
Produce reportes CMF/SBIF con hash SHA-256 e inmutabilidad garantizada.
Los reportes se persisten en PostgreSQL y en disco (JSON + CSV).
"""

import csv
import hashlib
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL   = os.getenv("DATABASE_URL", "postgresql://auditoria_user:auditoria_pass@localhost:5432/fintech_auditoria")
REPORTES_PATH  = Path(os.getenv("REPORTES_OUTPUT_PATH", "/data/reportes"))


# ---------------------------------------------------------------------------
# Estructuras de datos
# ---------------------------------------------------------------------------
@dataclass
class ResumenTransaccion:
    tipo: str
    cantidad: int
    monto_total_clp: float
    monto_promedio_clp: float
    porcentaje_del_total: float


@dataclass
class AlertaCMF:
    cuenta_id: str
    event_id: str
    monto_clp: float
    tipo_transaccion: str
    timestamp_utc: str
    motivo: str


@dataclass
class ReporteRegulatorio:
    reporte_id: str
    periodo: str                          # "YYYY-MM"
    anio: int
    mes: int
    fecha_generacion_utc: str
    total_transacciones: int
    monto_total_clp: float
    monto_promedio_clp: float
    transacciones_alta_valor: int         # ≥ 10M CLP
    transacciones_reportables_cmf: int
    resumen_por_tipo: list[ResumenTransaccion]
    alertas: list[AlertaCMF]
    hash_reporte: str = field(default="")
    generado_por: str = "sistema_auditoria_v1"

    def calcular_hash(self) -> str:
        """
        Computa SHA-256 sobre el contenido del reporte (excluyendo el propio hash).
        Este valor permite detectar cualquier modificación posterior del reporte.
        """
        contenido = {
            k: v for k, v in asdict(self).items() if k != "hash_reporte"
        }
        serializado = json.dumps(contenido, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(serializado.encode("utf-8")).hexdigest()

    def sellar(self):
        """Calcula y asigna el hash SHA-256 al reporte (operación de sellado)."""
        self.hash_reporte = self.calcular_hash()

    def verificar_integridad(self) -> bool:
        """Recomputa el hash y compara con el almacenado para detectar alteraciones."""
        return self.hash_reporte == self.calcular_hash()


# ---------------------------------------------------------------------------
# Generador principal
# ---------------------------------------------------------------------------
class GeneradorReporteRegulatorio:
    """
    Genera reportes regulatorios mensuales desde PostgreSQL.
    Cada reporte es sellado con SHA-256 e inmutable una vez persistido.
    """

    def __init__(self):
        self._conn = psycopg2.connect(DATABASE_URL)
        self._conn.autocommit = False
        REPORTES_PATH.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Consultas a PostgreSQL
    # ------------------------------------------------------------------
    def _totales_periodo(self, anio: int, mes: int) -> dict:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)                                          AS total_transacciones,
                    COALESCE(SUM(monto_clp), 0)                       AS monto_total_clp,
                    COALESCE(AVG(monto_clp), 0)                       AS monto_promedio_clp,
                    COUNT(*) FILTER (WHERE monto_clp >= 10000000)     AS transacciones_alta_valor,
                    COUNT(*) FILTER (WHERE requiere_reporte_cmf)      AS transacciones_reportables_cmf
                FROM auditoria.transacciones
                WHERE EXTRACT(YEAR  FROM timestamp_utc) = %s
                  AND EXTRACT(MONTH FROM timestamp_utc) = %s
                  AND hash_valido = TRUE
                """,
                (anio, mes),
            )
            return dict(cur.fetchone())

    def _resumen_por_tipo(self, anio: int, mes: int, monto_total: float) -> list[ResumenTransaccion]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    tipo,
                    COUNT(*)            AS cantidad,
                    SUM(monto_clp)      AS monto_total_clp,
                    AVG(monto_clp)      AS monto_promedio_clp
                FROM auditoria.transacciones
                WHERE EXTRACT(YEAR  FROM timestamp_utc) = %s
                  AND EXTRACT(MONTH FROM timestamp_utc) = %s
                  AND hash_valido = TRUE
                GROUP BY tipo
                ORDER BY monto_total_clp DESC
                """,
                (anio, mes),
            )
            filas = cur.fetchall()

        total = monto_total or 1.0
        return [
            ResumenTransaccion(
                tipo=f["tipo"],
                cantidad=f["cantidad"],
                monto_total_clp=float(f["monto_total_clp"]),
                monto_promedio_clp=float(f["monto_promedio_clp"]),
                porcentaje_del_total=round(float(f["monto_total_clp"]) / total * 100, 2),
            )
            for f in filas
        ]

    def _alertas_cmf(self, anio: int, mes: int) -> list[AlertaCMF]:
        with self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cuenta_id, event_id::text AS event_id, monto_clp, tipo AS tipo_transaccion,
                       timestamp_utc::text AS timestamp_utc,
                       CASE
                           WHEN monto_clp >= 10000000 THEN 'Supera umbral CMF (≥10M CLP)'
                           WHEN tipo = 'RETIRO_EFECTIVO' THEN 'Retiro en efectivo reportable'
                           ELSE 'Operación inusual'
                       END AS motivo
                FROM auditoria.transacciones
                WHERE EXTRACT(YEAR  FROM timestamp_utc) = %s
                  AND EXTRACT(MONTH FROM timestamp_utc) = %s
                  AND requiere_reporte_cmf = TRUE
                  AND hash_valido = TRUE
                ORDER BY monto_clp DESC
                LIMIT 500
                """,
                (anio, mes),
            )
            return [AlertaCMF(**dict(f)) for f in cur.fetchall()]

    # ------------------------------------------------------------------
    # Construcción y sellado del reporte
    # ------------------------------------------------------------------
    def generar(self, anio: int, mes: int) -> ReporteRegulatorio:
        logger.info("Generando reporte regulatorio %04d-%02d ...", anio, mes)

        totales  = self._totales_periodo(anio, mes)
        resumen  = self._resumen_por_tipo(anio, mes, float(totales["monto_total_clp"]))
        alertas  = self._alertas_cmf(anio, mes)

        reporte = ReporteRegulatorio(
            reporte_id=str(uuid.uuid4()),
            periodo=f"{anio:04d}-{mes:02d}",
            anio=anio,
            mes=mes,
            fecha_generacion_utc=datetime.now(timezone.utc).isoformat(),
            total_transacciones=int(totales["total_transacciones"]),
            monto_total_clp=float(totales["monto_total_clp"]),
            monto_promedio_clp=float(totales["monto_promedio_clp"]),
            transacciones_alta_valor=int(totales["transacciones_alta_valor"]),
            transacciones_reportables_cmf=int(totales["transacciones_reportables_cmf"]),
            resumen_por_tipo=resumen,
            alertas=alertas,
        )

        reporte.sellar()
        logger.info(
            "Reporte %s sellado con SHA-256: %s",
            reporte.reporte_id,
            reporte.hash_reporte[:16] + "...",
        )
        return reporte

    # ------------------------------------------------------------------
    # Persistencia (ACID — transacción única)
    # ------------------------------------------------------------------
    def persistir(self, reporte: ReporteRegulatorio):
        """
        Persiste el reporte en PostgreSQL dentro de una única transacción ACID.
        Una vez insertado, el registro es de solo lectura (inmutable por diseño de schema).
        """
        try:
            with self._conn.cursor() as cur:
                # Validar que no exista ya un reporte para el mismo período
                cur.execute(
                    "SELECT 1 FROM auditoria.reportes_regulatorios WHERE periodo = %s",
                    (reporte.periodo,),
                )
                if cur.fetchone():
                    raise ValueError(
                        f"Ya existe un reporte para el período {reporte.periodo}. "
                        "Los reportes son inmutables."
                    )

                cur.execute(
                    """
                    INSERT INTO auditoria.reportes_regulatorios
                        (reporte_id, periodo, anio, mes, fecha_generacion_utc,
                         total_transacciones, monto_total_clp, monto_promedio_clp,
                         transacciones_alta_valor, transacciones_reportables_cmf,
                         resumen_json, alertas_json, hash_reporte, estado)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'GENERADO')
                    """,
                    (
                        reporte.reporte_id,
                        reporte.periodo,
                        reporte.anio,
                        reporte.mes,
                        reporte.fecha_generacion_utc,
                        reporte.total_transacciones,
                        reporte.monto_total_clp,
                        reporte.monto_promedio_clp,
                        reporte.transacciones_alta_valor,
                        reporte.transacciones_reportables_cmf,
                        json.dumps([asdict(r) for r in reporte.resumen_por_tipo], ensure_ascii=False),
                        json.dumps([asdict(a) for a in reporte.alertas], ensure_ascii=False),
                        reporte.hash_reporte,
                    ),
                )
            self._conn.commit()
            logger.info("Reporte %s persistido en PostgreSQL.", reporte.reporte_id)
        except Exception:
            self._conn.rollback()
            raise

    # ------------------------------------------------------------------
    # Exportación a archivos
    # ------------------------------------------------------------------
    def exportar_json(self, reporte: ReporteRegulatorio) -> Path:
        ruta = REPORTES_PATH / f"reporte_{reporte.periodo}_{reporte.reporte_id[:8]}.json"
        with open(ruta, "w", encoding="utf-8") as f:
            json.dump(asdict(reporte), f, indent=2, ensure_ascii=False)
        logger.info("Reporte exportado a JSON: %s", ruta)
        return ruta

    def exportar_csv_alertas(self, reporte: ReporteRegulatorio) -> Path:
        ruta = REPORTES_PATH / f"alertas_{reporte.periodo}_{reporte.reporte_id[:8]}.csv"
        with open(ruta, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(reporte.alertas[0]).keys()) if reporte.alertas else [])
            writer.writeheader()
            for alerta in reporte.alertas:
                writer.writerow(asdict(alerta))
        logger.info("Alertas CMF exportadas a CSV: %s", ruta)
        return ruta

    def __del__(self):
        if self._conn and not self._conn.closed:
            self._conn.close()


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------
def generar_reporte_periodo(anio: int, mes: int) -> ReporteRegulatorio:
    generador = GeneradorReporteRegulatorio()
    reporte = generador.generar(anio, mes)
    generador.persistir(reporte)
    generador.exportar_json(reporte)
    if reporte.alertas:
        generador.exportar_csv_alertas(reporte)

    assert reporte.verificar_integridad(), "ERROR CRÍTICO: integridad del reporte comprometida."
    logger.info(
        "Reporte %s finalizado. Total transacciones: %d | Monto: $%,.0f CLP | Hash: %s",
        reporte.periodo,
        reporte.total_transacciones,
        reporte.monto_total_clp,
        reporte.hash_reporte,
    )
    return reporte


if __name__ == "__main__":
    import sys
    hoy = date.today()
    anio = int(sys.argv[1]) if len(sys.argv) > 1 else hoy.year
    mes  = int(sys.argv[2]) if len(sys.argv) > 2 else hoy.month
    generar_reporte_periodo(anio, mes)
