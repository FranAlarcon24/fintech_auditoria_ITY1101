
"""
Capa de servicio (Serving Layer) - Arquitectura Lambda
API Gateway REST con FastAPI para consultas sobre la vista maestra y eventos de auditoría.
Expone endpoints de solo lectura sobre PostgreSQL (resultados batch) y Delta Lake.
"""

import hashlib
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Annotated, Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://auditoria_user:auditoria_pass@localhost:5432/fintech_auditoria",
)
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "cambia_esto_en_produccion")

# 1. Pool de conexiones PostgreSQL
_pool: Optional[asyncpg.Pool] = None


async def obtener_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
    return _pool


async def get_db() -> asyncpg.Connection:
    pool = await obtener_pool()
    async with pool.acquire() as conn:
        yield conn


DBConn = Annotated[asyncpg.Connection, Depends(get_db)]

# 2. Ciclo de vida de la aplicación
@asynccontextmanager
async def lifespan(app: FastAPI):
    await obtener_pool()
    logger.info("API Gateway iniciado — pool PostgreSQL listo.")
    yield
    if _pool:
        await _pool.close()
    logger.info("API Gateway detenido.")


app = FastAPI(
    title="FinTech Auditoría — API Gateway",
    description="Capa de servicio para consultas de auditoría regulatoria.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# 3. Modelos de respuesta
class TransaccionResumen(BaseModel):
    event_id: str
    cuenta_id: str
    monto: float
    moneda: str
    tipo: str
    timestamp_utc: datetime
    hash_integridad: str
    hash_valido: bool


class ReporteRegulatorioCabecera(BaseModel):
    reporte_id: str
    periodo: str
    total_transacciones: int
    monto_total_clp: float
    hash_reporte: str
    generado_en: datetime
    estado: str


class EstadisticasCuenta(BaseModel):
    cuenta_id: str
    total_transacciones: int
    monto_total: float
    monto_promedio: float
    primera_transaccion: datetime
    ultima_transaccion: datetime


class HealthResponse(BaseModel):
    status: str
    db_ok: bool
    timestamp: datetime = Field(default_factory=datetime.utcnow)

# 4. Middleware de auditoría de accesos
@app.middleware("http")
async def registrar_acceso(request: Request, call_next):
    inicio = datetime.utcnow()
    response = await call_next(request)
    duracion_ms = (datetime.utcnow() - inicio).total_seconds() * 1000
    logger.info(
        "ACCESS [%s] %s %s → %d (%.1fms)",
        request.client.host if request.client else "unknown",
        request.method,
        request.url.path,
        response.status_code,
        duracion_ms,
    )
    return response

# 5.Endpoints
@app.get("/health", response_model=HealthResponse, tags=["Sistema"])
async def health_check(db: DBConn):
    try:
        await db.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return HealthResponse(status="ok" if db_ok else "degradado", db_ok=db_ok)


@app.get("/transacciones", response_model=list[TransaccionResumen], tags=["Transacciones"])
async def listar_transacciones(
    db: DBConn,
    cuenta_id: Optional[str] = Query(None, description="Filtrar por ID de cuenta"),
    desde: Optional[date] = Query(None, description="Fecha de inicio (YYYY-MM-DD)"),
    hasta: Optional[date] = Query(None, description="Fecha de fin (YYYY-MM-DD)"),
    limite: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    condiciones = ["1=1"]
    params: list = []
    idx = 1

    if cuenta_id:
        condiciones.append(f"cuenta_id = ${idx}")
        params.append(cuenta_id)
        idx += 1
    if desde:
        condiciones.append(f"timestamp_utc >= ${idx}")
        params.append(datetime.combine(desde, datetime.min.time()))
        idx += 1
    if hasta:
        condiciones.append(f"timestamp_utc < ${idx}::date + interval '1 day'")
        params.append(hasta)
        idx += 1

    where = " AND ".join(condiciones)
    query = f"""
        SELECT event_id, cuenta_id, monto, moneda, tipo,
               timestamp_utc, hash_integridad, hash_valido
        FROM auditoria.transacciones
        WHERE {where}
        ORDER BY timestamp_utc DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    params += [limite, offset]

    filas = await db.fetch(query, *params)
    return [dict(f) for f in filas]


@app.get("/transacciones/{event_id}", response_model=TransaccionResumen, tags=["Transacciones"])
async def obtener_transaccion(event_id: str, db: DBConn):
    fila = await db.fetchrow(
        """SELECT event_id, cuenta_id, monto, moneda, tipo,
                  timestamp_utc, hash_integridad, hash_valido
           FROM auditoria.transacciones
           WHERE event_id = $1""",
        event_id,
    )
    if not fila:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transacción no encontrada.")
    return dict(fila)


@app.get("/cuentas/{cuenta_id}/estadisticas", response_model=EstadisticasCuenta, tags=["Cuentas"])
async def estadisticas_cuenta(cuenta_id: str, db: DBConn):
    fila = await db.fetchrow(
        """SELECT
               cuenta_id,
               COUNT(*)                        AS total_transacciones,
               SUM(monto)                      AS monto_total,
               AVG(monto)                      AS monto_promedio,
               MIN(timestamp_utc)              AS primera_transaccion,
               MAX(timestamp_utc)              AS ultima_transaccion
           FROM auditoria.transacciones
           WHERE cuenta_id = $1
           GROUP BY cuenta_id""",
        cuenta_id,
    )
    if not fila or fila["total_transacciones"] == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cuenta no encontrada.")
    return dict(fila)


@app.get("/reportes", response_model=list[ReporteRegulatorioCabecera], tags=["Reportes"])
async def listar_reportes(
    db: DBConn,
    anio: Optional[int] = Query(None, ge=2000, le=2099),
    mes: Optional[int] = Query(None, ge=1, le=12),
):
    condiciones = ["1=1"]
    params: list = []
    idx = 1

    if anio:
        condiciones.append(f"EXTRACT(YEAR  FROM generado_en) = ${idx}")
        params.append(anio)
        idx += 1
    if mes:
        condiciones.append(f"EXTRACT(MONTH FROM generado_en) = ${idx}")
        params.append(mes)
        idx += 1

    where = " AND ".join(condiciones)
    filas = await db.fetch(
        f"""SELECT reporte_id, periodo, total_transacciones,
                   monto_total_clp, hash_reporte, generado_en, estado
            FROM auditoria.reportes_regulatorios
            WHERE {where}
            ORDER BY generado_en DESC""",
        *params,
    )
    return [dict(f) for f in filas]


@app.get("/reportes/{reporte_id}", tags=["Reportes"])
async def obtener_reporte(reporte_id: str, db: DBConn):
    fila = await db.fetchrow(
        """SELECT * FROM auditoria.reportes_regulatorios WHERE reporte_id = $1""",
        reporte_id,
    )
    if not fila:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reporte no encontrado.")
    return dict(fila)


@app.get("/alertas", tags=["Alertas"])
async def listar_alertas(
    db: DBConn,
    resuelta: Optional[bool] = Query(None),
    limite: int = Query(50, ge=1, le=500),
):
    condicion = "" if resuelta is None else f"WHERE resuelta = {str(resuelta).upper()}"
    filas = await db.fetch(
        f"""SELECT alerta_id, tipo_alerta, descripcion, event_id,
                   creada_en, resuelta, resuelta_en
            FROM auditoria.alertas_regulatorias
            {condicion}
            ORDER BY creada_en DESC
            LIMIT $1""",
        limite,
    )
    return [dict(f) for f in filas]

# 6. Manejadores de error globales
@app.exception_handler(Exception)
async def manejador_error_generico(request: Request, exc: Exception):
    logger.exception("Error no controlado en %s: %s", request.url.path, exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Error interno del servidor."},
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api_gateway:app", host="0.0.0.0", port=8000, reload=False, workers=2)
