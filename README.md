# FinTech Auditoría — Sistema de Auditoría Regulatoria

Sistema de auditoría para fintech basado en **Arquitectura Lambda**, diseñado para garantizar **inmutabilidad de registros**, **transacciones ACID** y **reportes regulatorios** mensuales conformes a estándares CMF/SBIF.

---

## Arquitectura

```
                     ┌─────────────────────────────────────────┐
                     │           ARQUITECTURA LAMBDA            │
                     └─────────────────────────────────────────┘

  Fuentes de datos
  ┌─────────────┐
  │ Transacc.   │──►  Kafka Topics  ──►  Speed Layer  ──►  Vista en tiempo real
  │ Alertas     │     (inmutables)        (Kafka)
  │ Accesos     │          │
  └─────────────┘          ▼
                     Batch Layer
                     (Spark + Delta Lake)
                          │
                          ▼
                     Vista Maestra
                     (Delta Tables)
                          │
                          ▼
                     Serving Layer
                     (PostgreSQL + FastAPI)
                          │
                          ▼
                     Reportes CMF/SBIF
                     (SHA-256 sellado)
```

### Componentes

| Componente | Tecnología | Rol |
|---|---|---|
| **Ingesta** | Apache Kafka | Captura eventos financieros con hash SHA-256 |
| **Batch** | Apache Spark + Delta Lake | Transformaciones ACID, detección de anomalías |
| **Servicio** | FastAPI + asyncpg | API REST de solo lectura sobre PostgreSQL |
| **Reportes** | Python + psycopg2 | Generación de reportes CMF sellados |
| **Almacén** | PostgreSQL 16 | Vista maestra de servicio e historial de reportes |

---

## Garantías del sistema

### Inmutabilidad
- **Kafka**: topics append-only con retención de 7 días.
- **Delta Lake**: escritura append-only, historial completo via `DESCRIBE HISTORY`.
- **PostgreSQL**: triggers bloquean `UPDATE` y `DELETE` en tablas críticas.

### Integridad
- Cada evento incluye un hash **SHA-256** calculado sobre el payload en el momento de la ingesta.
- El pipeline Spark recomputa el hash y descarta registros alterados enviándolos a cuarentena.
- Cada reporte regulatorio es sellado con SHA-256 antes de persistirse.

### ACID
- Delta Lake garantiza atomicidad y consistencia en escrituras concurrentes.
- PostgreSQL usa transacciones explícitas para persistir reportes; ante error hace rollback completo.

---

## Estructura del proyecto

```
fintech_auditoria_ITY1101/
├── src/
│   ├── ingesta/
│   │   └── kafka_producer.py      # Productor Kafka con hash SHA-256
│   ├── batch/
│   │   └── spark_transform.py     # Pipeline Spark → Delta Lake
│   ├── servicio/
│   │   └── api_gateway.py         # API REST FastAPI
│   └── reportes/
│       └── reporte_regulatorio.py # Generador de reportes CMF
├── init/
│   └── schema.sql                 # Schema PostgreSQL con triggers de inmutabilidad
├── docker-compose.yml             # Orquestación completa
├── Dockerfile                     # Multi-stage (ingesta/batch/api/reportes)
├── requirements.txt
├── .env.example
└── README.md
```

---

## Inicio rápido

### Prerrequisitos
- Docker Desktop 24+
- Docker Compose v2

### Levantar el entorno

```bash
# 1. Configurar variables de entorno
cp .env.example .env
# Editar .env con credenciales seguras

# 2. Levantar todos los servicios
docker compose up -d

# 3. Verificar estado
docker compose ps
```

### Verificar la API

```bash
# Health check
curl http://localhost:8000/health

# Listar transacciones
curl "http://localhost:8000/transacciones?limite=10"

# Estadísticas de una cuenta
curl http://localhost:8000/cuentas/CTA-001234/estadisticas
```

### Generar reporte regulatorio mensual

```bash
# Generar reporte para mayo 2026
docker compose run --rm reportes python -m src.reportes.reporte_regulatorio 2026 5
```

### Verificar integridad de un reporte en PostgreSQL

```sql
SELECT * FROM auditoria.verificar_reporte('<reporte_id>');
```

---

## Flujo de datos

```
1. kafka_producer.py
   └─ Publica evento con hash SHA-256 → topic fintech.transacciones

2. spark_transform.py (cada 60 segundos)
   ├─ Lee micro-batch desde Kafka
   ├─ Recomputa y valida hash SHA-256
   ├─ Registros inválidos → Delta tabla cuarentena
   ├─ Enriquece (monto_clp, flags CMF)
   └─ Escribe append-only en Delta Lake (ACID)

3. api_gateway.py
   └─ Expone endpoints GET sobre PostgreSQL (solo lectura)

4. reporte_regulatorio.py (ejecución mensual)
   ├─ Agrega transacciones del período desde PostgreSQL
   ├─ Identifica alertas regulatorias (≥10M CLP, retiros)
   ├─ Sella el reporte con SHA-256
   └─ Persiste en PostgreSQL (inmutable) + exporta JSON/CSV
```

---

## Endpoints de la API

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/health` | Estado del servicio |
| `GET` | `/transacciones` | Listar transacciones (filtros: cuenta, fechas) |
| `GET` | `/transacciones/{event_id}` | Detalle de una transacción |
| `GET` | `/cuentas/{cuenta_id}/estadisticas` | Estadísticas por cuenta |
| `GET` | `/reportes` | Listar reportes regulatorios |
| `GET` | `/reportes/{reporte_id}` | Detalle de un reporte |
| `GET` | `/alertas` | Listar alertas regulatorias |

Documentación interactiva disponible en `http://localhost:8000/docs`.

---

## Configuración de variables de entorno

Ver `.env.example` para la lista completa. Variables críticas:

| Variable | Descripción |
|---|---|
| `DATABASE_URL` | URL de conexión PostgreSQL |
| `KAFKA_BOOTSTRAP_SERVERS` | Brokers Kafka |
| `DELTA_LAKE_PATH` | Ruta base para tablas Delta |
| `API_SECRET_KEY` | Clave secreta de la API (cambiar en producción) |
| `REPORTES_OUTPUT_PATH` | Directorio de salida para JSON/CSV de reportes |

---

## Cumplimiento regulatorio

El sistema está diseñado para cumplir con:
- **CMF Chile** — Reportes de operaciones inusuales (≥ 10M CLP).
- **SBIF** — Trazabilidad completa de transacciones financieras.
- **Ley 19.913** — Prevención de lavado de activos (alertas automáticas).

Cada reporte contiene: período, totales, desglose por tipo de operación, listado de operaciones reportables y hash SHA-256 para verificación de integridad.
