-- FinTech Auditoría — Schema PostgreSQL
-- Diseñado para inmutabilidad: los registros NO se actualizan ni eliminan.
-- Las tablas críticas tienen triggers que bloquean UPDATE y DELETE.

-- Extensiones requeridas
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Schema dedicado para aislamiento
CREATE SCHEMA IF NOT EXISTS auditoria;

SET search_path TO auditoria, public;

-- 1° Transacciones.
CREATE TABLE IF NOT EXISTS auditoria.transacciones (
    id                          BIGSERIAL       PRIMARY KEY,
    event_id                    UUID            NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
    kafka_offset                BIGINT,
    kafka_partition             SMALLINT,

    -- Datos de la transacción
    cuenta_id                   VARCHAR(64)     NOT NULL,
    cuenta_destino              VARCHAR(64),
    monto                       NUMERIC(18, 2)  NOT NULL CHECK (monto > 0),
    monto_clp                   NUMERIC(18, 2)  NOT NULL CHECK (monto_clp > 0),
    moneda                      CHAR(3)         NOT NULL DEFAULT 'CLP',
    tipo                        VARCHAR(64)     NOT NULL,
    canal                       VARCHAR(64),
    ip_origen                   INET,

    -- Trazabilidad temporal
    timestamp_utc               TIMESTAMPTZ     NOT NULL,
    fecha_proceso               DATE            NOT NULL DEFAULT CURRENT_DATE,
    anio                        SMALLINT        NOT NULL DEFAULT 0,
    mes                         SMALLINT        NOT NULL DEFAULT 0,

    -- Integridad e inmutabilidad
    hash_integridad             CHAR(64)        NOT NULL,         -- SHA-256 del payload original
    hash_valido                 BOOLEAN         NOT NULL DEFAULT FALSE,
    schema_version              VARCHAR(16)     NOT NULL DEFAULT '1.0',

    -- Flags regulatorios
    es_alta_valor               BOOLEAN         NOT NULL DEFAULT FALSE,
    requiere_reporte_cmf        BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Metadata de inserción (solo escritura)
    insertado_en                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    insertado_por               VARCHAR(128)    NOT NULL DEFAULT CURRENT_USER,

    CONSTRAINT chk_moneda       CHECK (moneda ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_hash_formato CHECK (hash_integridad ~ '^[a-f0-9]{64}$')
);

-- Índices para consultas regulatorias y de auditoría.
CREATE INDEX IF NOT EXISTS idx_tx_cuenta_id  ON auditoria.transacciones (cuenta_id);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp  ON auditoria.transacciones (timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_tx_periodo    ON auditoria.transacciones (anio, mes);
CREATE INDEX IF NOT EXISTS idx_tx_cmf        ON auditoria.transacciones (requiere_reporte_cmf) WHERE requiere_reporte_cmf = TRUE;
CREATE INDEX IF NOT EXISTS idx_tx_hash       ON auditoria.transacciones (hash_integridad);
CREATE INDEX IF NOT EXISTS idx_tx_tipo       ON auditoria.transacciones (tipo);

-- TABLA: reportes_regulatorios
-- Cada reporte mensual se sella con SHA-256 y es inmutable.
CREATE TABLE IF NOT EXISTS auditoria.reportes_regulatorios (
    id                              BIGSERIAL       PRIMARY KEY,
    reporte_id                      UUID            NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
    periodo                         CHAR(7)         NOT NULL UNIQUE,  -- "YYYY-MM"
    anio                            SMALLINT        NOT NULL,
    mes                             SMALLINT        NOT NULL CHECK (mes BETWEEN 1 AND 12),

    -- Estadísticas del período
    total_transacciones             INTEGER         NOT NULL DEFAULT 0,
    monto_total_clp                 NUMERIC(20, 2)  NOT NULL DEFAULT 0,
    monto_promedio_clp              NUMERIC(18, 2)  NOT NULL DEFAULT 0,
    transacciones_alta_valor        INTEGER         NOT NULL DEFAULT 0,
    transacciones_reportables_cmf   INTEGER         NOT NULL DEFAULT 0,

    -- Contenido completo en JSON (inmutable)
    resumen_json                    JSONB           NOT NULL DEFAULT '[]',
    alertas_json                    JSONB           NOT NULL DEFAULT '[]',

    -- Sellado criptográfico
    hash_reporte                    CHAR(64)        NOT NULL,
    algoritmo_hash                  VARCHAR(16)     NOT NULL DEFAULT 'SHA-256',

    -- Estado del reporte
    estado                          VARCHAR(32)     NOT NULL DEFAULT 'GENERADO'
                                        CHECK (estado IN ('GENERADO', 'ENVIADO_CMF', 'ACEPTADO_CMF', 'OBSERVADO_CMF')),
    fecha_generacion_utc            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    generado_en                     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    generado_por                    VARCHAR(128)    NOT NULL DEFAULT CURRENT_USER,
    enviado_cmf_en                  TIMESTAMPTZ,
    referencia_cmf                  VARCHAR(128),

    CONSTRAINT chk_reporte_periodo  CHECK (periodo ~ '^\d{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT chk_reporte_hash     CHECK (hash_reporte ~ '^[a-f0-9]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_rep_periodo ON auditoria.reportes_regulatorios (periodo DESC);
CREATE INDEX IF NOT EXISTS idx_rep_estado  ON auditoria.reportes_regulatorios (estado);


-- TABLA: alertas_regulatorias
CREATE TABLE IF NOT EXISTS auditoria.alertas_regulatorias (
    id              BIGSERIAL       PRIMARY KEY,
    alerta_id       UUID            NOT NULL UNIQUE DEFAULT uuid_generate_v4(),
    tipo_alerta     VARCHAR(64)     NOT NULL,
    descripcion     TEXT            NOT NULL,
    event_id        UUID            REFERENCES auditoria.transacciones(event_id),
    cuenta_id       VARCHAR(64),
    monto_clp       NUMERIC(18, 2),
    severidad       VARCHAR(16)     NOT NULL DEFAULT 'MEDIA'
                        CHECK (severidad IN ('BAJA', 'MEDIA', 'ALTA', 'CRITICA')),
    resuelta        BOOLEAN         NOT NULL DEFAULT FALSE,
    resuelta_en     TIMESTAMPTZ,
    resuelta_por    VARCHAR(128),
    creada_en       TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerta_tipo     ON auditoria.alertas_regulatorias (tipo_alerta);
CREATE INDEX IF NOT EXISTS idx_alerta_resuelta ON auditoria.alertas_regulatorias (resuelta) WHERE resuelta = FALSE;
CREATE INDEX IF NOT EXISTS idx_alerta_cuenta   ON auditoria.alertas_regulatorias (cuenta_id);

-- TABLA: log_sistema
-- Registro de accesos y operaciones del sistema.
CREATE TABLE IF NOT EXISTS auditoria.log_sistema (
    id              BIGSERIAL       PRIMARY KEY,
    log_id          UUID            NOT NULL DEFAULT uuid_generate_v4(),
    accion          VARCHAR(64)     NOT NULL,
    entidad         VARCHAR(64),
    entidad_id      VARCHAR(128),
    usuario_id      VARCHAR(128),
    ip_origen       INET,
    detalle         JSONB,
    resultado       VARCHAR(16)     NOT NULL DEFAULT 'OK'
                        CHECK (resultado IN ('OK', 'ERROR', 'RECHAZADO')),
    mensaje_error   TEXT,
    timestamp_utc   TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_log_usuario   ON auditoria.log_sistema (usuario_id);
CREATE INDEX IF NOT EXISTS idx_log_timestamp ON auditoria.log_sistema (timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_log_accion    ON auditoria.log_sistema (accion);


-- TRIGGER: poblar anio/mes automáticamente en cada INSERT
CREATE OR REPLACE FUNCTION auditoria.fn_set_periodo()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.anio := EXTRACT(YEAR  FROM NEW.timestamp_utc AT TIME ZONE 'UTC')::SMALLINT;
    NEW.mes  := EXTRACT(MONTH FROM NEW.timestamp_utc AT TIME ZONE 'UTC')::SMALLINT;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_tx_set_periodo
    BEFORE INSERT ON auditoria.transacciones
    FOR EACH ROW EXECUTE FUNCTION auditoria.fn_set_periodo();

-- TRIGGERS: inmutabilidad — bloquear UPDATE y DELETE en tablas críticas
CREATE OR REPLACE FUNCTION auditoria.fn_bloquear_modificacion()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION
        'Operación % denegada en tabla "%": los registros de auditoría son inmutables.',
        TG_OP, TG_TABLE_NAME;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER trg_tx_no_update
    BEFORE UPDATE ON auditoria.transacciones
    FOR EACH ROW EXECUTE FUNCTION auditoria.fn_bloquear_modificacion();

CREATE OR REPLACE TRIGGER trg_tx_no_delete
    BEFORE DELETE ON auditoria.transacciones
    FOR EACH ROW EXECUTE FUNCTION auditoria.fn_bloquear_modificacion();

CREATE OR REPLACE TRIGGER trg_rep_no_update
    BEFORE UPDATE ON auditoria.reportes_regulatorios
    FOR EACH ROW EXECUTE FUNCTION auditoria.fn_bloquear_modificacion();

CREATE OR REPLACE TRIGGER trg_rep_no_delete
    BEFORE DELETE ON auditoria.reportes_regulatorios
    FOR EACH ROW EXECUTE FUNCTION auditoria.fn_bloquear_modificacion();


-- FUNCTION: Verifica la integridad SHA-256 de un reporte
CREATE OR REPLACE FUNCTION auditoria.verificar_reporte(p_reporte_id UUID)
RETURNS TABLE (
    reporte_id  UUID,
    periodo     CHAR(7),
    hash_actual CHAR(64),
    hash_valido BOOLEAN
) LANGUAGE plpgsql AS $$
DECLARE
    v_fila auditoria.reportes_regulatorios%ROWTYPE;
    v_hash CHAR(64);
BEGIN
    SELECT * INTO v_fila
    FROM auditoria.reportes_regulatorios
    WHERE reportes_regulatorios.reporte_id = p_reporte_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Reporte % no encontrado.', p_reporte_id;
    END IF;

    v_hash := encode(
        digest(
            concat_ws('|',
                v_fila.reporte_id::text,
                v_fila.periodo,
                v_fila.total_transacciones::text,
                v_fila.monto_total_clp::text,
                v_fila.resumen_json::text,
                v_fila.alertas_json::text
            ),
            'sha256'
        ),
        'hex'
    );

    RETURN QUERY SELECT
        v_fila.reporte_id,
        v_fila.periodo,
        v_fila.hash_reporte,
        (v_fila.hash_reporte = v_hash);
END;
$$;

-- Roles y permisos mínimos (principio de mínimo privilegio)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'auditoria_writer') THEN
        CREATE ROLE auditoria_writer;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'auditoria_reader') THEN
        CREATE ROLE auditoria_reader;
    END IF;
END $$;

GRANT USAGE                   ON SCHEMA auditoria                    TO auditoria_writer, auditoria_reader;
GRANT INSERT                  ON auditoria.transacciones             TO auditoria_writer;
GRANT INSERT                  ON auditoria.reportes_regulatorios     TO auditoria_writer;
GRANT INSERT                  ON auditoria.alertas_regulatorias      TO auditoria_writer;
GRANT INSERT                  ON auditoria.log_sistema               TO auditoria_writer;
GRANT SELECT                  ON ALL TABLES IN SCHEMA auditoria       TO auditoria_reader;
GRANT USAGE, SELECT           ON ALL SEQUENCES IN SCHEMA auditoria    TO auditoria_writer;


-- Datos de ejemplo para pruebas
INSERT INTO auditoria.transacciones (
    event_id, cuenta_id, monto, monto_clp, moneda, tipo, canal,
    timestamp_utc, hash_integridad, hash_valido, requiere_reporte_cmf, es_alta_valor
) VALUES
(
    uuid_generate_v4(), 'CTA-001234', 1500000.00, 1500000.00, 'CLP',
    'TRANSFERENCIA', 'APP_MOVIL',
    NOW() - INTERVAL '1 day',
    encode(sha256('demo_hash_transaccion_1'::bytea), 'hex'),
    TRUE, FALSE, FALSE
),
(
    uuid_generate_v4(), 'CTA-005678', 12000000.00, 12000000.00, 'CLP',
    'TRANSFERENCIA', 'WEB',
    NOW() - INTERVAL '2 days',
    encode(sha256('demo_hash_transaccion_2'::bytea), 'hex'),
    TRUE, TRUE, TRUE
)
ON CONFLICT DO NOTHING;
