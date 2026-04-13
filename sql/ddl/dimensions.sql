-- ============================================================
-- dimensions.sql — DIMENSION TABLES  (Gold Layer)
--
-- Schema: {schema}  (resolved at runtime as {env}_ems)
-- Executed automatically by init_db() in etl/load.py.
-- Safe to run multiple times — every block is idempotent.
--
-- SCD strategy summary
--   dim_calendar           Type 0 — static pre-populated spine
--   dim_geography          Type 1 — overwrite on correction
--   dim_dispatch_complaint Type 1 — overwrite on correction
--   dim_ems_providers      Type 2 — track certification changes
-- ============================================================


-- ------------------------------------------------------------
-- 1. dim_calendar  (SCD Type 0)
--    Static date spine 2020-01-01 → 2030-12-31 (~4,018 rows).
--    Auto-populated on first dim load (idempotent).
--    Join to fact via: incident_dt = full_date  (no surrogate key).
-- ------------------------------------------------------------

-- name: create_dim_calendar
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'dim_calendar'
)
BEGIN
    CREATE TABLE {schema}.dim_calendar (
        date_key             INT          NOT NULL CONSTRAINT PK_dim_calendar PRIMARY KEY,  -- YYYYMMDD
        full_date            DATE         NOT NULL,
        year                 SMALLINT     NOT NULL,
        quarter              TINYINT      NOT NULL,   -- 1-4
        month                TINYINT      NOT NULL,   -- 1-12
        month_name           NVARCHAR(12) NOT NULL,   -- January … December
        month_short          NCHAR(3)     NOT NULL,   -- Jan … Dec
        day                  TINYINT      NOT NULL,   -- 1-31
        day_of_week          TINYINT      NOT NULL,   -- 1=Sunday … 7=Saturday
        day_name             NVARCHAR(12) NOT NULL,   -- Sunday … Saturday
        day_short            NCHAR(3)     NOT NULL,   -- Sun … Sat
        week_of_year         TINYINT      NOT NULL,   -- ISO week 1-53
        is_weekend           BIT          NOT NULL,
        is_weekday           BIT          NOT NULL,
        is_leap_year         BIT          NOT NULL,
        first_day_of_month   DATE         NOT NULL,
        last_day_of_month    DATE         NOT NULL,
        first_day_of_quarter DATE         NOT NULL,
        last_day_of_quarter  DATE         NOT NULL,
        first_day_of_year    DATE         NOT NULL,
        last_day_of_year     DATE         NOT NULL
    );

    CREATE UNIQUE NONCLUSTERED INDEX uix_calendar_date ON {schema}.dim_calendar (full_date);
END


-- ------------------------------------------------------------
-- 2. dim_geography  (SCD Type 1)
--    County-level geography. 93 distinct values in the dataset.
--    Rows overwritten in place on correction — no history needed.
-- ------------------------------------------------------------

-- name: create_dim_geography
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'dim_geography'
)
BEGIN
    CREATE TABLE {schema}.dim_geography (
        geography_key   INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_geography PRIMARY KEY,
        incident_county NVARCHAR(100)     NOT NULL   -- e.g. "BEXAR", "HARRIS", "DALLAS"
    );

    CREATE UNIQUE NONCLUSTERED INDEX uix_geography_county ON {schema}.dim_geography (incident_county);
END


-- ------------------------------------------------------------
-- 3. dim_dispatch_complaint  (SCD Type 1)
--    NEMSIS dispatch complaint codes. 118 distinct values.
--    Rows overwritten on NEMSIS version update — no history needed.
-- ------------------------------------------------------------

-- name: create_dim_dispatch_complaint
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'dim_dispatch_complaint'
)
BEGIN
    CREATE TABLE {schema}.dim_dispatch_complaint (
        complaint_key                INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_dispatch_complaint PRIMARY KEY,
        chief_complaint_dispatch     NVARCHAR(500)     NOT NULL,  -- 118 distinct; 100% populated
        chief_complaint_anatomic_loc NVARCHAR(500)     NULL       -- 10 distinct; 34% populated
    );

    CREATE UNIQUE NONCLUSTERED INDEX uix_complaint ON {schema}.dim_dispatch_complaint (chief_complaint_dispatch);
END


-- ------------------------------------------------------------
-- 4. dim_ems_providers  (SCD Type 2)
--    Provider certification level can change (BLS → ALS).
--    Historical fact rows must retain the level active at the
--    time of the incident.
--      effective_from  — date this row became active
--      effective_to    — NULL = currently active
--      is_current      — 1 = active row; 0 = historical
-- ------------------------------------------------------------

-- name: create_dim_ems_providers
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'dim_ems_providers'
)
BEGIN
    CREATE TABLE {schema}.dim_ems_providers (
        provider_key                INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_ems_providers PRIMARY KEY,
        provider_type_structure     NVARCHAR(200)     NULL,
        provider_type_service       NVARCHAR(200)     NULL,
        provider_type_service_level NVARCHAR(200)     NOT NULL,  -- "NOT RECORDED" sentinel where blank
        effective_from              DATE              NOT NULL DEFAULT CAST(GETUTCDATE() AS DATE),
        effective_to                DATE              NULL,
        is_current                  BIT               NOT NULL DEFAULT 1
    );

    -- Filtered unique index: one active row per provider combination
    CREATE UNIQUE NONCLUSTERED INDEX uix_providers
        ON {schema}.dim_ems_providers (provider_type_structure, provider_type_service, provider_type_service_level)
        WHERE is_current = 1;
END
