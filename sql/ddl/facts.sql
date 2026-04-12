-- ============================================================
-- facts.sql — FACT TABLE  fct_history_ems_incidents  (Gold Layer)
--
-- Schema: {schema}  (resolved at runtime as {env}_ems)
-- Executed automatically by init_db() in etl/load.py.
-- Safe to run multiple times — block is idempotent.
-- Run after init_schema.sql, staging.sql, and dimensions.sql.
--
-- Grain: one row per EMS run (one source CSV row)
-- Design: wide / enriched fact — all source columns present.
--   Surrogate keys are nullable; no FK constraints enforced.
--   Denormalized dim values enable zero-join DA queries.
-- ============================================================

-- name: create_fct_history_ems_incidents
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'fct_history_ems_incidents'
)
BEGIN
    CREATE TABLE {schema}.fct_history_ems_incidents (
        ems_incident_key              BIGINT    IDENTITY(1,1) NOT NULL
            CONSTRAINT PK_fct_history_ems_incidents PRIMARY KEY,

        -- Date milestones (all DATE or NULL)
        -- Join to dim_calendar via: incident_dt = full_date
        incident_dt                   DATE      NULL,   -- ~99.9% populated
        unit_notified_by_dispatch_dt  DATE      NULL,   -- ~99.9% populated
        unit_arrived_on_scene_dt      DATE      NULL,   -- ~98%   populated
        unit_arrived_to_patient_dt    DATE      NULL,   -- ~64%   populated
        unit_left_scene_dt            DATE      NULL,   -- ~61%   populated
        patient_arrived_destination_dt DATE     NULL,   -- ~78%   populated

        -- Dimension surrogate keys (nullable — no FK constraints)
        -- NULL = no matching dim row found for this batch
        geography_key                 INT       NULL,
        provider_key                  INT       NULL,
        complaint_key                 INT       NULL,

        -- Denormalized geography value (zero-join for DA)
        incident_county               NVARCHAR(100)  NULL,  -- 93 distinct

        -- Denormalized provider values (zero-join for DA)
        provider_type_structure       NVARCHAR(200)  NULL,
        provider_type_service         NVARCHAR(200)  NULL,
        provider_type_service_level   NVARCHAR(200)  NULL,

        -- Denormalized complaint values (zero-join for DA)
        chief_complaint_dispatch      NVARCHAR(500)  NULL,  -- 118 distinct; 100% populated
        chief_complaint_anatomic_loc  NVARCHAR(500)  NULL,  -- 10 distinct;  34% populated

        -- Clinical
        primary_symptom               NVARCHAR(1000) NULL,  -- 1,231 distinct; 82% populated
        provider_impression_primary   NVARCHAR(1000) NULL,  -- 1,813 distinct; 86% populated

        -- Response flags
        injury_flg                    NVARCHAR(20)   NULL,  -- YES / NO
        naloxone_given_flg            NVARCHAR(20)   NULL,  -- YES / NO
        medication_given_other_flg    NVARCHAR(20)   NULL,  -- YES / NO

        -- Disposition / destination
        disposition_ed                NVARCHAR(500)  NULL,  -- 21 distinct; ~3% populated
        disposition_hospital          NVARCHAR(500)  NULL,  -- 19 distinct; ~2.5% populated
        destination_type              NVARCHAR(200)  NULL,  -- 27 distinct; high volume

        -- Measures
        provider_to_scene_mins        SMALLINT NULL,
        provider_to_destination_mins  SMALLINT NULL,

        -- ETL audit
        etl_batch_id                  INT       NOT NULL,
        etl_loaded_at                 DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT CK_fct_scene_mins CHECK (provider_to_scene_mins >= 0),
        CONSTRAINT CK_fct_dest_mins  CHECK (provider_to_destination_mins >= 0)
    );

    -- Minimum index set — 4 indexes to keep write amplification low.
    -- For large initial loads: disable all indexes before INSERT,
    -- then ALTER INDEX ALL ON {schema}.fct_history_ems_incidents REBUILD.

    -- 1. Date — primary filter for all time-series dashboards
    CREATE NONCLUSTERED INDEX ix_fct_incident_dt
        ON {schema}.fct_history_ems_incidents (incident_dt)
        INCLUDE (provider_to_scene_mins, provider_to_destination_mins);

    -- 2. County — geographic group-by for operational reports
    CREATE NONCLUSTERED INDEX ix_fct_county
        ON {schema}.fct_history_ems_incidents (incident_county);

    -- 3. Provider — dimensional join + service-level analysis
    CREATE NONCLUSTERED INDEX ix_fct_provider
        ON {schema}.fct_history_ems_incidents (provider_key)
        INCLUDE (provider_type_structure, provider_type_service, provider_type_service_level);

    -- 4. ETL batch — batch-level reloads and idempotent deletes
    CREATE NONCLUSTERED INDEX ix_fct_batch
        ON {schema}.fct_history_ems_incidents (etl_batch_id);
END
