-- ============================================================
-- staging.sql — TWO-TABLE STAGING DESIGN  (Bronze + Silver)
--
-- Schema: {schema}  (resolved at runtime as {env}_ems)
-- Executed automatically by init_db() in etl/load.py.
-- Safe to run multiple times — every block is idempotent.
--
-- ── stg_ems_raw  (Bronze) ──────────────────────────────────
--   Raw landing zone. Every column is NVARCHAR, preserving
--   the original CSV value exactly as it came off disk.
--   Column names are lowercase (project-wide convention).
--   Purpose: permanent audit trail and triage layer.
--
-- ── stg_ems_clean  (Silver) ────────────────────────────────
--   Normalized clean zone. Populated from the Python transform
--   output (proper DATE / SMALLINT types, case-normalized
--   string values). Sole source for all Gold layer SQL loads
--   (dim MERGE + fact INSERT...SELECT).
--
-- Both tables are year-agnostic. source_year + source_file
-- identify which CSV file produced each row. etl_batch_id
-- links every row back to the pipeline run that loaded it.
-- ============================================================


-- ============================================================
-- Bronze: {schema}.stg_ems_raw
-- ============================================================

-- name: create_stg_ems_raw
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'stg_ems_raw'
)
BEGIN
    CREATE TABLE {schema}.stg_ems_raw (

        -- Source columns — all NVARCHAR (no type assumption;
        -- original string value is preserved verbatim)
        incident_dt                    NVARCHAR(50)   NULL,  -- e.g. "2024-01-15";           ~99.9% populated
        incident_county                NVARCHAR(100)  NULL,  -- e.g. "BEXAR";                93 distinct; 100% populated
        chief_complaint_dispatch       NVARCHAR(500)  NULL,  -- e.g. "chest pain, ...";      118 distinct; 100% populated
        chief_complaint_anatomic_loc   NVARCHAR(500)  NULL,  -- e.g. "CHEST";                10 distinct;  34% populated
        primary_symptom                NVARCHAR(1000) NULL,  -- e.g. "Chest Pain/Discomfort"; 1,231 distinct; 82% populated
        provider_impression_primary    NVARCHAR(1000) NULL,  -- e.g. "Chest Pain Non-Traumatic"; 1,813 distinct; 86% populated
        disposition_ed                 NVARCHAR(500)  NULL,  -- e.g. "Patient Treated, Transported"; 21 distinct; ~3% populated
        disposition_hospital           NVARCHAR(500)  NULL,  -- e.g. "Admitted";             19 distinct; ~2.5% populated
        injury_flg                     NVARCHAR(20)   NULL,  -- "Yes" / "No";                binary flag
        naloxone_given_flg             NVARCHAR(20)   NULL,  -- "Yes" / "No";                binary flag
        medication_given_other_flg     NVARCHAR(20)   NULL,  -- "Yes" / "No";                binary flag
        destination_type               NVARCHAR(200)  NULL,  -- e.g. "Hospital";             27 distinct; high volume
        provider_type_structure        NVARCHAR(200)  NULL,  -- e.g. "emergency medical services"
        provider_type_service          NVARCHAR(200)  NULL,  -- e.g. "ems - advanced life support"
        provider_type_service_level    NVARCHAR(200)  NULL,  -- e.g. "advanced life support"; sentinel "NOT RECORDED" where blank
        provider_to_scene_mins         NVARCHAR(50)   NULL,  -- numeric string; validated >= 0 in transform
        provider_to_destination_mins   NVARCHAR(50)   NULL,  -- numeric string; validated >= 0 in transform
        unit_notified_by_dispatch_dt   NVARCHAR(50)   NULL,  -- e.g. "2024-01-15";           ~99.9% populated
        unit_arrived_on_scene_dt       NVARCHAR(50)   NULL,  -- e.g. "2024-01-15";           ~98%   populated
        unit_arrived_to_patient_dt     NVARCHAR(50)   NULL,  -- e.g. "2024-01-15";           ~64%   populated
        unit_left_scene_dt             NVARCHAR(50)   NULL,  -- e.g. "2024-01-15";           ~61%   populated
        patient_arrived_destination_dt NVARCHAR(50)   NULL,  -- e.g. "2024-01-15";           ~78%   populated

        -- Source provenance
        source_year     SMALLINT      NULL,  -- e.g. 2024; extracted from filename
        source_file     NVARCHAR(500) NULL,  -- e.g. "ems_runs_2024 (1).csv"

        -- ETL audit
        etl_batch_id    INT       NULL,
        etl_loaded_at   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    -- Heap (no clustered index) — maximises bulk INSERT throughput.
    -- For very large initial loads (50M+ rows): disable indexes before
    -- load, rebuild in a single pass afterwards.
    CREATE NONCLUSTERED INDEX ix_raw_batch ON {schema}.stg_ems_raw (etl_batch_id);
    CREATE NONCLUSTERED INDEX ix_raw_year  ON {schema}.stg_ems_raw (source_year);
END


-- ============================================================
-- Silver: {schema}.stg_ems_clean
-- ============================================================

-- name: create_stg_ems_clean
IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN   sys.schemas s ON s.schema_id = t.schema_id
    WHERE  s.name = '{schema}' AND t.name = 'stg_ems_clean'
)
BEGIN
    CREATE TABLE {schema}.stg_ems_clean (

        -- Date columns (parsed from string; NULL where unparseable)
        incident_dt                    DATE NULL,  -- primary incident date; required field (~99.9% populated)
        unit_notified_by_dispatch_dt   DATE NULL,  -- unit dispatch notification (~99.9% populated)
        unit_arrived_on_scene_dt       DATE NULL,  -- unit on-scene arrival (~98% populated)
        unit_arrived_to_patient_dt     DATE NULL,  -- unit reached patient (~64% populated)
        unit_left_scene_dt             DATE NULL,  -- unit departed scene (~61% populated)
        patient_arrived_destination_dt DATE NULL,  -- patient arrived at destination (~78% populated)

        -- Geography dimension natural key
        incident_county                NVARCHAR(100)  NULL,  -- e.g. "BEXAR"; 93 distinct; required field

        -- Dispatch complaint dimension natural keys
        chief_complaint_dispatch       NVARCHAR(500)  NULL,  -- 118 distinct; 100% populated
        chief_complaint_anatomic_loc   NVARCHAR(500)  NULL,  -- 10 distinct; 34% populated

        -- Provider dimension natural keys
        provider_type_structure        NVARCHAR(200)  NULL,
        provider_type_service          NVARCHAR(200)  NULL,
        provider_type_service_level    NVARCHAR(200)  NULL,  -- "NOT RECORDED" sentinel when blank

        -- Degenerate / enrichment columns (land directly on Gold fact)
        primary_symptom                NVARCHAR(1000) NULL,  -- 1,231 distinct; 82% populated
        provider_impression_primary    NVARCHAR(1000) NULL,  -- 1,813 distinct; 86% populated
        disposition_ed                 NVARCHAR(500)  NULL,  -- 21 distinct; ~3% populated
        disposition_hospital           NVARCHAR(500)  NULL,  -- 19 distinct; ~2.5% populated
        injury_flg                     NVARCHAR(20)   NULL,  -- YES / NO
        naloxone_given_flg             NVARCHAR(20)   NULL,  -- YES / NO
        medication_given_other_flg     NVARCHAR(20)   NULL,  -- YES / NO
        destination_type               NVARCHAR(200)  NULL,  -- 27 distinct; high volume

        -- Numeric measures (cast and validated in Python transform)
        provider_to_scene_mins         SMALLINT NULL,  -- minutes dispatch → scene; validated >= 0
        provider_to_destination_mins   SMALLINT NULL,  -- minutes scene → destination; validated >= 0

        -- Source provenance
        source_year     SMALLINT      NULL,
        source_file     NVARCHAR(500) NULL,

        -- ETL audit
        etl_batch_id    INT       NULL,
        etl_loaded_at   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE NONCLUSTERED INDEX ix_stgc_batch ON {schema}.stg_ems_clean (etl_batch_id);
    CREATE NONCLUSTERED INDEX ix_stgc_year  ON {schema}.stg_ems_clean (source_year);
END
