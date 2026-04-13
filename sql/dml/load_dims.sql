-- ============================================================
-- load_dims.sql
-- Populates all three dimension tables from stg_ems_clean
-- using set-based MERGE statements (SQL Server T-SQL).
-- SQLLoader in load.py parses the -- name: headers and executes
-- each statement individually via conn.execute(text(...), params).
--
-- Schema: {schema}  (substituted at runtime by SQLLoader.from_file())
--
-- Parameters (passed per statement)
--   :bid      (INT)  — etl_batch_id for the current run
--   :inc_from (DATE) — lower-bound date for incremental loads,
--                      or NULL to include all rows in the batch
--
-- The (:inc_from IS NULL OR incident_dt >= :inc_from) predicate
-- handles both full and incremental runs with the same static
-- SQL — no dynamic string building needed in Python.
--
-- Execution order matters: dims must be fully populated before
-- load_fact.sql runs.
-- ============================================================


-- name: populate_calendar
-- dim_calendar  (SCD Type 0 — one-time static date spine)
-- Only inserts if the table is empty (idempotent).
-- Generates dates from 2020-01-01 to 2030-12-31 covering all
-- practical EMS incident years.
IF NOT EXISTS (SELECT 1 FROM {schema}.dim_calendar)
BEGIN
    ;WITH date_cte AS (
        SELECT CAST('2020-01-01' AS DATE) AS dt
        UNION ALL
        SELECT DATEADD(DAY, 1, dt) FROM date_cte WHERE dt < '2030-12-31'
    )
    INSERT INTO {schema}.dim_calendar (
        date_key, full_date, year, quarter, month, month_name, month_short,
        day, day_of_week, day_name, day_short, week_of_year,
        is_weekend, is_weekday, is_leap_year,
        first_day_of_month, last_day_of_month,
        first_day_of_quarter, last_day_of_quarter,
        first_day_of_year, last_day_of_year
    )
    SELECT
        YEAR(dt) * 10000 + MONTH(dt) * 100 + DAY(dt),
        dt,
        YEAR(dt),
        DATEPART(QUARTER, dt),
        MONTH(dt),
        DATENAME(MONTH, dt),
        LEFT(DATENAME(MONTH, dt), 3),
        DAY(dt),
        DATEPART(WEEKDAY, dt),
        DATENAME(WEEKDAY, dt),
        LEFT(DATENAME(WEEKDAY, dt), 3),
        DATEPART(ISO_WEEK, dt),
        CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 1 ELSE 0 END,
        CASE WHEN DATEPART(WEEKDAY, dt) IN (1, 7) THEN 0 ELSE 1 END,
        CASE WHEN (YEAR(dt) % 4 = 0 AND YEAR(dt) % 100 != 0) OR YEAR(dt) % 400 = 0 THEN 1 ELSE 0 END,
        DATEFROMPARTS(YEAR(dt), MONTH(dt), 1),
        EOMONTH(dt),
        DATEFROMPARTS(YEAR(dt), (DATEPART(QUARTER, dt) - 1) * 3 + 1, 1),
        EOMONTH(DATEFROMPARTS(YEAR(dt), DATEPART(QUARTER, dt) * 3, 1)),
        DATEFROMPARTS(YEAR(dt), 1, 1),
        DATEFROMPARTS(YEAR(dt), 12, 31)
    FROM date_cte
    OPTION (MAXRECURSION 5000);
END


-- name: merge_geography
-- dim_geography  (SCD Type 1 — county name is stable)
MERGE {schema}.dim_geography AS tgt
USING (
    SELECT DISTINCT incident_county
    FROM   {schema}.stg_ems_clean
    WHERE  etl_batch_id = :bid
      AND  incident_county IS NOT NULL
      AND  (:inc_from IS NULL OR incident_dt >= :inc_from)
) AS src
ON tgt.incident_county = src.incident_county
WHEN NOT MATCHED THEN
    INSERT (incident_county)
    VALUES (src.incident_county);


-- name: merge_complaint
-- dim_dispatch_complaint  (SCD Type 1 — dispatch codes are stable)
-- chief_complaint_anatomic_loc: MIN() takes the first encountered
-- value per dispatch code — no history tracking needed.
MERGE {schema}.dim_dispatch_complaint AS tgt
USING (
    SELECT   chief_complaint_dispatch,
             MIN(chief_complaint_anatomic_loc) AS chief_complaint_anatomic_loc
    FROM     {schema}.stg_ems_clean
    WHERE    etl_batch_id = :bid
      AND    chief_complaint_dispatch IS NOT NULL
      AND    (:inc_from IS NULL OR incident_dt >= :inc_from)
    GROUP BY chief_complaint_dispatch
) AS src
ON tgt.chief_complaint_dispatch = src.chief_complaint_dispatch
WHEN NOT MATCHED THEN
    INSERT (chief_complaint_dispatch, chief_complaint_anatomic_loc)
    VALUES (src.chief_complaint_dispatch, src.chief_complaint_anatomic_loc);


-- name: merge_providers
-- dim_ems_providers  (SCD Type 2 — new combinations inserted as
-- current rows; expiry of changed rows is a separate future step)
MERGE {schema}.dim_ems_providers AS tgt
USING (
    SELECT DISTINCT
        provider_type_structure,
        provider_type_service,
        provider_type_service_level
    FROM  {schema}.stg_ems_clean
    WHERE etl_batch_id = :bid
      AND (:inc_from IS NULL OR incident_dt >= :inc_from)
) AS src
ON  tgt.provider_type_structure     = src.provider_type_structure
AND tgt.provider_type_service       = src.provider_type_service
AND tgt.provider_type_service_level = src.provider_type_service_level
AND tgt.is_current = 1
WHEN NOT MATCHED THEN
    INSERT (provider_type_structure, provider_type_service,
            provider_type_service_level, is_current, effective_from)
    VALUES (src.provider_type_structure, src.provider_type_service,
            src.provider_type_service_level, 1, CAST(GETDATE() AS DATE));
