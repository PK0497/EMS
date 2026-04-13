-- ============================================================
-- dq_checks.sql
-- Post-staging data quality assertions against stg_ems_clean.
-- Each section is introduced by a "-- name: <query_name>" header.
-- SQLLoader in load.py parses these headers and gives Python
-- attribute-style access:  dq.row_count, dq.null_incident_dt, etc.
--
-- Schema: {schema}  (substituted at runtime by SQLLoader.from_file())
--
-- Parameter: :bid (INT) — etl_batch_id for the current run.
--
-- All checks return a single integer count.
-- Pass criteria (enforced in load.py _DQ_EXPECT_ZERO / _DQ_WARN_ONLY):
--
--   Check                   Type   Threshold   Rationale
--   ─────────────────────   ────   ─────────   ─────────────────────────────────────
--   row_count               Hard   > 0         At least one row must have been staged
--   null_incident_dt        Hard   = 0         Required field; transform rejects nulls
--   null_incident_cnty      Hard   = 0         Required field; transform rejects nulls
--   neg_scene_mins          Hard   = 0         Transform rejects negatives; any here = code bug
--   neg_dest_mins           Hard   = 0         Same as above
--   future_incident_dt      Hard   = 0         Incidents cannot occur in the future
--   invalid_injury_flg      Hard   = 0         After transform, only YES/NO/UNKNOWN/NULL allowed
--   null_complaint_dispatch Warn   logged      Source has sparse NULLs (~0.01%); not a code bug
-- ============================================================


-- ── Completeness ─────────────────────────────────────────────

-- name: row_count
-- Batch must contain at least one staged row.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid

-- name: null_incident_dt
-- incident_dt is a required field; transform already rejects nulls.
-- Any residual nulls here indicate an unexpected code path.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  incident_dt IS NULL

-- name: null_incident_cnty
-- incident_county is required; 93 distinct values; 100% populated in source.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  incident_county IS NULL

-- name: null_complaint_dispatch
-- chief_complaint_dispatch is nearly 100% populated (118 distinct values).
-- A small number of NULLs exist in the source data and are acceptable.
-- Treated as warn-only in load.py — logged but does not abort the pipeline.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  chief_complaint_dispatch IS NULL


-- ── Numeric measures ─────────────────────────────────────────

-- name: neg_scene_mins
-- Transform rejects negative provider_to_scene_mins. Any here = code bug.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  provider_to_scene_mins < 0

-- name: neg_dest_mins
-- Transform rejects negative provider_to_destination_mins. Any here = code bug.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  provider_to_destination_mins < 0


-- ── Temporal validity ─────────────────────────────────────────

-- name: future_incident_dt
-- EMS incidents cannot occur in the future. Any row with incident_dt
-- beyond today indicates a data entry error or system clock skew.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  incident_dt > CAST(GETDATE() AS DATE)


-- ── Value-set conformance ─────────────────────────────────────

-- name: invalid_injury_flg
-- After case normalization in transform, injury_flg must be 'YES', 'NO',
-- 'UNKNOWN', or NULL. NEMSIS sources commonly include 'UNKNOWN' as a
-- legitimate response value.
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  injury_flg IS NOT NULL
  AND  injury_flg NOT IN ('YES', 'NO', 'UNKNOWN')
