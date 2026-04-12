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
-- Pass criteria (enforced in load.py _DQ_EXPECT_ZERO):
--
--   Check                   Threshold   Rationale
--   ─────────────────────   ─────────   ──────────────────────────────────────────────
--   row_count               > 0         At least one row must have been staged
--   null_incident_dt        = 0         Required field; transform rejects nulls
--   null_incident_cnty      = 0         Required field; transform rejects nulls
--   null_complaint_dispatch = 0         100% populated in source — null signals corruption
--   neg_scene_mins          = 0         Transform rejects negatives; any here = code bug
--   neg_dest_mins           = 0         Same as above
--   future_incident_dt      = 0         Incidents cannot occur in the future
--   invalid_injury_flg      = 0         After transform, only 'YES', 'NO', or NULL allowed
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
-- chief_complaint_dispatch is 100% populated (118 distinct values) in the
-- source CSV. Any NULL here means the CSV is corrupted or a staging bug
-- silently dropped values.
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
-- or NULL. Any other value means an unexpected source code appeared.
-- Same pattern applies to naloxone_given_flg and medication_given_other_flg
-- (covered by same transform logic; spot-check injury_flg as representative).
SELECT COUNT(*)
FROM   {schema}.stg_ems_clean
WHERE  etl_batch_id = :bid
  AND  injury_flg IS NOT NULL
  AND  injury_flg NOT IN ('YES', 'NO')
