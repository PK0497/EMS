-- ============================================================
-- load_fact.sql
-- Bulk-inserts rows into fct_history_ems_incidents by reading
-- from stg_ems_clean and resolving surrogate keys via LEFT JOINs
-- to the three dimension tables.
-- SQLLoader in load.py loads this file as: fact.insert_fact
--
-- Schema: {schema}  (substituted at runtime by SQLLoader.from_file())
--
-- Parameters
--   :bid      (INT)  — etl_batch_id for the current run
--   :inc_from (DATE) — lower-bound date for incremental loads,
--                      or NULL to include all rows in the batch
--
-- Surrogate key resolution
-- ------------------------
-- Each dim is joined on its natural key. When no matching dim row
-- exists (LEFT JOIN returns NULL), the surrogate key column on the
-- fact is left as NULL — no unknown-member sentinel required.
-- Fact rows are never discarded due to an unresolved dim lookup.
--
-- Run this only after load_dims.sql has completed so that all
-- natural keys from the current batch already exist in the dim tables.
-- ============================================================

-- name: insert_fact
INSERT INTO {schema}.fct_history_ems_incidents (
    incident_dt,
    unit_notified_by_dispatch_dt,
    unit_arrived_on_scene_dt,
    unit_arrived_to_patient_dt,
    unit_left_scene_dt,
    patient_arrived_destination_dt,
    geography_key,
    provider_key,
    complaint_key,
    incident_county,
    provider_type_structure,
    provider_type_service,
    provider_type_service_level,
    chief_complaint_dispatch,
    chief_complaint_anatomic_loc,
    primary_symptom,
    provider_impression_primary,
    injury_flg,
    naloxone_given_flg,
    medication_given_other_flg,
    disposition_ed,
    disposition_hospital,
    destination_type,
    provider_to_scene_mins,
    provider_to_destination_mins,
    etl_batch_id
)
SELECT
    s.incident_dt,
    s.unit_notified_by_dispatch_dt,
    s.unit_arrived_on_scene_dt,
    s.unit_arrived_to_patient_dt,
    s.unit_left_scene_dt,
    s.patient_arrived_destination_dt,
    g.geography_key,
    p.provider_key,
    d.complaint_key,
    s.incident_county,
    s.provider_type_structure,
    s.provider_type_service,
    s.provider_type_service_level,
    s.chief_complaint_dispatch,
    s.chief_complaint_anatomic_loc,
    s.primary_symptom,
    s.provider_impression_primary,
    s.injury_flg,
    s.naloxone_given_flg,
    s.medication_given_other_flg,
    s.disposition_ed,
    s.disposition_hospital,
    s.destination_type,
    s.provider_to_scene_mins,
    s.provider_to_destination_mins,
    s.etl_batch_id
FROM       {schema}.stg_ems_clean           s
LEFT JOIN  {schema}.dim_geography            g
        ON g.incident_county             = s.incident_county
LEFT JOIN  {schema}.dim_dispatch_complaint   d
        ON d.chief_complaint_dispatch    = s.chief_complaint_dispatch
LEFT JOIN  {schema}.dim_ems_providers        p
        ON  p.provider_type_structure     = s.provider_type_structure
        AND p.provider_type_service       = s.provider_type_service
        AND p.provider_type_service_level = s.provider_type_service_level
        AND p.is_current = 1
WHERE  s.etl_batch_id = :bid
  AND  (:inc_from IS NULL OR s.incident_dt >= :inc_from);
