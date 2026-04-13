# EMS Data Warehouse — Entity Relationship Diagram

**Project:** Emergency Medical Services (EMS) Analytics  
**Methodology:** Kimball Dimensional Modeling (Star Schema)  
**Target Platform:** SQL Server — schema `{env}_ems` (e.g. `dev_ems`, `test_ems`, `prod_ems`)  
**Grain:** One row per EMS run (one source CSV row)  
**Last Updated:** 2026-04-12  

---

## 1. Star Schema — Warehouse Layer

> **Note on relationships:** Foreign key constraints are not enforced at the database level.  
> Surrogate keys on the fact (`geography_key`, `provider_key`, `complaint_key`) are **nullable INT**.  
> `dim_calendar` is joined via `incident_dt = full_date` (date match), not a surrogate key.  
> All joins are **logical only** — modelled here for documentation purposes.

```mermaid
erDiagram

    dim_calendar {
        int      date_key             PK  "YYYYMMDD integer — PK"
        date     full_date            UK  "Join to fact on incident_dt"
        smallint year
        tinyint  quarter                  "1-4"
        tinyint  month                    "1-12"
        nvarchar month_name               "January … December"
        nchar    month_short              "Jan … Dec"
        tinyint  day                      "1-31"
        tinyint  day_of_week              "1=Sunday … 7=Saturday"
        nvarchar day_name                 "Sunday … Saturday"
        nchar    day_short                "Sun … Sat"
        tinyint  week_of_year             "ISO week 1-53"
        bit      is_weekend
        bit      is_weekday
        bit      is_leap_year
        date     first_day_of_month
        date     last_day_of_month
        date     first_day_of_quarter
        date     last_day_of_quarter
        date     first_day_of_year
        date     last_day_of_year
    }

    dim_geography {
        int      geography_key        PK  "IDENTITY surrogate key"
        nvarchar incident_county      UK  "e.g. BEXAR — 93 distinct; SCD Type 1"
    }

    dim_dispatch_complaint {
        int      complaint_key        PK  "IDENTITY surrogate key"
        nvarchar chief_complaint_dispatch  UK  "118 distinct; 100% populated; SCD Type 1"
        nvarchar chief_complaint_anatomic_loc  "10 distinct; 34% populated"
    }

    dim_ems_providers {
        int      provider_key         PK  "IDENTITY surrogate key"
        nvarchar provider_type_structure   "Nullable — e.g. fire department"
        nvarchar provider_type_service     "Nullable — e.g. 911 response"
        nvarchar provider_type_service_level  UK  "Composite UK with above 2; NOT RECORDED sentinel"
        date     effective_from            "SCD Type 2 — row valid from this date"
        date     effective_to              "SCD Type 2 — NULL means currently active"
        bit      is_current                "SCD Type 2 — 1=active row; 0=historical"
    }

    fct_history_ems_incidents {
        bigint   ems_incident_key     PK  "IDENTITY surrogate key"

        date     incident_dt          FK  "Logical join → dim_calendar.full_date"
        date     unit_notified_by_dispatch_dt   "~99.9% populated"
        date     unit_arrived_on_scene_dt       "~98% populated"
        date     unit_arrived_to_patient_dt     "~64% populated"
        date     unit_left_scene_dt             "~61% populated"
        date     patient_arrived_destination_dt "~78% populated"

        int      geography_key        FK  "Logical join → dim_geography; nullable"
        int      provider_key         FK  "Logical join → dim_ems_providers; nullable"
        int      complaint_key        FK  "Logical join → dim_dispatch_complaint; nullable"

        nvarchar incident_county          "Denormalized — zero-join for DA"
        nvarchar provider_type_structure  "Denormalized — zero-join for DA"
        nvarchar provider_type_service    "Denormalized — zero-join for DA"
        nvarchar provider_type_service_level  "Denormalized — zero-join for DA"
        nvarchar chief_complaint_dispatch     "Denormalized — zero-join for DA"
        nvarchar chief_complaint_anatomic_loc "Denormalized — zero-join for DA"

        nvarchar primary_symptom          "1,231 distinct; 82% populated"
        nvarchar provider_impression_primary  "1,813 distinct; 86% populated"

        nvarchar injury_flg               "YES / NO / UNKNOWN"
        nvarchar naloxone_given_flg       "Source-dependent flag values"
        nvarchar medication_given_other_flg  "Source-dependent flag values"

        nvarchar disposition_ed           "21 distinct; ~3% populated"
        nvarchar disposition_hospital     "19 distinct; ~2.5% populated"
        nvarchar destination_type         "27 distinct; high volume"

        smallint provider_to_scene_mins       "Measure — minutes; CHECK >= 0"
        smallint provider_to_destination_mins "Measure — minutes; CHECK >= 0"

        int      etl_batch_id             "Unix epoch seconds; tags every load run"
        datetime2 etl_loaded_at           "DEFAULT SYSUTCDATETIME()"
    }

    dim_calendar          ||--o{ fct_history_ems_incidents : "full_date = incident_dt"
    dim_geography         ||--o{ fct_history_ems_incidents : "geography_key (nullable)"
    dim_dispatch_complaint ||--o{ fct_history_ems_incidents : "complaint_key (nullable)"
    dim_ems_providers     ||--o{ fct_history_ems_incidents : "provider_key (nullable)"
```

---

## 2. Staging Layer

> The staging layer is **not part of the DW model** — it exists solely for the ETL pipeline.  
> Both tables are year-agnostic and append-only. `etl_batch_id` links every row back to its pipeline run.

```mermaid
erDiagram

    stg_ems_raw {
        nvarchar  incident_dt                    "Raw string — preserved verbatim from CSV"
        nvarchar  incident_county
        nvarchar  chief_complaint_dispatch
        nvarchar  chief_complaint_anatomic_loc
        nvarchar  primary_symptom
        nvarchar  provider_impression_primary
        nvarchar  disposition_ed
        nvarchar  disposition_hospital
        nvarchar  injury_flg
        nvarchar  naloxone_given_flg
        nvarchar  medication_given_other_flg
        nvarchar  destination_type
        nvarchar  provider_type_structure
        nvarchar  provider_type_service
        nvarchar  provider_type_service_level
        nvarchar  provider_to_scene_mins         "Stored as string; no casting"
        nvarchar  provider_to_destination_mins
        nvarchar  unit_notified_by_dispatch_dt
        nvarchar  unit_arrived_on_scene_dt
        nvarchar  unit_arrived_to_patient_dt
        nvarchar  unit_left_scene_dt
        nvarchar  patient_arrived_destination_dt
        smallint  source_year                    "e.g. 2024 — extracted from filename"
        nvarchar  source_file                    "e.g. ems_runs_2024 (1).csv"
        int       etl_batch_id                   "Links to stg_ems_clean and fact"
        datetime2 etl_loaded_at
    }

    stg_ems_clean {
        date     incident_dt                    "Parsed DATE; NULL if unparseable"
        date     unit_notified_by_dispatch_dt
        date     unit_arrived_on_scene_dt
        date     unit_arrived_to_patient_dt
        date     unit_left_scene_dt
        date     patient_arrived_destination_dt
        nvarchar incident_county                "Case-normalized; dim natural key"
        nvarchar chief_complaint_dispatch       "Case-normalized; dim natural key"
        nvarchar chief_complaint_anatomic_loc
        nvarchar provider_type_structure        "Case-normalized; dim natural key"
        nvarchar provider_type_service
        nvarchar provider_type_service_level    "NOT RECORDED sentinel when blank"
        nvarchar primary_symptom
        nvarchar provider_impression_primary
        nvarchar disposition_ed
        nvarchar disposition_hospital
        nvarchar injury_flg                     "YES / NO / UNKNOWN (uppercased)"
        nvarchar naloxone_given_flg
        nvarchar medication_given_other_flg
        nvarchar destination_type
        smallint provider_to_scene_mins         "Cast + validated >= 0"
        smallint provider_to_destination_mins
        smallint source_year
        nvarchar source_file
        int      etl_batch_id
        datetime2 etl_loaded_at
    }

    stg_ems_raw ||--|| stg_ems_clean : "same etl_batch_id (valid rows only)"
```

---

## 3. ETL Data Flow

```mermaid
flowchart TD
    subgraph SOURCE["Source"]
        CSV["📄 ems_runs_YYYY.csv\n~1.6M rows · 22 columns"]
    end

    subgraph PYTHON["ETL Pipeline  (main.py)"]
        direction TB
        E["extract.py\nChunked CSV read\nbatch_size = 100,000"]
        T["transform.py\nCase normalize · Parse dates\nCast numerics · Reject invalid"]
        R["rejects/rejects_YYYY_ts.csv\nRows failing validation"]
    end

    subgraph STAGING["Staging Layer  ─  {schema}"]
        RAW["stg_ems_raw\n🗂 HEAP · All NVARCHAR\nAudit / triage"]
        CLN["stg_ems_clean\n✅ Typed · Normalized\nSole DW source"]
    end

    subgraph DQ["Phase C — DQ Checks  (dq_checks.sql)"]
        DQC["7 hard + 1 warn-only assertions\nrow_count · nulls · flags · future dates\n❌ FAIL → pipeline aborts"]
    end

    subgraph DW["Data Warehouse  ─  {schema}"]
        direction TB
        subgraph DIMS["Dimensions  (load_dims.sql · MERGE)"]
            GEO["dim_geography\nSCD Type 1"]
            COMP["dim_dispatch_complaint\nSCD Type 1"]
            PROV["dim_ems_providers\nSCD Type 2"]
            CAL["dim_calendar\nSCD Type 0\n(auto-populated 2020–2030)"]
        end
        FCT["fct_history_ems_incidents\n(load_fact.sql · INSERT…SELECT)\nWide enriched fact · zero-join columns"]
    end

    CSV --> E
    E --> T
    T --> R
    E -->|"raw chunk\n(every row)"| RAW
    T -->|"valid rows only"| CLN
    CLN --> DQC
    DQC -->|"✅ All pass"| GEO
    DQC -->|"✅ All pass"| COMP
    DQC -->|"✅ All pass"| PROV
    GEO -->|"geography_key"| FCT
    COMP -->|"complaint_key"| FCT
    PROV -->|"provider_key"| FCT
    CAL -.->|"full_date = incident_dt\n(date join)"| FCT
    CLN -->|"INSERT…SELECT\nwith LEFT JOINs"| FCT
```

---

## 4. SCD Strategy Reference

| Table | SCD Type | Strategy | Rationale |
|---|---|---|---|
| `dim_calendar` | Type 0 | Static — rows never change | Date attributes are immutable facts |
| `dim_geography` | Type 1 | Overwrite in place | County names are stable; corrections replace the old value |
| `dim_dispatch_complaint` | Type 1 | Overwrite in place | NEMSIS dispatch codes updated at source version level |
| `dim_ems_providers` | Type 2 | Insert new row; close old | Certification level upgrades (BLS→ALS) must be traceable in history |

---

## 5. Index Reference

| Table | Index Name | Columns | Type | Purpose |
|---|---|---|---|---|
| `dim_calendar` | `uix_calendar_date` | `full_date` | UNIQUE NONCLUSTERED | Date join from fact |
| `dim_geography` | `uix_geography_county` | `incident_county` | UNIQUE NONCLUSTERED | MERGE natural key lookup |
| `dim_dispatch_complaint` | `uix_complaint` | `chief_complaint_dispatch` | UNIQUE NONCLUSTERED | MERGE natural key lookup |
| `dim_ems_providers` | `uix_providers` | `structure, service, level` WHERE `is_current=1` | UNIQUE NONCLUSTERED (filtered) | Enforce one active row per provider |
| `fct_history_ems_incidents` | `ix_fct_incident_dt` | `incident_dt` INCLUDE measures | NONCLUSTERED | Time-series dashboard filter |
| `fct_history_ems_incidents` | `ix_fct_county` | `incident_county` | NONCLUSTERED | Geographic group-by |
| `fct_history_ems_incidents` | `ix_fct_provider` | `provider_key` INCLUDE provider cols | NONCLUSTERED | Dimensional join + service analysis |
| `fct_history_ems_incidents` | `ix_fct_batch` | `etl_batch_id` | NONCLUSTERED | Batch-level reloads / deletes |
| `stg_ems_raw` | `ix_raw_batch` | `etl_batch_id` | NONCLUSTERED | Idempotent batch deletes |
| `stg_ems_raw` | `ix_raw_year` | `source_year` | NONCLUSTERED | Per-year staging queries |
| `stg_ems_clean` | `ix_stgc_batch` | `etl_batch_id` | NONCLUSTERED | DQ checks + DW load filter |
| `stg_ems_clean` | `ix_stgc_year` | `source_year` | NONCLUSTERED | Per-year staging queries |

---

## 6. Deployment Order

All DDL is executed **automatically** by `init_db()` in `etl/load.py` at pipeline
startup. Every block uses `IF NOT EXISTS` — safe to run on every execution.

```
Automatic (pipeline startup):
  1. init_schema.sql   → CREATE SCHEMA {schema}
  2. staging.sql       → stg_ems_raw (Bronze), stg_ems_clean (Silver)
  3. dimensions.sql    → dim_calendar, dim_geography,
                          dim_dispatch_complaint, dim_ems_providers  (Gold)
  4. facts.sql         → fct_history_ems_incidents  (Gold)

Automatic (dim load step):
  5. populate_calendar  → date spine 2020-01-01 through 2030-12-31 (idempotent)
```
