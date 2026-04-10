# EMS NEMSIS Data Engineering Assessment Solution

## 1) Architecture and Data Flow

This repository implements an end-to-end ETL for EMS incident records:

1. **Extract** (`extract.py`)
   - Reads CSV as strings to preserve source fidelity.
   - Adds `source_row_number` for reject traceability.
   - Adds unicode source column `unicode_notes`.
2. **Stage** (`load.py`)
   - Loads all raw rows to `stg_ems_raw` for auditability.
   - Loads validated rows to `stg_ems_valid`.
3. **Transform / Validate** (`transform.py`)
   - Type conversions, normalization, and domain checks.
   - Produces reject rows with `incident_id`, `source_row_number`, `error_type`, and `error_message`.
4. **Load DW** (`load.py`)
   - Loads dimensions first, then fact table.
   - Uses surrogate keys and unknown-member keys (`0`) to preserve referential integrity.
5. **Log and Control** (`load.py`)
   - Run-level and step-level logs.
   - Watermark control table for incremental mode.

---

## 2) Kimball Dimensional Model

### Business Process
EMS operational incident reporting for BI/statistical analysis.

### Grain
`fact_ems_incident` is **one row per EMS incident (`incident_id`)**.

### Conformed Dimensions
- `dim_date`
- `dim_patient`
- `dim_location`
- `dim_agency`

### Surrogate Keys and Unknown Members
- Surrogate keys are used in all dimensions.
- Unknown/default members are pre-seeded with key `0` in each dimension.
- Facts map to key `0` when a dimension lookup is unavailable, preventing FK breaks.

### SCD Strategy
- `dim_patient`: **SCD Type 2** (`start_date`, `end_date`, `is_current`) for historical change tracking of patient demographics.
- `dim_location` and `dim_agency`: **Type 1** overwrite style (non-historical by design, low volatility attributes).

---

## 3) Data Quality Rules

Implemented in `transform.py`:

- Required fields:
  - `incident_id`, `patient_id`, `incident_date`, `age`, `response_time`
- Type/domain validation:
  - `age` must be integer in `[0,120]`
  - `response_time` must be integer `>= 0`
  - `gender` must be one of configured values (`M`, `F`, `U`, `UNKNOWN`)
- Normalization:
  - String trimming, uppercasing, defaulting blank dimensions to `UNKNOWN`
- Derivations:
  - `date_key` (`YYYYMMDD`) and date parts (`year`, `month`, `day`)

Failures are written to reject output with diagnostic context.

---

## 4) Logging, Error Handling, and Rerunnability

### Logging Tables
- `etl_run_log`: run start/end, status, load mode, row counts, and run-level error text.
- `etl_step_log`: per-step status and rows affected.
- `etl_rejects`: reject diagnostics per failed source row.

### Error Handling
- Pipeline uses `try/except` in `main.py`.
- Exceptions are persisted to logs and run marked as `failure`.

### Rerunnability
- **Full mode**: truncates stage + DW (except unknown members), then reloads deterministically.
- **Incremental mode**: filters source rows using `ctl_watermark` (`incident_date` cutoff), then upserts facts.

---

## 5) Large Data Considerations

Even with small sample data, the design is built for volume:

- Set-based SQL inserts/updates for dimensions/facts.
- Bulk append with configurable `batch_size` (`to_sql(..., chunksize=...)`).
- Minimal row-by-row operations.
- Separate staging and DW indexing strategy.
- Incremental watermark pattern.
- Raw extraction avoids high-memory reshaping patterns.

---

## 6) Parameterization and Modularity

All runtime behavior is configurable through `config.yaml` (no code changes required):

- input file path
- DB connection URL
- batch size
- load mode (`full` / `incremental`)
- source system identifier
- validation domains (allowed gender values)

Code is separated cleanly into:
- `extract.py`
- `transform.py`
- `load.py`
- `main.py` (orchestration)

---

## 7) Repository Contents (Assessment Deliverables)

- SQL DDL scripts:
  - `staging.sql`
  - `dimensions.sql`
  - `facts.sql`
  - `logging.sql`
- ETL code:
  - `extract.py`
  - `transform.py`
  - `load.py`
  - `main.py`
- Config:
  - `config.yaml`
- Schema diagram:
  - `ERD Diagram.jpg`

---

## 8) How to Run End-to-End

1. Install dependencies:
   - `pip install pandas sqlalchemy pyyaml`
2. Configure `config.yaml` as needed.
3. Run:
   - `python main.py`
4. Review outputs in DB:
   - `stg_ems_raw`, `stg_ems_valid`, `dim_*`, `fact_ems_incident`
   - `etl_run_log`, `etl_step_log`, `etl_rejects`, `ctl_watermark`

---

## 9) Assumptions and Decisions

- SQL Server is the target platform in SQL scripts; SQLite is used for local runnable demo.
- Incident uniqueness is assumed by `incident_id`.
- Current sample has one file source; modular design supports extension to multiple files/sources.
- NEMSIS crosswalk/version mapping is represented by preserving raw source and standardized columns; this can be extended with dedicated reference/crosswalk tables.

---

## 10) Requirement Coverage Checklist

- [x] Kimball model with defined business process and grain
- [x] Conformed dimensions and surrogate keys
- [x] Unknown/default member strategy
- [x] SCD handling with rationale
- [x] Raw staging with traceability
- [x] Cleansing, normalization, domain validation
- [x] Reject/error output with diagnostics
- [x] Dimensions loaded before facts
- [x] Referential integrity design
- [x] Run/step logging and row counts
- [x] Graceful failure and rerunnable loads
- [x] Large-data-friendly loading patterns
- [x] Parameterized and modular ETL
- [x] Source-controlled SQL, ETL code, config, and schema diagram
