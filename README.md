# EMS NEMSIS Data Engineering Pipeline
## Business Scenario
This project addresses the challenge of processing disparate Emergency Medical Services (EMS) records. The solution ingests raw CSV data mapped to the National EMS Information System (NEMSIS) standard, validates it against strict clinical data rules, and transforms it into a Kimball-style Star Schema for high-performance analytical reporting.

## Architecture and Medallion Flow
The pipeline is built with modularity and scalability in mind, following a three-tier logical flow:
Staging (Bronze): Raw ingestion of CSV records into stg_ems_raw. This layer preserves the source of truth for auditability and includes the mandatory unicode_notes column.
Transformation (Silver): Data cleaning, null handling, and domain validation. Records that fail validation (e.g., missing IDs or illogical timestamps) are partitioned into an etl_rejects table.
Warehouse (Gold): Final loading into the Star Schema using Surrogate Keys and Type 2 Slowly Changing Dimensions (SCD).

## Dimensional Model
The warehouse is optimized for BI tools and statistical analysts.
Fact Table
fact_ems_incident: The primary grain is one row per EMS incident.
Measures: Response time, transport distance, procedure counts.
Keys: Foreign keys to all dimensions.
Dimensions
dim_patient: Implements SCD Type 2 logic (EffectiveDate, EndDate, IsCurrent) to track changes in patient demographics over time.
dim_location: Conformed dimension for Pick-up and Drop-off locations.
dim_agency: Contains metadata regarding the responding EMS provider.
dim_date: A standard calendar hierarchy for time-series analysis.

## Implementation
Language: Python 3.12 (Pandas for transformation, SQLAlchemy for ORM/Bulk Loading).
Database: SQL Server (Target) / SQLite (Local testing).
Configuration: YAML-based parameterization for environment switching (Dev/Prod).



