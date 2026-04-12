-- ============================================================
-- init_schema.sql
-- Creates the environment-specific schema if it does not exist.
-- Safe to run multiple times — idempotent.
--
-- Schema convention: {env}_ems  →  dev_ems | test_ems | prod_ems
-- {schema} is substituted at runtime by SQLLoader.from_file().
--
-- Executed automatically by the ETL pipeline at startup (init_db).
-- Can also be run manually in SSMS after replacing {schema}.
-- ============================================================

-- name: create_schema
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')
    EXEC('CREATE SCHEMA [{schema}]');
