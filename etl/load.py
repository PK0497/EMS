"""
load.py — EMS ETL Data Load Layer
==================================
Responsibility:
    0. Schema init   — init_db() creates schema + all tables IF NOT EXISTS
    A. Staging (raw)  — raw CSV chunk → stg_ems_raw  (Bronze audit layer)
    B. Staging (clean)— transform output → stg_ems_clean (Silver DW source)
    C. DQ checks      — SQL assertions against stg_ems_clean (Silver gate)
    D. Dimensions     — SQL MERGE from stg_ems_clean  (Gold dims)
    E. Fact           — SQL INSERT...SELECT from stg_ems_clean JOIN dims (Gold fact)

SQL separation
--------------
All DDL is stored in sql/ddl/ and executed by init_db() at startup.
All DML is stored in sql/dml/ and executed by the load functions.
SQLLoader reads each file, substitutes the {schema} placeholder, and
exposes named queries as attributes:

    File               Named queries
    ─────────────────  ─────────────────────────────────────────────────
    dq_checks.sql      row_count, null_incident_dt, null_incident_cnty,
                       null_complaint_dispatch, neg_scene_mins,
                       neg_dest_mins, future_incident_dt, invalid_injury_flg
    load_dims.sql      merge_geography, merge_complaint, merge_providers
    load_fact.sql      insert_fact

SQL files use '-- name: <query_name>' section headers (aiosql convention).
Runtime values are passed as SQLAlchemy named parameters (:bid, :inc_from),
which SQL Server processes as bound parameters — enabling query plan caching
and preventing SQL injection.

The schema name is derived in main.py as f"{env}_ems" and threaded through
every load function so that all objects target the correct schema at runtime.

Target platform: SQL Server (T-SQL).
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent   # project root (one level above etl/)


# ============================================================
# SQLLoader — named query access from .sql files
# ============================================================

class SQLLoader:
    """
    Loads a .sql file and exposes each named query as an attribute.

    SQL files use '-- name: <query_name>' section headers to delimit
    individual queries. Each attribute returns the SQL string for that
    query, ready to be passed to SQLAlchemy's text():

        loader = SQLLoader.from_file("sql/dml/load_dims.sql", schema="dev_ems")
        conn.execute(text(loader.merge_geography), params)

    Raises AttributeError if an unknown query name is accessed.
    """

    def __init__(self, queries: dict[str, str]) -> None:
        self._q = queries

    def __getattr__(self, name: str) -> str:
        try:
            return self._q[name]
        except KeyError:
            raise AttributeError(
                f"No SQL query named '{name}' in this loader. "
                f"Available: {sorted(self._q)}"
            )

    def statements(self) -> list[str]:
        """Return all SQL strings in definition order (used by init_db)."""
        return list(self._q.values())

    @classmethod
    def from_file(cls, filename: str, schema: str) -> "SQLLoader":
        """
        Read a .sql file from the project root, substitute the {schema}
        placeholder with the resolved schema name, and parse named sections.
        """
        sql = (ROOT / filename).read_text(encoding="utf-8")
        sql = sql.replace("{schema}", schema)
        return cls(_parse_named_queries(sql))


def _parse_named_queries(sql: str) -> dict[str, str]:
    """
    Parse a SQL file that uses '-- name: <query_name>' section headers
    into a {name: sql_string} mapping.

    Each '-- name:' line introduces a new named block; the block ends
    when the next '-- name:' header (or EOF) is reached. Blank lines
    and other comment lines within a block are preserved.

    Example input:
        -- name: merge_geography
        MERGE dim_geography AS tgt ...

        -- name: merge_complaint
        MERGE dim_dispatch_complaint AS tgt ...

    Returns:
        {"merge_geography": "MERGE dim_geography ...",
         "merge_complaint": "MERGE dim_dispatch_complaint ..."}
    """
    sections: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []

    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("-- name:"):
            if current_name is not None:
                sections[current_name] = "\n".join(current_lines).strip()
            current_name = stripped[len("-- name:"):].strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_name is not None:
        sections[current_name] = "\n".join(current_lines).strip()

    return sections


# ============================================================
# Engine factory
# ============================================================

def get_engine(env: str | None = None) -> Engine:
    """
    Create a SQLAlchemy engine from config.yaml.

    env : override the active environment (dev / test / prod).
          When None, falls back to the 'environment' key in config.yaml.
          Passing the value resolved by main.py ensures CLI --env overrides
          take effect without re-reading the config file here.

    fast_executemany=True maximises bulk-insert throughput on SQL Server.
    """
    with open(ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    resolved_env = env or cfg.get("environment", "dev")
    url = cfg["db"][resolved_env]
    return create_engine(url, fast_executemany=True)


# ============================================================
# Schema + table initialisation (runs at pipeline startup)
# ============================================================

# DDL files executed in dependency order.
_DDL_FILES = [
    "sql/ddl/init_schema.sql",   # CREATE SCHEMA  (must be first)
    "sql/ddl/staging.sql",        # Bronze + Silver staging tables
    "sql/ddl/dimensions.sql",     # Gold dimension tables
    "sql/ddl/facts.sql",          # Gold fact table
]


def init_db(engine: Engine, schema: str) -> None:
    """
    Create the schema and all tables if they do not already exist.

    Every DDL block is wrapped in IF NOT EXISTS — safe to call on every
    pipeline run without dropping or altering existing data.

    Execution order (dependency-safe):
        1. init_schema.sql  → CREATE SCHEMA {schema}
        2. staging.sql      → stg_ems_raw (Bronze), stg_ems_clean (Silver)
        3. dimensions.sql   → dim_calendar, dim_geography,
                               dim_dispatch_complaint, dim_ems_providers
        4. facts.sql        → fct_history_ems_incidents (Gold fact)
    """
    for filename in _DDL_FILES:
        loader = SQLLoader.from_file(filename, schema=schema)
        with engine.begin() as conn:
            for sql in loader.statements():
                if sql.strip():
                    conn.execute(text(sql))
    logger.info("Database initialised  schema=%s", schema)


# ============================================================
# A. Raw staging load
# ============================================================

def load_staging_raw(
    raw_chunk: pd.DataFrame,
    batch_id: int,
    engine: Engine,
    source_year: int | None = None,
    source_file: str = "",
    schema: str = "dev_ems",
) -> int:
    """
    Append the raw CSV chunk to {schema}.stg_ems_raw (audit layer).
    Column names are lowercased to match the DDL convention even though
    the CSV header arrives in UPPERCASE.
    Returns the number of rows staged.
    """
    df = raw_chunk.copy()
    df.columns = df.columns.str.lower()   # UPPERCASE CSV headers → lowercase DDL columns
    df["source_year"]   = source_year
    df["source_file"]   = source_file
    df["etl_batch_id"]  = batch_id
    df["etl_loaded_at"] = datetime.now(timezone.utc).isoformat()

    df.to_sql(
        "stg_ems_raw",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=5_000,
        method="multi",
    )
    logger.debug(
        "Staged %d raw rows → %s.stg_ems_raw  batch_id=%d  source_year=%s",
        len(df), schema, batch_id, source_year,
    )
    return len(df)


# ============================================================
# B. Clean staging load
# ============================================================

def load_staging_clean(
    valid_df: pd.DataFrame,
    batch_id: int,
    engine: Engine,
    source_year: int | None = None,
    source_file: str = "",
    schema: str = "dev_ems",
) -> int:
    """
    Append the normalized transform output (lowercase column names,
    proper DATE/numeric types) to {schema}.stg_ems_clean.
    This table is the sole source for all downstream SQL DW loads.
    Returns the number of rows staged.
    """
    df = valid_df.copy()
    df["source_year"]   = source_year
    df["source_file"]   = source_file
    df["etl_batch_id"]  = batch_id
    df["etl_loaded_at"] = datetime.now(timezone.utc).isoformat()

    df.to_sql(
        "stg_ems_clean",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
        chunksize=5_000,
        method="multi",
    )
    logger.debug(
        "Staged %d clean rows → %s.stg_ems_clean  batch_id=%d  source_year=%s",
        len(df), schema, batch_id, source_year,
    )
    return len(df)


# ============================================================
# C. Post-staging DQ checks (SQL)
# ============================================================

# Maps each named check to its pass criterion.
# True  → the returned count must equal zero.
# False → the returned count must be greater than zero (row_count).
_DQ_EXPECT_ZERO: dict[str, bool] = {
    # Completeness
    "row_count":              False,
    "null_incident_dt":       True,
    "null_incident_cnty":     True,
    "null_complaint_dispatch": True,
    # Numeric measures
    "neg_scene_mins":         True,
    "neg_dest_mins":          True,
    # Temporal validity
    "future_incident_dt":     True,
    # Value-set conformance
    "invalid_injury_flg":     True,
}

_DQ_LABELS: dict[str, str] = {
    # Completeness
    "row_count":              "stg_ems_clean row count",
    "null_incident_dt":       "NULL incident_dt",
    "null_incident_cnty":     "NULL incident_county",
    "null_complaint_dispatch": "NULL chief_complaint_dispatch",
    # Numeric measures
    "neg_scene_mins":         "negative provider_to_scene_mins",
    "neg_dest_mins":          "negative provider_to_destination_mins",
    # Temporal validity
    "future_incident_dt":     "future incident_dt",
    # Value-set conformance
    "invalid_injury_flg":     "invalid injury_flg value (not YES/NO/NULL)",
}


def run_dq_checks(batch_id: int, engine: Engine, schema: str = "dev_ems") -> bool:
    """
    Execute every named query from dq_checks.sql against {schema}.stg_ems_clean
    for the current batch. Logs PASS / FAIL per check.
    Returns False if any check fails; True if all pass.

    These checks are a post-staging safety net: the Python transform
    already rejects bad rows, so failures here indicate an unexpected
    code-path or schema mismatch.
    """
    dq     = SQLLoader.from_file("sql/dml/dq_checks.sql", schema=schema)
    params = {"bid": batch_id}
    passed = True

    with engine.connect() as conn:
        for name, expect_zero in _DQ_EXPECT_ZERO.items():
            sql   = getattr(dq, name)
            count = conn.execute(text(sql), params).scalar()
            label = _DQ_LABELS.get(name, name)

            if expect_zero and count != 0:
                logger.warning(
                    "DQ FAIL: %s = %d  batch_id=%d", label, count, batch_id
                )
                passed = False
            elif not expect_zero and count == 0:
                logger.warning(
                    "DQ FAIL: %s = 0 (expected > 0)  batch_id=%d", label, batch_id
                )
                passed = False
            else:
                logger.info(
                    "DQ PASS: %s = %d  batch_id=%d", label, count, batch_id
                )

    return passed


# ============================================================
# D. Dimension loads — SQL MERGE (load_dims.sql)
# ============================================================

def load_dims_sql(
    batch_id: int,
    engine: Engine,
    inc_from: "datetime.date | None" = None,
    schema: str = "dev_ems",
) -> None:
    """
    Execute each named MERGE statement from load_dims.sql in order
    against {schema}.dim_* tables.
    inc_from is passed as :inc_from; when None the SQL predicate
    (:inc_from IS NULL OR ...) includes all rows for the batch.
    """
    dims   = SQLLoader.from_file("sql/dml/load_dims.sql", schema=schema)
    params = {
        "bid":      batch_id,
        "inc_from": inc_from.isoformat() if inc_from else None,
    }
    with engine.begin() as conn:
        conn.execute(text(dims.merge_geography), params)
        conn.execute(text(dims.merge_complaint),  params)
        conn.execute(text(dims.merge_providers),  params)
    logger.info("Dimensions loaded from stg_ems_clean  batch_id=%d", batch_id)


# ============================================================
# E. Fact load — SQL INSERT...SELECT (load_fact.sql)
# ============================================================

def load_fact_sql(
    batch_id: int,
    engine: Engine,
    inc_from: "datetime.date | None" = None,
    schema: str = "dev_ems",
) -> int:
    """
    Execute the insert_fact query from load_fact.sql against
    {schema}.fct_history_ems_incidents.
    Single INSERT...SELECT with LEFT JOINs to dims; unresolved surrogate
    keys are left as NULL (no unknown-member sentinel).
    Returns the number of fact rows inserted (via result.rowcount).
    """
    fact   = SQLLoader.from_file("sql/dml/load_fact.sql", schema=schema)
    params = {
        "bid":      batch_id,
        "inc_from": inc_from.isoformat() if inc_from else None,
    }
    with engine.begin() as conn:
        result = conn.execute(text(fact.insert_fact), params)
        count = result.rowcount

    logger.info("Fact: inserted %d rows  batch_id=%d", count, batch_id)
    return count
