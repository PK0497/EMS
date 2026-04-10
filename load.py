from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


@dataclass
class LoadStats:
    rows_staged_raw: int = 0
    rows_valid: int = 0
    rows_rejected: int = 0
    dim_inserted: int = 0
    dim_updated: int = 0
    fact_inserted: int = 0
    fact_updated: int = 0


def get_engine(database_url: str) -> Engine:
    return create_engine(database_url, future=True)


def bootstrap_schema(engine: Engine) -> None:
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS etl_run_log (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT NOT NULL,
            load_mode TEXT NOT NULL,
            source_system TEXT NOT NULL,
            rows_extracted INTEGER DEFAULT 0,
            rows_staged_raw INTEGER DEFAULT 0,
            rows_valid INTEGER DEFAULT 0,
            rows_rejected INTEGER DEFAULT 0,
            rows_dim_inserted INTEGER DEFAULT 0,
            rows_dim_updated INTEGER DEFAULT 0,
            rows_fact_inserted INTEGER DEFAULT 0,
            rows_fact_updated INTEGER DEFAULT 0,
            error_message TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS etl_step_log (
            step_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            step_name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            status TEXT NOT NULL,
            rows_affected INTEGER DEFAULT 0,
            error_message TEXT,
            FOREIGN KEY (run_id) REFERENCES etl_run_log(run_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS etl_rejects (
            reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            incident_id TEXT,
            source_row_number INTEGER,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES etl_run_log(run_id)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS ctl_watermark (
            entity_name TEXT PRIMARY KEY,
            watermark_value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS stg_ems_raw (
            run_id INTEGER NOT NULL,
            source_system TEXT NOT NULL,
            source_row_number INTEGER NOT NULL,
            incident_id_raw TEXT,
            patient_id_raw TEXT,
            age_raw TEXT,
            gender_raw TEXT,
            incident_date_raw TEXT,
            city_raw TEXT,
            agency_raw TEXT,
            response_time_raw TEXT,
            unicode_notes TEXT,
            loaded_at TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS stg_ems_valid (
            run_id INTEGER NOT NULL,
            source_row_number INTEGER NOT NULL,
            incident_id TEXT NOT NULL,
            patient_id TEXT NOT NULL,
            age INTEGER NOT NULL,
            gender TEXT NOT NULL,
            incident_date TEXT NOT NULL,
            date_key INTEGER NOT NULL,
            city TEXT NOT NULL,
            agency TEXT NOT NULL,
            response_time INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            day INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_patient (
            patient_key INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id TEXT NOT NULL,
            gender TEXT NOT NULL,
            age INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            is_current INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_location (
            location_key INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_agency (
            agency_key INTEGER PRIMARY KEY AUTOINCREMENT,
            agency_name TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_ems_incident (
            incident_key INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_id TEXT NOT NULL UNIQUE,
            date_key INTEGER NOT NULL,
            patient_key INTEGER NOT NULL,
            location_key INTEGER NOT NULL,
            agency_key INTEGER NOT NULL,
            response_time INTEGER NOT NULL,
            record_count INTEGER NOT NULL DEFAULT 1,
            load_run_id INTEGER NOT NULL,
            load_time TEXT NOT NULL,
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
            FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
            FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
            FOREIGN KEY (agency_key) REFERENCES dim_agency(agency_key),
            FOREIGN KEY (load_run_id) REFERENCES etl_run_log(run_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_stg_raw_run ON stg_ems_raw(run_id);",
        "CREATE INDEX IF NOT EXISTS idx_stg_valid_run ON stg_ems_valid(run_id);",
        "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_ems_incident(date_key);",
        "CREATE INDEX IF NOT EXISTS idx_fact_patient ON fact_ems_incident(patient_key);",
        "CREATE INDEX IF NOT EXISTS idx_dim_patient_lookup ON dim_patient(patient_id, is_current);",
    ]
    with engine.begin() as conn:
        for stmt in ddl:
            conn.execute(text(stmt))
        _seed_unknown_members(conn)


def _seed_unknown_members(conn: Any) -> None:
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_date(date_key, full_date, year, month, day)
            VALUES (0, '1900-01-01', 1900, 1, 1);
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_location(location_key, city)
            VALUES (0, 'UNKNOWN');
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_agency(agency_key, agency_name)
            VALUES (0, 'UNKNOWN');
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT OR IGNORE INTO dim_patient(
                patient_key, patient_id, gender, age, start_date, end_date, is_current
            )
            VALUES (0, 'UNKNOWN', 'UNKNOWN', 0, '1900-01-01', '9999-12-31', 1);
            """
        )
    )


def start_run(engine: Engine, load_mode: str, source_system: str) -> int:
    now = datetime.now(UTC).isoformat()
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO etl_run_log(start_time, status, load_mode, source_system)
                VALUES (:start_time, 'running', :load_mode, :source_system);
                """
            ),
            {"start_time": now, "load_mode": load_mode, "source_system": source_system},
        )
        return int(result.lastrowid)


def finish_run(
    engine: Engine,
    run_id: int,
    status: str,
    stats: LoadStats,
    rows_extracted: int,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC).isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE etl_run_log
                SET end_time = :end_time,
                    status = :status,
                    rows_extracted = :rows_extracted,
                    rows_staged_raw = :rows_staged_raw,
                    rows_valid = :rows_valid,
                    rows_rejected = :rows_rejected,
                    rows_dim_inserted = :rows_dim_inserted,
                    rows_dim_updated = :rows_dim_updated,
                    rows_fact_inserted = :rows_fact_inserted,
                    rows_fact_updated = :rows_fact_updated,
                    error_message = :error_message
                WHERE run_id = :run_id;
                """
            ),
            {
                "run_id": run_id,
                "end_time": now,
                "status": status,
                "rows_extracted": rows_extracted,
                "rows_staged_raw": stats.rows_staged_raw,
                "rows_valid": stats.rows_valid,
                "rows_rejected": stats.rows_rejected,
                "rows_dim_inserted": stats.dim_inserted,
                "rows_dim_updated": stats.dim_updated,
                "rows_fact_inserted": stats.fact_inserted,
                "rows_fact_updated": stats.fact_updated,
                "error_message": error_message,
            },
        )


def log_step(
    engine: Engine,
    run_id: int,
    step_name: str,
    status: str,
    rows_affected: int = 0,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC).isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO etl_step_log(
                    run_id, step_name, start_time, end_time, status, rows_affected, error_message
                )
                VALUES (:run_id, :step_name, :now, :now, :status, :rows_affected, :error_message);
                """
            ),
            {
                "run_id": run_id,
                "step_name": step_name,
                "now": now,
                "status": status,
                "rows_affected": rows_affected,
                "error_message": error_message,
            },
        )


def get_incremental_cutoff(engine: Engine) -> str | None:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT watermark_value
                FROM ctl_watermark
                WHERE entity_name = 'incident_date';
                """
            )
        ).fetchone()
        return str(row[0]) if row else None


def update_incremental_cutoff(engine: Engine, max_incident_date: str) -> None:
    now = datetime.now(UTC).isoformat()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO ctl_watermark(entity_name, watermark_value, updated_at)
                VALUES ('incident_date', :watermark_value, :updated_at)
                ON CONFLICT(entity_name)
                DO UPDATE SET
                    watermark_value = excluded.watermark_value,
                    updated_at = excluded.updated_at;
                """
            ),
            {"watermark_value": max_incident_date, "updated_at": now},
        )


def reset_for_full_load(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_ems_incident;"))
        conn.execute(text("DELETE FROM dim_patient WHERE patient_key <> 0;"))
        conn.execute(text("DELETE FROM dim_location WHERE location_key <> 0;"))
        conn.execute(text("DELETE FROM dim_agency WHERE agency_key <> 0;"))
        conn.execute(text("DELETE FROM dim_date WHERE date_key <> 0;"))
        conn.execute(text("DELETE FROM stg_ems_raw;"))
        conn.execute(text("DELETE FROM stg_ems_valid;"))


def stage_raw(engine: Engine, run_id: int, raw_df: pd.DataFrame, batch_size: int) -> int:
    if raw_df.empty:
        return 0
    payload = pd.DataFrame(
        {
            "run_id": run_id,
            "source_system": raw_df["source_system"],
            "source_row_number": raw_df["source_row_number"].astype(int),
            "incident_id_raw": raw_df["incident_id"],
            "patient_id_raw": raw_df["patient_id"],
            "age_raw": raw_df["age"],
            "gender_raw": raw_df["gender"],
            "incident_date_raw": raw_df["incident_date"],
            "city_raw": raw_df["city"],
            "agency_raw": raw_df["agency"],
            "response_time_raw": raw_df["response_time"],
            "unicode_notes": raw_df["unicode_notes"],
            "loaded_at": datetime.now(UTC).isoformat(),
        }
    )
    payload.to_sql("stg_ems_raw", engine, if_exists="append", index=False, chunksize=batch_size, method="multi")
    return int(len(payload))


def stage_valid(engine: Engine, run_id: int, valid_df: pd.DataFrame, batch_size: int) -> int:
    if valid_df.empty:
        return 0
    payload = valid_df.sort_values("source_row_number").drop_duplicates("incident_id", keep="last").copy()
    payload = payload[
        [
            "source_row_number",
            "incident_id",
            "patient_id",
            "age",
            "gender",
            "incident_date",
            "date_key",
            "city",
            "agency",
            "response_time",
        ]
    ]
    payload.insert(0, "run_id", run_id)
    payload.to_sql("stg_ems_valid", engine, if_exists="append", index=False, chunksize=batch_size, method="multi")
    return int(len(payload))


def load_rejects(engine: Engine, run_id: int, rejects_df: pd.DataFrame, batch_size: int) -> int:
    if rejects_df.empty:
        return 0
    payload = rejects_df.copy()
    payload.insert(0, "run_id", run_id)
    payload["created_at"] = datetime.now(UTC).isoformat()
    payload.to_sql("etl_rejects", engine, if_exists="append", index=False, chunksize=batch_size, method="multi")
    return int(len(payload))


def load_dimensions(engine: Engine, run_id: int, load_timestamp: str) -> tuple[int, int]:
    with engine.begin() as conn:
        before_dates = conn.execute(text("SELECT COUNT(*) FROM dim_date WHERE date_key <> 0;")).scalar_one()
        before_locations = conn.execute(text("SELECT COUNT(*) FROM dim_location WHERE location_key <> 0;")).scalar_one()
        before_agencies = conn.execute(text("SELECT COUNT(*) FROM dim_agency WHERE agency_key <> 0;")).scalar_one()
        before_patients = conn.execute(
            text("SELECT COUNT(*) FROM dim_patient WHERE patient_key <> 0 AND is_current = 1;")
        ).scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO dim_date(date_key, full_date, year, month, day)
                SELECT DISTINCT s.date_key, s.incident_date, CAST(substr(s.incident_date, 1, 4) AS INT),
                       CAST(substr(s.incident_date, 6, 2) AS INT), CAST(substr(s.incident_date, 9, 2) AS INT)
                FROM stg_ems_valid s
                LEFT JOIN dim_date d ON d.date_key = s.date_key
                WHERE s.run_id = :run_id AND d.date_key IS NULL;
                """
            ),
            {"run_id": run_id},
        )

        conn.execute(
            text(
                """
                INSERT INTO dim_location(city)
                SELECT DISTINCT s.city
                FROM stg_ems_valid s
                LEFT JOIN dim_location d ON d.city = s.city
                WHERE s.run_id = :run_id
                  AND d.location_key IS NULL;
                """
            ),
            {"run_id": run_id},
        )

        conn.execute(
            text(
                """
                INSERT INTO dim_agency(agency_name)
                SELECT DISTINCT s.agency
                FROM stg_ems_valid s
                LEFT JOIN dim_agency d ON d.agency_name = s.agency
                WHERE s.run_id = :run_id
                  AND d.agency_key IS NULL;
                """
            ),
            {"run_id": run_id},
        )

        # Close current SCD2 rows for patients with changed attributes.
        updated = conn.execute(
            text(
                """
                UPDATE dim_patient
                SET end_date = :load_timestamp,
                    is_current = 0
                WHERE patient_key <> 0
                  AND is_current = 1
                  AND EXISTS (
                      SELECT 1
                      FROM (
                          SELECT s1.patient_id, s1.gender, s1.age
                          FROM stg_ems_valid s1
                          JOIN (
                              SELECT patient_id, MAX(source_row_number) AS max_row
                              FROM stg_ems_valid
                              WHERE run_id = :run_id
                              GROUP BY patient_id
                          ) x
                          ON x.patient_id = s1.patient_id
                         AND x.max_row = s1.source_row_number
                         AND s1.run_id = :run_id
                      ) latest
                      WHERE latest.patient_id = dim_patient.patient_id
                        AND (
                            COALESCE(latest.gender, '') <> COALESCE(dim_patient.gender, '')
                            OR COALESCE(latest.age, -1) <> COALESCE(dim_patient.age, -1)
                        )
                  );
                """
            ),
            {"run_id": run_id, "load_timestamp": load_timestamp},
        ).rowcount

        # Insert new current rows for new or changed patients.
        conn.execute(
            text(
                """
                INSERT INTO dim_patient(patient_id, gender, age, start_date, end_date, is_current)
                SELECT latest.patient_id, latest.gender, latest.age, :load_timestamp, '9999-12-31', 1
                FROM (
                    SELECT s1.patient_id, s1.gender, s1.age
                    FROM stg_ems_valid s1
                    JOIN (
                        SELECT patient_id, MAX(source_row_number) AS max_row
                        FROM stg_ems_valid
                        WHERE run_id = :run_id
                        GROUP BY patient_id
                    ) x
                    ON x.patient_id = s1.patient_id
                   AND x.max_row = s1.source_row_number
                   AND s1.run_id = :run_id
                ) latest
                LEFT JOIN dim_patient curr
                  ON curr.patient_id = latest.patient_id
                 AND curr.is_current = 1
                 AND curr.patient_key <> 0
                WHERE curr.patient_key IS NULL
                   OR (
                        COALESCE(latest.gender, '') <> COALESCE(curr.gender, '')
                        OR COALESCE(latest.age, -1) <> COALESCE(curr.age, -1)
                   );
                """
            ),
            {"run_id": run_id, "load_timestamp": load_timestamp},
        )

        after_dates = conn.execute(text("SELECT COUNT(*) FROM dim_date WHERE date_key <> 0;")).scalar_one()
        after_locations = conn.execute(text("SELECT COUNT(*) FROM dim_location WHERE location_key <> 0;")).scalar_one()
        after_agencies = conn.execute(text("SELECT COUNT(*) FROM dim_agency WHERE agency_key <> 0;")).scalar_one()
        after_patients = conn.execute(
            text("SELECT COUNT(*) FROM dim_patient WHERE patient_key <> 0 AND is_current = 1;")
        ).scalar_one()

        inserted = int(
            (after_dates - before_dates)
            + (after_locations - before_locations)
            + (after_agencies - before_agencies)
            + (after_patients - before_patients)
        )
        return inserted, int(updated)


def load_facts(engine: Engine, run_id: int, load_timestamp: str) -> tuple[int, int]:
    with engine.begin() as conn:
        existing_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM stg_ems_valid s
                JOIN fact_ems_incident f ON f.incident_id = s.incident_id
                WHERE s.run_id = :run_id;
                """
            ),
            {"run_id": run_id},
        ).scalar_one()

        source_count = conn.execute(
            text("SELECT COUNT(*) FROM stg_ems_valid WHERE run_id = :run_id;"),
            {"run_id": run_id},
        ).scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO fact_ems_incident(
                    incident_id,
                    date_key,
                    patient_key,
                    location_key,
                    agency_key,
                    response_time,
                    record_count,
                    load_run_id,
                    load_time
                )
                SELECT
                    s.incident_id,
                    COALESCE(d.date_key, 0) AS date_key,
                    COALESCE(p.patient_key, 0) AS patient_key,
                    COALESCE(l.location_key, 0) AS location_key,
                    COALESCE(a.agency_key, 0) AS agency_key,
                    s.response_time,
                    1,
                    :run_id,
                    :load_time
                FROM stg_ems_valid s
                LEFT JOIN dim_date d ON d.date_key = s.date_key
                LEFT JOIN dim_patient p
                  ON p.patient_id = s.patient_id
                 AND p.is_current = 1
                LEFT JOIN dim_location l ON l.city = s.city
                LEFT JOIN dim_agency a ON a.agency_name = s.agency
                WHERE s.run_id = :run_id
                ON CONFLICT(incident_id) DO UPDATE SET
                    date_key = excluded.date_key,
                    patient_key = excluded.patient_key,
                    location_key = excluded.location_key,
                    agency_key = excluded.agency_key,
                    response_time = excluded.response_time,
                    load_run_id = excluded.load_run_id,
                    load_time = excluded.load_time;
                """
            ),
            {"run_id": run_id, "load_time": load_timestamp},
        )
        updated = int(existing_count)
        inserted = int(source_count - existing_count)
        return inserted, updated