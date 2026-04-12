"""
main.py — EMS ETL Pipeline Entry Point
=======================================
Orchestrates the five-phase ETL pipeline:

  Phase 0 — Init DB    : init_db() creates schema + tables IF NOT EXISTS
  Phase A — Stage raw  : raw CSV chunk  →  stg_ems_raw   (Bronze audit layer)
  Phase B — Stage clean: transform output → stg_ems_clean (Silver DW source)
  Phase C — DQ checks  : SQL assertions via dq_checks.sql (Silver → Gold gate)
  Phase D — Load DW    : load_dims.sql (MERGE) → load_fact.sql (INSERT...SELECT)

The dim and fact loads are single set-based SQL statements executed once
after all chunks are staged — no per-chunk DW writes, no in-memory caches.
Target platform: SQL Server (T-SQL).

Usage
-----
  python main.py                                                   # all defaults from config.yaml
  python main.py --file "data/ems_runs_2024 (1).csv"              # override source file
  python main.py --env prod                                        # run against production DB
  python main.py --env test --load-mode incremental \
                 --from-date 2024-06-01                            # incremental load on test
  python main.py --file "data/ems_runs_2023.csv" --env prod       # prior-year load to prod

CLI arguments always override the corresponding config.yaml value.
All other settings (batch_size, log_dir, reject_dir) remain config-file only.

Logging strategy
----------------
  Two handlers are active for every run:
    1. StreamHandler (stdout)  — INFO and above; shows live progress.
    2. FileHandler             — DEBUG and above; written to logs/etl_<ts>.log.
       One file per run; the filename encodes the source year and start time.

  Every required metric is emitted as a structured log line:
    RUN START  batch_id=...  file=...  year=...
    STEP       extract | stage_raw | stage_clean | dq_checks | load_dims | load_fact
    Chunk      extracted=N  staged_raw=N  staged_clean=N  rejected=N
    RUN END    status=SUCCESS|FAILED  elapsed=Xs  extracted=N  loaded=N  rejected=N
    WARNING    N row(s) rejected → rejects/rejects_<year>_<ts>.csv

Reject quarantine
-----------------
  Rows failing validation are collected across all chunks and written
  once per run to:  rejects/rejects_<year>_YYYYMMDD_HHMMSS.csv
  Each row contains all transformed fields plus reject_reason.

Re-runnability
--------------
  - etl_batch_id (Unix epoch seconds) tags stg_ems_raw, stg_ems_clean, and
    fct_history_ems_incidents rows so any run can be identified and replayed.
  - Running the pipeline twice on the same file appends a second copy
    (new batch_id).  To reprocess cleanly, delete rows WHERE
    etl_batch_id = <previous_batch_id> from all three tables first.

Exit codes:
    0  → pipeline completed (check reject count in final log line)
    1  → unrecoverable exception raised
"""

import argparse
import datetime
import logging
import re
import sys
from pathlib import Path

import pandas as pd
import yaml

from etl.extract import extract
from etl.load import (
    get_engine,
    init_db,
    load_staging_raw,
    load_staging_clean,
    run_dq_checks,
    load_dims_sql,
    load_fact_sql,
)
from etl.transform import transform

ROOT = Path(__file__).resolve().parent


# ── Logging setup ─────────────────────────────────────────────────────────────

def _configure_logging(log_dir: Path, source_year: int | None, ts: str) -> Path:
    """
    Attach two handlers to the root logger:
      1. StreamHandler  — INFO and above → stdout (live progress)
      2. FileHandler    — DEBUG and above → logs/etl_<year>_<ts>.log

    Including the year in the filename lets operators archive logs by year
    without opening the file to see which dataset it covers.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    year_tag = str(source_year) if source_year else "unknown"
    log_file = log_dir / f"etl_{year_tag}_{ts}.log"

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    return log_file


logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_year(filename: str) -> int | None:
    """Extract the first 4-digit year found in a filename, or None."""
    m = re.search(r"(19|20)\d{2}", filename)
    return int(m.group()) if m else None


def _write_rejects(
    frames: list[pd.DataFrame],
    reject_dir: Path,
    source_year: int | None,
    ts: str,
) -> Path | None:
    """
    Concatenate all per-chunk reject frames and write one CSV per run.
    Each row contains all transformed fields plus 'reject_reason'.
    Returns the path written, or None if there were no rejects.
    """
    if not frames:
        return None

    reject_dir.mkdir(parents=True, exist_ok=True)
    year_tag = str(source_year) if source_year else "unknown"
    path = reject_dir / f"rejects_{year_tag}_{ts}.csv"

    pd.concat(frames, ignore_index=True).to_csv(path, index=False)
    return path


# ── ETL entry point ───────────────────────────────────────────────────────────

def run_etl() -> None:
    # ── CLI argument parsing ──────────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description="EMS ETL pipeline — loads any year's ems_runs CSV into the warehouse."
    )
    parser.add_argument(
        "--file",
        metavar="CSV_FILENAME",
        help="Source CSV filename (overrides file_path in config.yaml). "
             "Example: --file \"ems_runs_2024 (1).csv\"",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "test", "prod"],
        help="Target environment (overrides environment in config.yaml). "
             "Example: --env prod",
    )
    parser.add_argument(
        "--load-mode",
        choices=["full", "incremental"],
        dest="load_mode",
        help="Load mode (overrides load_mode in config.yaml). "
             "Example: --load-mode incremental",
    )
    parser.add_argument(
        "--from-date",
        metavar="YYYY-MM-DD",
        dest="from_date",
        help="Incremental lower-bound date (overrides incremental_from_date "
             "in config.yaml). Only used when load_mode=incremental. "
             "Example: --from-date 2024-06-01",
    )
    args = parser.parse_args()

    # ── Load config ───────────────────────────────────────────────────────────
    cfg = yaml.safe_load((ROOT / "config" / "config.yaml").read_text(encoding="utf-8"))

    # CLI arguments take precedence over config.yaml for all overridable settings.
    if args.env:
        cfg["environment"] = args.env
    if args.load_mode:
        cfg["load_mode"] = args.load_mode
    if args.from_date:
        cfg["incremental_from_date"] = args.from_date

    source_file = args.file or cfg.get("file_path", "")
    log_dir     = ROOT / cfg.get("log_dir", "logs")
    reject_dir  = ROOT / cfg.get("reject_dir", "rejects")
    load_mode   = cfg.get("load_mode", "full")

    # Incremental date window — only used when load_mode = incremental.
    # Rows where incident_dt < inc_from are staged but skipped for DW load.
    _inc_raw = cfg.get("incremental_from_date")
    inc_from = datetime.date.fromisoformat(_inc_raw) if _inc_raw else None

    source_year = _parse_year(source_file)
    run_start   = datetime.datetime.utcnow()
    ts          = run_start.strftime("%Y%m%d_%H%M%S")
    batch_id    = int(run_start.timestamp())

    env    = cfg.get("environment", "dev")   # resolved environment (CLI or config)
    schema = f"{env}_ems"                    # dev_ems / test_ems / prod_ems

    log_file = _configure_logging(log_dir, source_year, ts)

    logger.info("=" * 65)
    logger.info(
        "RUN START  batch_id=%d  file=%s  year=%s  env=%s  schema=%s  load_mode=%s",
        batch_id, source_file, source_year, env, schema, load_mode,
    )
    if load_mode == "incremental" and inc_from:
        logger.info("Incremental window: incident_dt >= %s", inc_from)
    logger.info("Log file : %s", log_file)
    logger.info("=" * 65)

    engine = get_engine(env=env)

    # ── Phase 0: Ensure schema and all tables exist ───────────────────────────
    logger.info("STEP  init_db  (schema=%s)", schema)
    init_db(engine, schema=schema)

    # ── Per-chunk accumulators ────────────────────────────────────────────────
    total_extracted  = 0
    total_staged_raw = 0
    total_staged_cln = 0
    total_rejected   = 0
    total_loaded     = 0
    reject_frames: list[pd.DataFrame] = []
    error_msg: str | None = None

    try:
        # ── Phase A + B: Chunk loop — stage raw and clean ─────────────────────
        logger.info("STEP  extract + stage")
        for chunk in extract(file_path=source_file):
            chunk_size = len(chunk)
            total_extracted += chunk_size

            # Transform: DQ rules + normalization → valid rows + rejects
            logger.debug("STEP  transform  (chunk %d rows)", chunk_size)
            valid, rejects = transform(chunk)
            total_rejected += len(rejects)
            if not rejects.empty:
                reject_frames.append(rejects)

            # Phase A — stage every raw row for auditability
            logger.debug("STEP  stage_raw")
            staged_raw = load_staging_raw(
                chunk, batch_id, engine,
                source_year=source_year,
                source_file=source_file,
                schema=schema,
            )
            total_staged_raw += staged_raw

            # Phase B — stage normalized valid rows as the DW source
            logger.debug("STEP  stage_clean")
            staged_cln = load_staging_clean(
                valid, batch_id, engine,
                source_year=source_year,
                source_file=source_file,
                schema=schema,
            )
            total_staged_cln += staged_cln

            logger.info(
                "Chunk | extracted=%5d  staged_raw=%5d  staged_clean=%5d  rejected=%d",
                chunk_size, staged_raw, staged_cln, len(rejects),
            )

        # ── Phase C: DQ checks against stg_ems_clean ─────────────────────────
        logger.info("STEP  dq_checks")
        dq_passed = run_dq_checks(batch_id, engine, schema=schema)
        if not dq_passed:
            raise RuntimeError(
                f"DQ checks FAILED for batch_id={batch_id}. "
                "DW load aborted to protect data integrity. "
                "Review the FAIL lines above, fix the root cause, "
                "then delete staged rows (WHERE etl_batch_id=<id>) and re-run."
            )

        # ── Phase D: SQL-based DW load (dims first, then fact) ────────────────
        effective_inc = inc_from if load_mode == "incremental" else None

        logger.info("STEP  load_dims")
        load_dims_sql(batch_id, engine, inc_from=effective_inc, schema=schema)

        logger.info("STEP  load_fact")
        total_loaded = load_fact_sql(batch_id, engine, inc_from=effective_inc, schema=schema)

    except Exception as exc:
        error_msg = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "ETL pipeline FAILED  batch_id=%d  error=%s", batch_id, error_msg
        )
        raise

    finally:
        run_end = datetime.datetime.utcnow()
        elapsed = (run_end - run_start).total_seconds()
        status  = "FAILED" if error_msg else "SUCCESS"

        rej_path = _write_rejects(reject_frames, reject_dir, source_year, ts)

        logger.info("=" * 65)
        logger.info(
            "RUN END  status=%-8s  elapsed=%6.1fs  "
            "extracted=%d  staged_raw=%d  staged_clean=%d  loaded=%d  rejected=%d",
            status, elapsed,
            total_extracted, total_staged_raw, total_staged_cln,
            total_loaded, total_rejected,
        )
        if rej_path:
            logger.warning(
                "%d row(s) rejected — see %s",
                total_rejected, rej_path,
            )
        if error_msg:
            logger.error("Failure detail: %s", error_msg)
        logger.info("=" * 65)


if __name__ == "__main__":
    run_etl()
