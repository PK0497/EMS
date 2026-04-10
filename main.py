from __future__ import annotations

import traceback
from datetime import UTC, datetime

import yaml

from extract import extract
from load import (
    LoadStats,
    bootstrap_schema,
    finish_run,
    get_engine,
    get_incremental_cutoff,
    load_dimensions,
    load_facts,
    load_rejects,
    log_step,
    reset_for_full_load,
    stage_raw,
    stage_valid,
    start_run,
    update_incremental_cutoff,
)
from transform import transform


def _read_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_etl(config_path: str = "config.yaml") -> int:
    cfg = _read_config(config_path)
    db_url = cfg["database"]["url"]
    csv_path = cfg["paths"]["input_csv"]
    batch_size = int(cfg["etl"]["batch_size"])
    load_mode = str(cfg["etl"]["load_mode"]).lower()
    source_system = cfg["etl"]["source_system"]
    allowed_gender = cfg["validation"]["allowed_gender"]

    engine = get_engine(db_url)
    bootstrap_schema(engine)
    run_id = start_run(engine, load_mode=load_mode, source_system=source_system)
    stats = LoadStats()

    rows_extracted = 0
    try:
        if load_mode not in {"full", "incremental"}:
            raise ValueError("etl.load_mode must be either 'full' or 'incremental'")

        if load_mode == "full":
            reset_for_full_load(engine)
            log_step(engine, run_id, "reset_for_full_load", "success", 1)

        raw_df = extract(csv_path=csv_path, source_system=source_system)
        rows_extracted = len(raw_df)
        log_step(engine, run_id, "extract", "success", rows_extracted)

        if load_mode == "incremental":
            cutoff = get_incremental_cutoff(engine)
            if cutoff:
                raw_df = raw_df[raw_df["incident_date"] > cutoff].copy()
            log_step(engine, run_id, "incremental_filter", "success", len(raw_df))

        stats.rows_staged_raw = stage_raw(engine, run_id=run_id, raw_df=raw_df, batch_size=batch_size)
        log_step(engine, run_id, "stage_raw", "success", stats.rows_staged_raw)

        valid_df, rejects_df = transform(raw_df, allowed_gender=allowed_gender)
        stats.rows_valid = stage_valid(engine, run_id=run_id, valid_df=valid_df, batch_size=batch_size)
        log_step(engine, run_id, "stage_valid", "success", stats.rows_valid)

        stats.rows_rejected = load_rejects(engine, run_id=run_id, rejects_df=rejects_df, batch_size=batch_size)
        log_step(engine, run_id, "load_rejects", "success", stats.rows_rejected)

        load_ts = datetime.now(UTC).isoformat()
        stats.dim_inserted, stats.dim_updated = load_dimensions(engine, run_id=run_id, load_timestamp=load_ts)
        log_step(
            engine,
            run_id,
            "load_dimensions",
            "success",
            stats.dim_inserted + stats.dim_updated,
        )

        stats.fact_inserted, stats.fact_updated = load_facts(engine, run_id=run_id, load_timestamp=load_ts)
        log_step(engine, run_id, "load_facts", "success", stats.fact_inserted + stats.fact_updated)

        if not valid_df.empty:
            update_incremental_cutoff(engine, str(valid_df["incident_date"].max()))
            log_step(engine, run_id, "update_watermark", "success", 1)

        finish_run(
            engine,
            run_id=run_id,
            status="success",
            stats=stats,
            rows_extracted=rows_extracted,
            error_message=None,
        )
        print(f"ETL run {run_id} completed successfully.")
        return 0
    except Exception as exc:  # noqa: BLE001
        message = f"{exc}\n{traceback.format_exc()}"
        log_step(engine, run_id, "pipeline_failed", "failure", 0, message)
        finish_run(
            engine,
            run_id=run_id,
            status="failure",
            stats=stats,
            rows_extracted=rows_extracted,
            error_message=message,
        )
        print(f"ETL run {run_id} failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(run_etl())