import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column classification
# Column names are normalized to lowercase at the top of transform().
# ---------------------------------------------------------------------------

# Single-word code / flag columns → UPPER CASE
UPPER_CASE_COLS = {
    "incident_county",
    "injury_flg",
    "naloxone_given_flg",
    "medication_given_other_flg",
}

# Multi-word description / code-set columns → lower case
LOWER_CASE_COLS = {
    "chief_complaint_dispatch",
    "chief_complaint_anatomic_loc",
    "primary_symptom",
    "provider_impression_primary",
    "provider_type_structure",
    "provider_type_service",
    "provider_type_service_level",
    "disposition_ed",
    "disposition_hospital",
    "destination_type",
}

# Numeric measure columns — cast to nullable Int16
NUMERIC_COLS = [
    "provider_to_scene_mins",
    "provider_to_destination_mins",
]

# Date columns — parsed from string to Python date objects
DATE_COLS = [
    "incident_dt",
    "unit_notified_by_dispatch_dt",
    "unit_arrived_on_scene_dt",
    "unit_arrived_to_patient_dt",
    "unit_left_scene_dt",
    "patient_arrived_destination_dt",
]

# Fields required to be non-null for a row to pass validation
REQUIRED_COLS = ["incident_dt", "incident_county"]

# Columns that must carry a sentinel when blank (part of a dim composite key)
# Blank strings in these columns would cause UNIQUE INDEX duplicates in SQL Server.
SENTINEL_COLS = {
    "provider_type_service_level": "NOT RECORDED",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _apply_case(series: pd.Series, mode: str) -> pd.Series:
    """
    Vectorized case normalization — avoids row-by-row apply() on large frames.
      mode='upper' → UPPER CASE
      mode='lower' → lower case
      mode='auto'  → single-word token → UPPER, multi-word → lower
    Blank / whitespace-only strings become pd.NA.
    """
    s = series.str.strip()
    s = s.where(s != "", other=pd.NA)

    if mode == "upper":
        return s.str.upper()
    if mode == "lower":
        return s.str.lower()
    word_count = s.str.split().str.len()
    return pd.Series(
        np.where(word_count == 1, s.str.upper(), s.str.lower()),
        index=series.index,
        dtype="object",
    )


# ---------------------------------------------------------------------------
# Main transform
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean, validate, and standardize one CSV chunk.

    Steps
    -----
    1. Normalize column names to lowercase snake_case.
    2. Case-normalize string columns (vectorized).
    3. Apply sentinel values for dim composite-key columns.
    4. Parse and validate date columns.
    5. Cast and validate numeric measure columns.
    6. Reject rows missing required fields.

    Returns
    -------
    valid   : DataFrame — rows that passed all validation rules, ready for load
    rejects : DataFrame — rejected rows with a 'reject_reason' column
    """
    df = df.copy()  # never mutate the caller's dataframe
    reject_frames: list[pd.DataFrame] = []

    # ------------------------------------------------------------------
    # 1. Normalize column names → lowercase to match fact/dim DDL
    # ------------------------------------------------------------------
    df.columns = df.columns.str.strip().str.lower()

    # ------------------------------------------------------------------
    # 2. Case-normalize string columns (vectorized, no row-by-row apply)
    # ------------------------------------------------------------------
    for col in UPPER_CASE_COLS:
        if col in df.columns:
            df[col] = _apply_case(df[col], mode="upper")

    for col in LOWER_CASE_COLS:
        if col in df.columns:
            word_counts = df[col].str.strip().str.split().str.len()
            is_single = word_counts == 1
            if is_single.all():
                df[col] = _apply_case(df[col], mode="upper")
            elif (~is_single).all():
                df[col] = _apply_case(df[col], mode="lower")
            else:
                df[col] = _apply_case(df[col], mode="auto")

    # ------------------------------------------------------------------
    # 3. Sentinel substitution — prevent NULL in dim composite unique keys
    # ------------------------------------------------------------------
    for col, sentinel in SENTINEL_COLS.items():
        if col in df.columns:
            df[col] = df[col].fillna(sentinel).replace("", sentinel)

    # ------------------------------------------------------------------
    # 4. Parse date columns → Python date objects (NULL where unparseable)
    # ------------------------------------------------------------------
    for col in DATE_COLS:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            # Rows where source had a value but parsing failed → reject
            bad_mask = df[col].notna() & (df[col].astype(str).str.strip() != "") & parsed.isna()
            if bad_mask.any():
                bad = df[bad_mask].copy()
                bad["reject_reason"] = f"Unparseable date in column: {col}"
                reject_frames.append(bad)
                df = df[~bad_mask].copy()
                parsed = parsed[~bad_mask]
            df[col] = parsed.dt.date

    # ------------------------------------------------------------------
    # 5. Cast and validate numeric measures
    # ------------------------------------------------------------------
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int16")
            neg_mask = df[col].notna() & (df[col] < 0)
            if neg_mask.any():
                bad = df[neg_mask].copy()
                bad["reject_reason"] = f"Negative value in column: {col}"
                reject_frames.append(bad)
                df = df[~neg_mask].copy()

    # ------------------------------------------------------------------
    # 6. Required-field validation → reject null rows
    # ------------------------------------------------------------------
    for col in REQUIRED_COLS:
        if col in df.columns:
            null_mask = df[col].isna()
            if null_mask.any():
                bad = df[null_mask].copy()
                bad["reject_reason"] = f"Null in required column: {col}"
                reject_frames.append(bad)
                df = df[~null_mask].copy()

    # ------------------------------------------------------------------
    # 7. Assemble rejects output
    # ------------------------------------------------------------------
    rejects = (
        pd.concat(reject_frames, ignore_index=True)
        if reject_frames
        else pd.DataFrame(columns=[*df.columns, "reject_reason"])
    )

    logger.info("Transform: valid=%d | rejected=%d", len(df), len(rejects))
    return df, rejects
