from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class ValidationIssue:
    incident_id: str
    source_row_number: int
    error_type: str
    error_message: str


def _is_blank(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().eq("")


def _to_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _build_reject_frame(
    issues: Iterable[ValidationIssue],
) -> pd.DataFrame:
    rows = [
        {
            "incident_id": i.incident_id,
            "source_row_number": i.source_row_number,
            "error_type": i.error_type,
            "error_message": i.error_message,
        }
        for i in issues
    ]
    return pd.DataFrame(rows, columns=["incident_id", "source_row_number", "error_type", "error_message"])


def transform(df: pd.DataFrame, allowed_gender: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleanse and validate source data, returning (valid_rows, rejects).
    """
    working = df.copy()
    working["incident_id"] = working["incident_id"].astype(str).str.strip()
    working["patient_id"] = working["patient_id"].astype(str).str.strip()
    working["gender"] = working["gender"].astype(str).str.strip().str.upper().replace("", "UNKNOWN")
    working["city"] = working["city"].astype(str).str.strip().replace("", "UNKNOWN")
    working["agency"] = working["agency"].astype(str).str.strip().replace("", "UNKNOWN")

    working["age_num"] = _to_int(working["age"])
    working["response_time_num"] = _to_int(working["response_time"])
    working["incident_dt"] = pd.to_datetime(working["incident_date"], errors="coerce")
    working["date_key"] = pd.to_numeric(working["incident_dt"].dt.strftime("%Y%m%d"), errors="coerce").astype("Int64")

    issues: list[ValidationIssue] = []
    issue_rows: set[int] = set()

    required_checks = {
        "incident_id": _is_blank(working["incident_id"]),
        "patient_id": _is_blank(working["patient_id"]),
        "incident_date": working["incident_dt"].isna(),
        "response_time": _is_blank(working["response_time"]),
        "age": _is_blank(working["age"]),
    }
    for col, mask in required_checks.items():
        for _, r in working[mask].iterrows():
            issues.append(
                ValidationIssue(
                    incident_id=r["incident_id"],
                    source_row_number=int(r["source_row_number"]),
                    error_type="required_field_missing_or_invalid",
                    error_message=f"{col} is missing or invalid",
                )
            )
            issue_rows.add(int(r.name))

    age_bad = working["age_num"].isna() | (working["age_num"] < 0) | (working["age_num"] > 120)
    for _, r in working[age_bad].iterrows():
        issues.append(
            ValidationIssue(
                incident_id=r["incident_id"],
                source_row_number=int(r["source_row_number"]),
                error_type="domain_validation",
                error_message="age must be an integer between 0 and 120",
            )
        )
        issue_rows.add(int(r.name))

    rt_bad = working["response_time_num"].isna() | (working["response_time_num"] < 0)
    for _, r in working[rt_bad].iterrows():
        issues.append(
            ValidationIssue(
                incident_id=r["incident_id"],
                source_row_number=int(r["source_row_number"]),
                error_type="domain_validation",
                error_message="response_time must be a non-negative integer",
            )
        )
        issue_rows.add(int(r.name))

    valid_gender = set(g.upper() for g in allowed_gender)
    gender_bad = ~working["gender"].isin(valid_gender)
    for _, r in working[gender_bad].iterrows():
        issues.append(
            ValidationIssue(
                incident_id=r["incident_id"],
                source_row_number=int(r["source_row_number"]),
                error_type="domain_validation",
                error_message=f"gender must be one of {sorted(valid_gender)}",
            )
        )
        issue_rows.add(int(r.name))

    rejects = _build_reject_frame(issues)
    valid = working.loc[~working.index.isin(issue_rows)].copy()

    # Keep raw values for lineage while adding standardized columns.
    valid["age"] = valid["age_num"].astype(int)
    valid["response_time"] = valid["response_time_num"].astype(int)
    valid["incident_date"] = valid["incident_dt"].dt.date.astype(str)
    valid["year"] = valid["incident_dt"].dt.year.astype(int)
    valid["month"] = valid["incident_dt"].dt.month.astype(int)
    valid["day"] = valid["incident_dt"].dt.day.astype(int)

    return valid, rejects