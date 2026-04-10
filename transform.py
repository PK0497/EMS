import pandas as pd


def transform(df):
    df = df.copy()
    df["gender"] = df["gender"].fillna("UNKNOWN").astype(str).str.upper()
    df["response_time"] = pd.to_numeric(df["response_time"], errors="coerce")

    incident_dates = pd.to_datetime(df["incident_date"], errors="coerce")
    invalid_incident_id = df["incident_id"].isna()
    invalid_incident_date = incident_dates.isna()
    reject_mask = invalid_incident_id | invalid_incident_date

    rejects = df[reject_mask].copy()
    rejects["error_reason"] = "Invalid incident date"
    rejects.loc[invalid_incident_id[reject_mask], "error_reason"] = "Missing incident_id"

    valid = df[~reject_mask].copy()
    valid["incident_id"] = valid["incident_id"].astype(int)
    valid["incident_date"] = incident_dates[~reject_mask].dt.strftime("%Y-%m-%d")
    valid["date_key"] = incident_dates[~reject_mask].dt.strftime("%Y%m%d").astype(int)

    return valid, rejects