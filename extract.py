import pandas as pd
import yaml

DEFAULT_2024_EMS_URL = (
    "https://hub.mph.in.gov/dataset/8404e4ae-c244-48c1-95ee-c2eff5e177e6/"
    "resource/249f8df8-7728-4cab-a167-7c93c5e43eee/download/ems_runs_2024.csv"
)


def _load_config():
    with open("config.yaml", "r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file) or {}


def _pick_column(df, candidates):
    for candidate in candidates:
        if candidate in df.columns:
            return df[candidate]
    return pd.Series([pd.NA] * len(df), index=df.index)


def _standardize_schema(raw_df):
    standardized = pd.DataFrame(index=raw_df.index)

    standardized["incident_id"] = _pick_column(raw_df, ["incident_id", "INCIDENT_ID"])
    if standardized["incident_id"].isna().all():
        standardized["incident_id"] = pd.RangeIndex(start=1, stop=len(raw_df) + 1)

    standardized["patient_id"] = _pick_column(raw_df, ["patient_id", "PATIENT_ID"]).fillna(
        "UNKNOWN"
    )
    standardized["age"] = pd.to_numeric(_pick_column(raw_df, ["age", "AGE"]), errors="coerce")
    standardized["gender"] = (
        _pick_column(raw_df, ["gender", "GENDER"]).fillna("UNKNOWN").astype(str).str.upper()
    )

    incident_dt = pd.to_datetime(
        _pick_column(raw_df, ["incident_date", "INCIDENT_DT"]), errors="coerce"
    )
    standardized["incident_date"] = incident_dt.dt.strftime("%Y-%m-%d")
    standardized["city"] = _pick_column(raw_df, ["city", "INCIDENT_COUNTY"]).fillna("UNKNOWN")
    standardized["agency"] = _pick_column(raw_df, ["agency", "PROVIDER_TYPE_SERVICE"]).fillna(
        "UNKNOWN"
    )
    standardized["response_time"] = pd.to_numeric(
        _pick_column(raw_df, ["response_time", "PROVIDER_TO_SCENE_MINS"]),
        errors="coerce",
    )
    standardized["unicode_notes"] = "EMS Record"

    return standardized


def extract():
    config = _load_config()
    source = config.get("file_path", DEFAULT_2024_EMS_URL)
    nrows = config.get("nrows")

    read_kwargs = {"low_memory": False}
    if nrows:
        read_kwargs["nrows"] = nrows

    raw_df = pd.read_csv(source, **read_kwargs)
    return _standardize_schema(raw_df)