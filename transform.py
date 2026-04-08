import pandas as pd

def transform(df):
    df["age"] = pd.to_numeric(df["age"], errors="coerce")
    df["gender"] = df["gender"].fillna("UNKNOWN").str.upper()

    rejects = df[df["age"].isna()]
    valid = df[df["age"].notna()]

    valid["date_key"] = valid["incident_date"].str.replace("-", "").astype(int)

    return valid, rejects