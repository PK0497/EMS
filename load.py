from sqlalchemy import create_engine

engine = create_engine("sqlite:///ems.db")


def load_staging(df):
    df.to_sql("stg_ems_raw", engine, if_exists="append", index=False)


def load_rejects(rejects):
    if rejects.empty:
        return

    rejects[["incident_id", "error_reason"]].to_sql(
        "etl_rejects", engine, if_exists="append", index=False
    )