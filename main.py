from extract import extract
from transform import transform
from load import load_staging, load_rejects
import datetime

def run_etl():
    start_time = datetime.datetime.now()

    try:
        df = extract()
        valid, rejects = transform(df)

        load_staging(valid)
        load_rejects(rejects)

    except Exception as e:
        print("ETL FAILED:", str(e))

    end_time = datetime.datetime.now()
    print(start_time, "→", end_time)

if __name__ == "__main__":
    run_etl()