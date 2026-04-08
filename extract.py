
import pandas as pd
import yaml

def extract():
    config = yaml.safe_load(open("config/config.yaml"))
    df = pd.read_csv(config["file_path"])

    df["unicode_notes"] = "EMS Record"
    return df