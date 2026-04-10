
from __future__ import annotations

import pandas as pd


def extract(csv_path: str, source_system: str) -> pd.DataFrame:
    """
    Read raw CSV as strings to preserve source values for traceability.
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df.insert(0, "source_row_number", range(2, len(df) + 2))
    df["source_system"] = source_system
    # Required unicode source column from assessment prompt.
    df["unicode_notes"] = df.get("unicode_notes", "").replace("", "EMS Record")
    return df