import logging
from pathlib import Path
from typing import Iterator

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent   # project root (one level above etl/)


def _load_config() -> dict:
    with open(ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


def extract(file_path: str | None = None) -> Iterator[pd.DataFrame]:
    """
    Yield batches of raw rows from the source CSV.

    Parameters
    ----------
    file_path : str | None
        CSV filename (relative to project root) to extract from.
        If None, falls back to file_path in config.yaml.
        This lets callers pass --file CLI arguments without rewriting config.

    Reads in chunks (controlled by config batch_size) so that
    1.6 M+ rows are never fully loaded into memory at once.
    Column names are NOT normalised here — that is transform()'s job.

    Yields
    ------
    pd.DataFrame — raw chunk with original UPPERCASE column names and string dtypes.
                   The DataFrame index is cumulative across chunks (pandas default),
                   so index+2 gives the 1-based CSV line number for reject tracking.
    """
    config     = _load_config()
    resolved   = file_path or config["file_path"]
    csv_path   = ROOT / resolved
    batch_size = int(config.get("batch_size", 10_000))

    logger.info("Extracting from %s  (batch_size=%d)", csv_path.name, batch_size)

    total_rows = 0
    chunk_num  = 0

    # dtype=str — no silent type coercion at ingest; typing is explicit in transform().
    # keep_default_na=False — prevents pandas turning "" / "NA" / "NULL" into NaN.
    with pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        chunksize=batch_size,
        encoding="utf-8",
    ) as reader:
        for chunk in reader:
            chunk_num  += 1
            total_rows += len(chunk)
            logger.debug(
                "Chunk %d — %d rows  (running total: %d)",
                chunk_num, len(chunk), total_rows,
            )
            yield chunk

    logger.info(
        "Extract complete — %d total rows in %d chunks", total_rows, chunk_num
    )
