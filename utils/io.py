"""
I/O helpers shared across all pipeline layers.

Centralises read/write logic so each layer processor only calls
these helpers and never touches pandas I/O directly.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Read a CSV from *path* and return a DataFrame."""
    if not path.exists():
        raise FileNotFoundError(f"read_csv: file not found → {path}")
    df = pd.read_csv(path, **kwargs)
    logger.debug(f"read_csv: {len(df)} rows ← {path}")
    return df


def read_csv_if_exists(path: Path, **kwargs) -> pd.DataFrame | None:
    """Return a DataFrame if the file exists, otherwise return None."""
    if path.exists():
        return read_csv(path, **kwargs)
    logger.debug(f"read_csv_if_exists: not found (skipped) → {path}")
    return None


def write_csv(df: pd.DataFrame, path: Path, **kwargs) -> None:
    """Write *df* to *path* as CSV, creating parent directories as needed.

    Null values are written as the literal string ``NULL`` so that downstream
    readers can distinguish intentionally-missing fields from empty strings.
    Callers may override via ``na_rep`` in *kwargs*.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    kwargs.setdefault("na_rep", "NULL")
    df.to_csv(path, index=False, **kwargs)
    logger.info(f"write_csv: {len(df)} rows -> {path}")
