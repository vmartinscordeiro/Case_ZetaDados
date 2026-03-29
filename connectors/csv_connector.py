"""
CSV connector — reads one file, a directory of CSVs, or a glob pattern.

Usage examples
--------------
# Single file
connector = CSVConnector("data/orders.csv")

# All CSVs inside a folder
connector = CSVConnector("data/ingest_date=2025-12-01/")

df = connector.extract()
"""

from pathlib import Path
import pandas as pd

from connectors.base_connector import BaseConnector


class CSVConnector(BaseConnector):
    """Reads CSV files from the local filesystem.

    Parameters
    ----------
    path:
        Path to a single .csv file or a directory that contains .csv files.
    read_kwargs:
        Extra keyword arguments forwarded to pandas.read_csv()
        (e.g. sep=";", encoding="latin-1").
    """

    def __init__(self, path: str | Path, **read_kwargs):
        self.path = Path(path)
        self.read_kwargs = read_kwargs

    def extract(self, **kwargs) -> pd.DataFrame:
        """Return a DataFrame with all data read as raw strings."""
        if self.path.is_file():
            return self._read_single(self.path)

        if self.path.is_dir():
            return self._read_directory(self.path)

        raise FileNotFoundError(f"CSVConnector: path not found → {self.path}")

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _read_single(self, file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(file_path, dtype=str, keep_default_na=False, **self.read_kwargs)
        df["_source_file"] = file_path.name
        return df

    def _read_directory(self, dir_path: Path) -> pd.DataFrame:
        csv_files = sorted(dir_path.glob("*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"CSVConnector: no .csv files found in {dir_path}")

        frames = [self._read_single(f) for f in csv_files]
        return pd.concat(frames, ignore_index=True)
