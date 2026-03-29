"""
Bronze layer — raw ingestion.

Responsibilities
----------------
1. Receive a DataFrame from any BaseConnector (CSV, API, DB, …).
2. Attach metadata columns: _ingest_date, _source_file, _load_ts.
3. Validate the DataFrame against the Bronze schema (tolerant mode —
   violations are logged but data is NOT dropped at this layer).
4. Write the data as-is to:
       data/bronze/ingest_date=YYYY-MM-DD/<table_name>.csv

No business transformations happen here.
"""

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from pipeline.base import LayerProcessor
from connectors.base_connector import BaseConnector
from config.settings import BRONZE_DIR
from config.schemas import BRONZE_SCHEMAS

logger = logging.getLogger(__name__)


class BronzeIngestor(LayerProcessor):
    """Ingests data from a connector into the Bronze layer.

    Parameters
    ----------
    connector:
        Any class that implements ``BaseConnector.extract()``.
    ingest_date:
        Partition date (``YYYY-MM-DD``).  Defaults to today.
    """

    layer_name = "BRONZE"

    def __init__(self, connector: BaseConnector, ingest_date: str | None = None):
        self.connector = connector
        self.ingest_date = ingest_date or date.today().strftime("%Y-%m-%d")

    def run(self, table_name: str, **kwargs) -> pd.DataFrame:
        """
        Full Bronze ingestion for one table.

        Parameters
        ----------
        table_name:
            One of ``orders | order_items | shipments | customers | products``.
        **kwargs:
            Forwarded to ``connector.extract()``.

        Returns
        -------
        pd.DataFrame
            The raw DataFrame written to Bronze.
        """
        self._log(f"Ingesting '{table_name}' [{self.ingest_date}]")

        # 1. Extract from source
        df = self.connector.extract(**kwargs)
        self._log(f"'{table_name}': {len(df)} rows extracted from source")

        # 2. Add metadata (only if not already present — e.g. connector may add _source_file)
        df = self._add_metadata(df)

        # 3. Schema validation — tolerant: log issues but keep all data
        if table_name in BRONZE_SCHEMAS:
            df = self.validate(df, BRONZE_SCHEMAS[table_name], table_name, mode="tolerant")
        else:
            self._log(f"No Bronze schema defined for '{table_name}' — skipping validation", "warning")

        # 4. Write to partitioned path
        output_path = BRONZE_DIR / f"ingest_date={self.ingest_date}" / f"{table_name}.csv"
        self.write(df, output_path)

        self._log(f"'{table_name}' done → {len(df)} rows at {output_path}")
        return df

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _add_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "_ingest_date" not in df.columns:
            df["_ingest_date"] = self.ingest_date
        if "_source_file" not in df.columns:
            df["_source_file"] = "unknown"
        if "_load_ts" not in df.columns:
            df["_load_ts"] = datetime.now().isoformat(timespec="seconds")
        return df
