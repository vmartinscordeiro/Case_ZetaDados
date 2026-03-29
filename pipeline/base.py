"""
Abstract base class for all pipeline layer processors.

Each layer (Bronze, Silver, Gold) inherits LayerProcessor and must implement
the run() method.  validate() and write() are provided as concrete helpers
so subclasses stay DRY.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd
import pandera as pa

from validators.schema_validator import SchemaValidator
from utils.io import write_csv

logger = logging.getLogger(__name__)


class LayerProcessor(ABC):
    """
    Contract shared by BronzeIngestor, SilverCleaner, and GoldBuilder.

    Subclasses must set the class attribute ``layer_name`` (used only for
    log messages) and implement ``run()``.
    """

    layer_name: str = ""

    @abstractmethod
    def run(self, table_name: str, **kwargs) -> pd.DataFrame:
        """Execute the full processing logic for *table_name*."""

    # ── Shared helpers ────────────────────────────────────────────────────────

    def validate(
        self,
        df: pd.DataFrame,
        schema: pa.DataFrameSchema,
        table_name: str,
        mode: str = "tolerant",
    ) -> pd.DataFrame:
        """Validate *df* against *schema*.

        Parameters
        ----------
        mode:
            ``"tolerant"`` (default for Bronze) — logs and drops bad rows.
            ``"strict"``   (Silver/Gold)         — raises on any violation.
        """
        validator = SchemaValidator(schema, mode=mode)
        return validator.validate(df, table_name=f"{self.layer_name}.{table_name}")

    def write(self, df: pd.DataFrame, path: Path) -> None:
        """Persist *df* to *path* as CSV."""
        write_csv(df, path)

    def _log(self, message: str, level: str = "info") -> None:
        getattr(logger, level)(f"[{self.layer_name}] {message}")
