"""
Abstract base class for all data source connectors.

Any new source (CSV, API, database, S3, etc.) must implement BaseConnector
and provide an extract() method that returns a pandas DataFrame.
The BronzeIngestor only depends on this interface — it never imports a
concrete connector directly, keeping coupling minimal.
"""

from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """Contract for all data source connectors."""

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from the source and return it as a raw DataFrame.

        All columns should be returned as strings (dtype=str) so that
        Bronze receives data exactly as it arrived from the source.
        Type coercion happens in the Silver layer.
        """
