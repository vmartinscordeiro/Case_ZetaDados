"""
Database connector — executes a SQL query via SQLAlchemy and returns a DataFrame.

Supports any database with a SQLAlchemy-compatible driver:
    PostgreSQL  → "postgresql+psycopg2://user:pass@host/db"
    MySQL       → "mysql+pymysql://user:pass@host/db"
    SQLite      → "sqlite:///path/to/file.db"
    SQL Server  → "mssql+pyodbc://user:pass@host/db?driver=ODBC+Driver+17+for+SQL+Server"

Usage example
-------------
connector = DBConnector(
    connection_string="postgresql+psycopg2://user:pass@localhost/ecommerce",
    query="SELECT * FROM orders WHERE order_date = :dt",
    params={"dt": "2025-12-01"},
)
df = connector.extract()
"""

from typing import Any
import pandas as pd

from connectors.base_connector import BaseConnector


class DBConnector(BaseConnector):
    """Reads data from a SQL database using SQLAlchemy.

    Parameters
    ----------
    connection_string:
        SQLAlchemy connection URI.
    query:
        SQL query to execute.  Supports named bind parameters (e.g. ``:param``).
    params:
        Dictionary of bind parameters matched to the query placeholders.
    """

    def __init__(
        self,
        connection_string: str,
        query: str,
        params: dict[str, Any] | None = None,
    ):
        self.connection_string = connection_string
        self.query = query
        self.params = params or {}

    def extract(self, **kwargs) -> pd.DataFrame:
        try:
            from sqlalchemy import create_engine, text
        except ImportError as exc:
            raise ImportError(
                "SQLAlchemy is required for DBConnector. "
                "Install it with: pip install sqlalchemy"
            ) from exc

        engine = create_engine(self.connection_string)
        with engine.connect() as conn:
            df = pd.read_sql(text(self.query), conn, params=self.params)

        # Coerce all columns to string to match Bronze contract
        return df.astype(str)
