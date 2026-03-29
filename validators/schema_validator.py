"""
Pandera schema validation wrapper.

Two modes
---------
strict    → raises SchemaErrors on any violation (used in Silver and Gold).
tolerant  → logs violations and drops only the offending rows (used in Bronze,
            where we prefer not to discard data arriving from external sources).
"""

import logging

import pandas as pd
import pandera as pa

logger = logging.getLogger(__name__)


class SchemaValidator:
    """Validates a DataFrame against a Pandera schema.

    Parameters
    ----------
    schema:
        The Pandera ``DataFrameSchema`` to validate against.
    mode:
        ``"strict"``   — raise ``pa.errors.SchemaErrors`` on failure.
        ``"tolerant"`` — log violations and return DataFrame with bad rows removed.
    """

    def __init__(self, schema: pa.DataFrameSchema, mode: str = "strict"):
        if mode not in ("strict", "tolerant"):
            raise ValueError(f"mode must be 'strict' or 'tolerant', got '{mode}'")
        self.schema = schema
        self.mode = mode

    def validate(self, df: pd.DataFrame, table_name: str = "") -> pd.DataFrame:
        """Run validation.  Returns the (possibly filtered) DataFrame."""
        label = f"[{self.schema.name or table_name}]"
        try:
            validated = self.schema.validate(df, lazy=True)
            logger.debug(f"{label} Schema OK — {len(validated)} rows")
            return validated

        except pa.errors.SchemaErrors as exc:
            failure_cases = exc.failure_cases
            n_failures = len(failure_cases)

            logger.warning(
                f"{label} {n_failures} schema violation(s) detected:\n"
                f"{failure_cases[['schema_context', 'column', 'check', 'check_number', 'failure_case', 'index']].to_string(index=False)}"
            )

            if self.mode == "strict":
                raise

            # Tolerant: drop rows that triggered an error
            bad_index = (
                failure_cases["index"]
                .dropna()
                .astype(int)
                .unique()
            )
            clean_df = df.drop(index=bad_index, errors="ignore").reset_index(drop=True)
            logger.warning(
                f"{label} Dropped {len(bad_index)} row(s). "
                f"Remaining: {len(clean_df)} rows."
            )
            return clean_df
