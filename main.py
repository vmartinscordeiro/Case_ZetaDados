"""
ETL Pipeline — Orchestrator

Usage (CLI)
-----------
    python main.py --source Archives/ingest_date=2025-12-01 --date 2025-12-01
    python main.py --source Archives/ingest_date=2025-12-02 --date 2025-12-02

Usage (import)
--------------
    from main import run_pipeline
    run_pipeline(source_dir="Archives/ingest_date=2025-12-01", ingest_date="2025-12-01")

Pipeline flow
-------------
    Source CSVs
        ↓  BronzeIngestor  (raw ingest + metadata + tolerant schema validation)
    data/bronze/ingest_date=YYYY-MM-DD/
        ↓  SilverCleaner   (union, transforms, dedup, strict schema validation)
    data/silver/
        ↓  GoldBuilder     (joins, KPIs, fact/dim, strict schema validation)
    data/gold/
"""

import argparse
import io
import logging
import sys
from datetime import date
from pathlib import Path

from connectors.csv_connector import CSVConnector
from pipeline.bronze.ingestor import BronzeIngestor
from pipeline.silver.cleaner import SilverCleaner
from pipeline.gold.builder import GoldBuilder
from config.settings import TABLE_NAMES, GOLD_TABLES

# ── Logging setup (UTF-8 stream to avoid cp1252 issues on Windows) ────────────
_utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=_utf8_stdout,
)
logger = logging.getLogger(__name__)


# ── Layer runners ─────────────────────────────────────────────────────────────

def run_bronze(source_dir: Path, ingest_date: str) -> None:
    """Ingest all tables from *source_dir* into Bronze."""
    logger.info("=== BRONZE LAYER ===")
    for table in TABLE_NAMES:
        csv_path = source_dir / f"{table}.csv"
        if not csv_path.exists():
            logger.warning(f"[BRONZE] Source file not found — skipping: {csv_path}")
            continue
        connector = CSVConnector(csv_path)
        BronzeIngestor(connector, ingest_date=ingest_date).run(table)


def run_silver(ingest_date: str) -> None:
    """Clean and merge Bronze data into Silver."""
    logger.info("=== SILVER LAYER ===")
    cleaner = SilverCleaner(ingest_date=ingest_date)
    for table in TABLE_NAMES:
        cleaner.run(table)


def run_gold() -> None:
    """Build analytics tables from Silver data."""
    logger.info("=== GOLD LAYER ===")
    builder = GoldBuilder()
    for table in GOLD_TABLES:
        builder.run(table)


# ── Main entry point ──────────────────────────────────────────────────────────

def run_pipeline(source_dir: str | Path, ingest_date: str | None = None) -> None:
    """
    Execute the full Bronze → Silver → Gold pipeline.

    Parameters
    ----------
    source_dir:
        Folder that contains the source CSV files for one day
        (e.g. ``Archives/ingest_date=2025-12-01``).
    ingest_date:
        Partition date string ``YYYY-MM-DD``.  Defaults to today.
    """
    ingest_date = ingest_date or date.today().strftime("%Y-%m-%d")
    source_dir  = Path(source_dir)

    logger.info(f"Pipeline started  |  ingest_date={ingest_date}  |  source={source_dir}")

    run_bronze(source_dir, ingest_date)
    run_silver(ingest_date)
    run_gold()

    logger.info("Pipeline finished successfully.")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the full ETL pipeline (Bronze → Silver → Gold)."
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Path to the folder containing today's source CSV files.",
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Ingestion date.  Defaults to today.",
    )
    args = parser.parse_args()
    run_pipeline(source_dir=args.source, ingest_date=args.date)
