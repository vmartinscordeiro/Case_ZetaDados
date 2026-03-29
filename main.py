"""
ETL Pipeline — Orchestrator
============================

Usage (no arguments — auto-discovery)
--------------------------------------
    python main.py

    Scans ``Archives/`` for all ``ingest_date=YYYY-MM-DD`` sub-folders,
    sorts them ascending (oldest first), then runs the full pipeline for
    each date in order.

Usage (manual override)
-----------------------
    python main.py --source Archives/ingest_date=2025-12-01 --date 2025-12-01

Pipeline flow (per date)
------------------------
    Archives/ingest_date=YYYY-MM-DD/
        ↓  BronzeIngestor  (raw ingest + metadata + tolerant schema validation)
    data/bronze/ingest_date=YYYY-MM-DD/
        ↓  SilverCleaner   (union, transforms, dedup, strict schema validation)
    data/silver/
        ↓  (after all dates) GoldBuilder  (joins, KPIs, fact/dim tables)
    data/gold/
"""

import argparse
import io
import logging
import re
import sys
from datetime import date
from pathlib import Path

from connectors.csv_connector import CSVConnector
from pipeline.bronze.ingestor import BronzeIngestor
from pipeline.silver.cleaner import SilverCleaner
from pipeline.gold.builder import GoldBuilder
from config.settings import TABLE_NAMES, GOLD_TABLES, ARCHIVES_DIR

# ── Logging setup (UTF-8 stream to avoid cp1252 issues on Windows) ────────────
_utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=_utf8_stdout,
)
logger = logging.getLogger(__name__)

_DATE_FOLDER_RE = re.compile(r"^ingest_date=(\d{4}-\d{2}-\d{2})$")


# ── Discovery ─────────────────────────────────────────────────────────────────

def discover_dates(archives_dir: Path) -> list[tuple[str, Path]]:
    """Return ``(date_str, folder_path)`` pairs found in *archives_dir*,
    sorted ascending (oldest date first).

    Only sub-folders matching the pattern ``ingest_date=YYYY-MM-DD`` are
    considered.
    """
    found: list[tuple[str, Path]] = []
    for entry in archives_dir.iterdir():
        if not entry.is_dir():
            continue
        m = _DATE_FOLDER_RE.match(entry.name)
        if m:
            found.append((m.group(1), entry))
    found.sort(key=lambda t: t[0])   # lexicographic sort works for ISO dates
    return found


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


# ── Pipeline entry points ─────────────────────────────────────────────────────

def run_pipeline(source_dir: str | Path, ingest_date: str | None = None) -> None:
    """Execute Bronze + Silver for one date partition, then rebuild Gold.

    Parameters
    ----------
    source_dir:
        Folder containing source CSVs for this day
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


def run_pipeline_auto(archives_dir: Path = ARCHIVES_DIR) -> None:
    """Discover all date partitions in *archives_dir* and process them in
    chronological order (oldest → newest).

    Bronze + Silver are executed for each date; Gold is rebuilt once at the
    end so the final analytics tables reflect the full, merged Silver data.
    """
    partitions = discover_dates(archives_dir)
    if not partitions:
        logger.error(
            f"No 'ingest_date=YYYY-MM-DD' folders found in {archives_dir}. "
            "Nothing to process."
        )
        return

    logger.info(
        f"Auto-discovered {len(partitions)} partition(s) in {archives_dir}: "
        + ", ".join(d for d, _ in partitions)
    )

    for ingest_date, source_dir in partitions:
        logger.info(f"--- Processing partition {ingest_date} ---")
        logger.info(f"Pipeline started  |  ingest_date={ingest_date}  |  source={source_dir}")
        run_bronze(source_dir, ingest_date)
        run_silver(ingest_date)

    # Rebuild Gold once after all Silver data has been merged
    run_gold()
    logger.info("All partitions processed successfully.")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run the full ETL pipeline (Bronze → Silver → Gold). "
            "With no arguments, all date partitions in Archives/ are processed "
            "automatically from oldest to newest."
        )
    )
    parser.add_argument(
        "--source",
        default=None,
        help=(
            "Path to a specific source folder (e.g. Archives/ingest_date=2025-12-01). "
            "If omitted, all partitions in Archives/ are auto-discovered."
        ),
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Ingestion date. Required when --source is provided; ignored otherwise.",
    )
    args = parser.parse_args()

    if args.source:
        # Manual mode: single partition
        run_pipeline(source_dir=args.source, ingest_date=args.date)
    else:
        # Auto mode: discover and process all partitions
        run_pipeline_auto()
