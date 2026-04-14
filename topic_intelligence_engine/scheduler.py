"""
Standalone scheduler — run this once to keep Instagram ingestion
running every 12 hours automatically.

Usage:
    python scheduler.py

Keep this terminal/process running. It will fetch data on startup
and then every 12 hours automatically.
"""

import sys
import os
import logging

# Resolve paths so internal imports work
_dir = os.path.dirname(os.path.abspath(__file__))
if _dir not in sys.path:
    sys.path.insert(0, _dir)

from apscheduler.schedulers.blocking import BlockingScheduler
from ingestion.instagram import run_instagram_ingestion
from core.logger import logger

def job():
    logger.info("=== Scheduled ingestion triggered ===")
    try:
        posts = run_instagram_ingestion()
        logger.info(f"=== Ingestion done: {len(posts)} posts processed ===")
    except Exception as e:
        logger.error(f"=== Ingestion failed: {e} ===")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    scheduler = BlockingScheduler()
    scheduler.add_job(job, "interval", hours=12, id="instagram_ingestion")

    logger.info("Scheduler started. Running ingestion now, then every 12 hours...")
    job()  # run immediately on start

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
