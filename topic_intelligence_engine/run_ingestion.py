"""
Standalone ingestion runner — used by GitHub Actions (and local runs).
Fetches all posts for every configured creator via Apify and saves to Excel.

Usage:
    python run_ingestion.py
"""

import sys
import os
import logging

# Ensure all internal package imports resolve
_dir = os.path.dirname(os.path.abspath(__file__))
if _dir not in sys.path:
    sys.path.insert(0, _dir)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from ingestion.instagram import run_instagram_ingestion
from config.creators import CREATORS

if __name__ == "__main__":
    logger.info(f"Starting ingestion for {len(CREATORS)} creators: {CREATORS}")
    posts = run_instagram_ingestion()
    logger.info(f"Ingestion complete — {len(posts)} total posts processed.")
