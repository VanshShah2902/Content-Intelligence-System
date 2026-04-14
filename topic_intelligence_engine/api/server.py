import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import sys
import os

# Resolve paths
_api_dir         = os.path.dirname(os.path.abspath(__file__))          # .../api/
_pkg_dir         = os.path.dirname(_api_dir)                            # .../topic_intelligence_engine/
_project_root    = os.path.dirname(_pkg_dir)                            # .../Content_intelligence_engine/

# Add project root so `from topic_intelligence_engine.X import Y` resolves
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Add package dir so internal bare imports inside runner/instagram/etc. resolve
if _pkg_dir not in sys.path:
    sys.path.insert(1, _pkg_dir)

from topic_intelligence_engine.pipeline.runner import run_pipeline
from topic_intelligence_engine.ingestion.instagram import run_instagram_ingestion

logger = logging.getLogger(__name__)

def scheduled_ingestion():
    logger.info("Scheduled ingestion: fetching Instagram data for all creators...")
    try:
        posts = run_instagram_ingestion()
        logger.info(f"Scheduled ingestion complete. {len(posts)} posts processed.")
    except Exception as e:
        logger.error(f"Scheduled ingestion failed: {e}")

_scheduler = BackgroundScheduler()
_scheduler.add_job(scheduled_ingestion, "interval", hours=12, id="instagram_ingestion")

@asynccontextmanager
async def lifespan(app: FastAPI):
    _scheduler.start()
    logger.info("APScheduler started — Instagram ingestion every 12 hours.")
    # Run once immediately on startup
    scheduled_ingestion()
    yield
    _scheduler.shutdown()
    logger.info("APScheduler stopped.")

app = FastAPI(title="Content Intelligence API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache Configuration
CACHE_TTL = 3600  # 1 hour in seconds
_cache = {
    "last_run": 0,
    "last_result": []
}

def execute_pipeline():
    logger.info("Starting pipeline execution via API...")
    try:
        results = run_pipeline()
        _cache["last_result"] = results
        _cache["last_run"] = time.time()
        logger.info(f"Pipeline executed successfully. Generated {len(results)} topics.")
        return results
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return []

@app.get("/api/topics")
def get_topics():
    current_time = time.time()
    
    # Check if cache is valid (exists and newer than CACHE_TTL)
    if _cache["last_result"] and (current_time - _cache["last_run"] < CACHE_TTL):
        logger.info("Serving topics from memory cache.")
        return {"topics": _cache["last_result"]}
        
    # Cache missed or expired, running synchronously
    logger.info("Cache missed or expired. Triggering fresh pipeline run...")
    results = execute_pipeline()
    return {"topics": results}

@app.post("/api/refresh")
def refresh_topics():
    logger.info("Forcing pipeline refresh directly from API.")
    # Run synchronously to return fresh results immediately
    results = execute_pipeline()
    return {"topics": results}
