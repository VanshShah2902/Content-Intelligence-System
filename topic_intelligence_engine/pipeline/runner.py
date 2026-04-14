import time
from datetime import date
from typing import List, Dict, Any
from core.logger import logger

# ─── Stage Imports ───────────────────────────────────────────
from ingestion.instagram import run_instagram_ingestion
try:
    from ingestion.reddit import run_ingestion as run_reddit_ingestion
except ImportError:
    run_reddit_ingestion = None
from processing.validator import validate_posts
from processing.cleaner import apply_cleaning
from processing.momentum import process_momentum
from ai.extraction import extract_batch
from clustering.clustering_service import cluster_posts
from analysis.topic_analyzer import analyze_batch
from ai.strategy import generate_batch
from ai.content_brief import generate_brief_batch
from pipeline.scoring import apply_batch_scoring

# ─── Database & Repositories ──────────────────────────────────
from core.database import SessionLocal
from db.repositories.topic_repository import TopicRepository
from db.repositories.cluster_repository import ClusterRepository
from db.repositories.final_output_repository import FinalOutputRepository
from db.repositories.post_repository import PostRepository


# ─── Config ──────────────────────────────────────────────────

DEBUG_MODE = False

STRATEGY_CONFIDENCE_THRESHOLD = 0.0   # Disabled for demo: Minimum quality bar for final output
MAX_RETRIES = 2                         # Attempts for LLM-heavy stages
MIN_REQUIRED_POSTS = 1                  # Disabled for demo: Hard minimum after extraction before clustering
STRUCTURED_LOGGING = False              # Toggle: emit structured dict logs alongside text
USE_INSTAGRAM_ONLY = True               # Flag: switch between Instagram and Reddit ingestion


def run_ingestion() -> List[Dict[str, Any]]:
    """Unified ingestion wrapper. Toggle USE_INSTAGRAM_ONLY to switch source."""
    if USE_INSTAGRAM_ONLY or run_reddit_ingestion is None:
        return run_instagram_ingestion()
    return run_reddit_ingestion()


# ─── Utility ─────────────────────────────────────────────────

def _log_stage(run_id: str, stage: str, input_size: int, output_size: int, duration: float) -> None:
    logger.info(
        f"[Run {run_id}] Stage: {stage:<22} | "
        f"Input: {input_size:>4} | "
        f"Output: {output_size:>4} | "
        f"Time: {duration:.2f}s"
    )
    if STRUCTURED_LOGGING:
        logger.info({
            "run_id": run_id, "stage": stage,
            "input": input_size, "output": output_size, "duration": duration
        })


def _timed(fn, *args, **kwargs):
    """Calls fn(*args, **kwargs), returns (result, duration_seconds)."""
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, round(time.perf_counter() - t0, 3)


def _with_retry(fn, run_id: str, stage: str, failure_counts: Dict, *args, **kwargs):
    """
    Attempts fn up to MAX_RETRIES times with exponential backoff.
    Tracks retry failures and final failures separately in failure_counts.
    """
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            attempt_start = time.perf_counter()
            result = fn(*args, **kwargs)
            attempt_time = round(time.perf_counter() - attempt_start, 3)
            logger.info(f"[Run {run_id}] {stage} attempt {attempt + 1} succeeded in {attempt_time}s")
            return result
        except Exception as e:
            last_exc = e
            retry_key = f"{stage}_retry"
            failure_counts[retry_key] = failure_counts.get(retry_key, 0) + 1
            sleep_time = 2 ** attempt
            logger.warning(
                f"[Run {run_id}] {stage} attempt {attempt + 1}/{MAX_RETRIES} failed: {e}. "
                f"Retrying in {sleep_time}s..."
            )
            time.sleep(sleep_time)
    
    final_key = f"{stage}_final_fail"
    failure_counts[final_key] = failure_counts.get(final_key, 0) + 1
    raise last_exc


# ─── Individual Stage Runners ─────────────────────────────────

def run_ingestion_stage(run_id: str, failure_counts: Dict) -> List[Dict[str, Any]]:
    source = "Instagram" if USE_INSTAGRAM_ONLY else "Reddit"
    logger.info(f"[Run {run_id}] Using {source}-only ingestion mode")
    try:
        posts, duration = _timed(run_ingestion)
        _log_stage(run_id, "Ingestion", 0, len(posts), duration)
        if not posts:
            logger.warning(f"[Run {run_id}] {source} ingestion returned 0 posts.")
        return posts
    except Exception as e:
        failure_counts["Ingestion"] = failure_counts.get("Ingestion", 0) + 1
        logger.error(f"[Run {run_id}] Ingestion stage failed: {e}")
        return []


def run_validation_stage(run_id: str, failure_counts: Dict, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not posts:
        logger.warning(f"[Run {run_id}] Validation skipped: empty input.")
        return []
    try:
        valid, duration = _timed(validate_posts, posts)
        _log_stage(run_id, "Validation", len(posts), len(valid), duration)
        return valid
    except Exception as e:
        failure_counts["Validation"] = failure_counts.get("Validation", 0) + 1
        logger.error(f"[Run {run_id}] Validation stage failed: {e}")
        return posts


def run_cleaning_stage(run_id: str, failure_counts: Dict, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not posts:
        logger.warning(f"[Run {run_id}] Cleaning skipped: empty input.")
        return []
    try:
        cleaned, duration = _timed(apply_cleaning, posts)
        _log_stage(run_id, "Cleaning", len(posts), len(cleaned), duration)
        return cleaned
    except Exception as e:
        failure_counts["Cleaning"] = failure_counts.get("Cleaning", 0) + 1
        logger.error(f"[Run {run_id}] Cleaning stage failed: {e}")
        return posts


def run_momentum_stage(run_id: str, failure_counts: Dict, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not posts:
        logger.warning(f"[Run {run_id}] Momentum scoring skipped: empty input.")
        return []
    try:
        scored, duration = _timed(process_momentum, posts)
        _log_stage(run_id, "Momentum Scoring", len(posts), len(scored), duration)
        return scored
    except Exception as e:
        failure_counts["Momentum"] = failure_counts.get("Momentum", 0) + 1
        logger.error(f"[Run {run_id}] Momentum stage failed: {e}")
        return posts


def run_extraction_stage(run_id: str, failure_counts: Dict, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # NOTE: Future optimization — extraction can be parallelized per-post using ThreadPoolExecutor
    if not posts:
        logger.warning(f"[Run {run_id}] AI Extraction skipped: empty input.")
        return []
    try:
        extracted, duration = _timed(
            _with_retry, extract_batch, run_id, "AI Extraction", failure_counts, posts
        )
        _log_stage(run_id, "AI Extraction", len(posts), len(extracted), duration)
        return extracted
    except Exception as e:
        failure_counts["AI Extraction_final_fail"] = failure_counts.get("AI Extraction_final_fail", 0) + 1
        logger.error(f"[Run {run_id}] AI Extraction stage failed after all retries: {e}")
        # Safe fallback: only pass through posts that already have extracted_topic from prior runs
        safe = [p for p in posts if "extracted_topic" in p]
        logger.warning(f"[Run {run_id}] Extraction fallback: {len(safe)} pre-extracted posts forwarded.")
        return safe


def run_clustering_stage(run_id: str, failure_counts: Dict, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not posts:
        logger.warning(f"[Run {run_id}] Clustering skipped: empty input.")
        return []
    try:
        clusters, duration = _timed(cluster_posts, posts)
        _log_stage(run_id, "Clustering", len(posts), len(clusters), duration)
        return clusters
    except Exception as e:
        failure_counts["Clustering"] = failure_counts.get("Clustering", 0) + 1
        logger.error(f"[Run {run_id}] Clustering stage failed: {e}")
        return []


def run_analysis_stage(run_id: str, failure_counts: Dict, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # NOTE: Future optimization — analysis can be batched per cluster_id using async LLM calls
    if not clusters:
        logger.warning(f"[Run {run_id}] Analysis skipped: no clusters.")
        return []
    try:
        analyzed, duration = _timed(
            _with_retry, analyze_batch, run_id, "Topic Analysis", failure_counts, clusters
        )
        _log_stage(run_id, "Topic Analysis", len(clusters), len(analyzed), duration)
        return analyzed
    except Exception as e:
        failure_counts["Topic Analysis"] = failure_counts.get("Topic Analysis", 0) + 1
        logger.error(f"[Run {run_id}] Analysis stage failed after all retries: {e}")
        return clusters


def run_strategy_stage(run_id: str, failure_counts: Dict, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not clusters:
        logger.warning(f"[Run {run_id}] Strategy generation skipped: no analyzed clusters.")
        return []
    try:
        strategies, duration = _timed(
            _with_retry, generate_batch, run_id, "Strategy Generation", failure_counts, clusters
        )
        _log_stage(run_id, "Strategy Generation", len(clusters), len(strategies), duration)
        return strategies
    except Exception as e:
        failure_counts["Strategy"] = failure_counts.get("Strategy", 0) + 1
        logger.error(f"[Run {run_id}] Strategy stage failed after all retries: {e}")
        return clusters


def run_content_brief_stage(run_id: str, failure_counts: Dict, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not clusters:
        logger.warning(f"[Run {run_id}] Creative brief generation skipped: no clusters.")
        return []
    try:
        enriched, duration = _timed(
            _with_retry, generate_brief_batch, run_id, "Creative Briefing", failure_counts, clusters
        )
        _log_stage(run_id, "Creative Briefing", len(clusters), len(enriched), duration)
        return enriched
    except Exception as e:
        failure_counts["CreativeBrief"] = failure_counts.get("CreativeBrief", 0) + 1
        logger.error(f"[Run {run_id}] Creative Brief stage failed after all retries: {e}")
        return clusters


def format_final_output(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    formatted = []
    
    for c in clusters:
        # Fallback to look inside dict or natively
        val_status = c.get("validation_status", "uncertain")
        val_summary = c.get("validation_summary")
        if isinstance(val_summary, dict) and "status" in val_summary:
            val_status = val_summary["status"]
            
        opp_score = c.get("opportunity_score", 0.0)
        if val_status == "misinformation":
            opp_score *= 1.1
            
        content_strategy = c.get("content_strategy", "educational")
        target_audience = c.get("target_audience", "general audience")
            
        formatted_cluster = {
            "topic_name": c.get("topic_name", ""),
            "trend_stage": c.get("trend_stage", ""),
            "opportunity_score": round(opp_score, 4),
            "validation_status": val_status,
            "quick_action": f"Create {content_strategy} content targeting {target_audience}",
            "why_trending": c.get("why_trending", ""),
            "content_brief": c.get("content_brief", {}),
            "top_posts": c.get("top_posts", []),
            "metadata": {
                "platform_distribution": c.get("platform_distribution", {}),
                "risk_factor": c.get("risk_factor", ""),
                "strategy_confidence": c.get("strategy_confidence", 0.0)
            }
        }
        formatted.append(formatted_cluster)
        
    # Sort
    formatted.sort(key=lambda x: x.get("opportunity_score", 0.0), reverse=True)
    formatted = formatted[:10]
    logger.info(f"Final output generated: {len(formatted)} topics")
    
    return formatted


# ─── Main Orchestrator ─────────────────────────────────────────

def run_pipeline() -> List[Dict[str, Any]]:
    """
    Executes the full Topic Intelligence Pipeline end-to-end.
    Each stage is isolated — failures log and continue.
    Returns fully enriched, quality-filtered cluster objects with strategy decisions.
    """
    pipeline_start = time.perf_counter()
    run_id = str(int(time.time()))
    failure_counts: Dict[str, int] = {}

    logger.info("=" * 60)
    logger.info(f"[Run {run_id}] TOPIC INTELLIGENCE ENGINE — PIPELINE STARTED")
    logger.info("=" * 60)

    # Stage 1: Ingestion
    raw_posts = run_ingestion_stage(run_id, failure_counts)
    if not raw_posts:
        logger.error(f"[Run {run_id}] Pipeline halted: Ingestion returned 0 posts.")
        return []

    # raw_posts = raw_posts[:3] # REMOVED FOR DEMO: Allow all posts through

    if DEBUG_MODE:
        logger.info("DEBUG MODE ACTIVE: limiting pipeline execution")
        # raw_posts = raw_posts[:3] # REMOVED FOR DEMO: Allow all posts through

    # Stage 2: Validation
    valid_posts = run_validation_stage(run_id, failure_counts, raw_posts)

    # Stage 3: Cleaning
    clean_posts = run_cleaning_stage(run_id, failure_counts, valid_posts)

    # Stage 4: Momentum Scoring
    scored_posts = run_momentum_stage(run_id, failure_counts, clean_posts)

    # Stage 5: AI Extraction (retried up to MAX_RETRIES)
    extracted_posts = run_extraction_stage(run_id, failure_counts, scored_posts)

    # Hard guard: abort if extraction returned too few usable posts
    if not DEBUG_MODE and len(extracted_posts) < MIN_REQUIRED_POSTS:
        logger.error(
            f"[Run {run_id}] Pipeline halted: only {len(extracted_posts)} posts survived extraction "
            f"(minimum required: {MIN_REQUIRED_POSTS})."
        )
        return []
    
    if DEBUG_MODE:
        strategy_clusters = [{
            "topic_name": p.get("extracted_topic", f"Debug Topic {i+1}"),
            "strategy_confidence": 1.0,
            "opportunity_score": 9.9,
            "trend_stage": "Emerging",
            "validation_status": "verified",
            "top_posts": [p]
        } for i, p in enumerate(extracted_posts)]
    else:
        # Stage 6: Clustering
        clusters = run_clustering_stage(run_id, failure_counts, extracted_posts)
        if not clusters:
            logger.warning(f"[Run {run_id}] Pipeline produced 0 clusters. Halting.")
            return []

        # Stage 7: Topic Analysis (retried up to MAX_RETRIES)
        analyzed_clusters = run_analysis_stage(run_id, failure_counts, clusters)

        # Stage 8: Strategy Generation (retried up to MAX_RETRIES)
        strategy_clusters = run_strategy_stage(run_id, failure_counts, analyzed_clusters)
        
        # Stage 9: Opportunity Scoring (New)
        logger.info(f"[Run {run_id}] Applying Opportunity Scoring...")
        strategy_clusters = apply_batch_scoring(strategy_clusters)
        
        # Stage 10: High-Quality Content Brief (New)
        logger.info(f"[Run {run_id}] Generating High-Quality Creative Briefs...")
        strategy_clusters = run_content_brief_stage(run_id, failure_counts, strategy_clusters)

    # ── Quality Filter ───────────────────────────────────────
    final_clusters = [
        c for c in strategy_clusters
        if c.get("strategy_confidence", 0.0) >= STRATEGY_CONFIDENCE_THRESHOLD
    ]
    dropped = len(strategy_clusters) - len(final_clusters)
    if dropped > 0:
        logger.info(f"[Run {run_id}] Quality filter removed {dropped} low-confidence clusters.")

    # ── Content-Ready Formatting ─────────────────────────────
    final_payload = format_final_output(final_clusters)

    # ── Database Persistence ─────────────────────────────────
    if final_payload:
        logger.info(f"[Run {run_id}] Persisting {len(final_payload)} topics to database...")
        db = SessionLocal()
        try:
            for item in final_payload:
                # 1. Sync Topic
                topic_name = item.get("topic_name")
                existing_topic = TopicRepository.get_by_name(db, topic_name)
                
                if not existing_topic:
                    topic_obj = TopicRepository.create(db, {
                        "topic_name": topic_name,
                        "topic_signature": item.get("metadata", {}).get("platform_distribution")
                    })
                else:
                    topic_obj = TopicRepository.update_last_seen(db, existing_topic.topic_id, date.today())
                
                # 2. Sync Cluster Snapshot
                ClusterRepository.create(db, {
                    "topic_id": topic_obj.topic_id,
                    "date": date.today(),
                    "avg_momentum": None, # could be extracted from clusters if needed
                    "total_posts": len(item.get("top_posts", [])),
                    "trend_stage": item.get("trend_stage"),
                    "opportunity_score": item.get("opportunity_score")
                })
                
                # 3. Store Final Strategic Output
                brief = item.get("content_brief", {})
                FinalOutputRepository.create(db, {
                    "topic_id": topic_obj.topic_id,
                    "date": date.today(),
                    "opportunity_score": item.get("opportunity_score"),
                    "recommended_action": item.get("quick_action"),
                    "content_strategy": item.get("content_brief", {}).get("strategy"),
                    "target_audience": item.get("content_brief", {}).get("target_audience"),
                    "risk_factor": item.get("metadata", {}).get("risk_factor"),
                    "angles": brief.get("angles"),
                    "hooks": brief.get("hooks"),
                    "key_points": brief.get("key_points")
                })
            
            db.commit()
            logger.info(f"[Run {run_id}] Database persistence complete.")
        except Exception as e:
            logger.error(f"[Run {run_id}] Database persistence failed: {e}")
            db.rollback()
        finally:
            db.close()

    # ── Top Opportunities Summary ─────────────────────────────
    if final_payload:
        logger.info(f"[Run {run_id}] Top Opportunities:")
        for i, c in enumerate(final_payload[:3], 1):
            logger.info(
                f"  #{i} | {c.get('topic_name', 'N/A'):<35} | "
                f"Score: {c.get('opportunity_score', 0):.2f} | "
                f"Status: {c.get('validation_status', 'N/A')}"
            )

    # ── Failure Summary ───────────────────────────────────────
    if failure_counts:
        total_retries = sum(v for k, v in failure_counts.items() if k.endswith("_retry"))
        total_final = sum(v for k, v in failure_counts.items() if k.endswith("_final_fail"))
        logger.warning(
            f"[Run {run_id}] Failure summary | "
            f"Retries: {total_retries} | Final failures: {total_final} | Detail: {failure_counts}"
        )

    # ── Pipeline Summary ──────────────────────────────────────
    total_duration = round(time.perf_counter() - pipeline_start, 2)
    logger.info("=" * 60)
    logger.info(
        f"[Run {run_id}] PIPELINE COMPLETE | "
        f"Posts: {len(raw_posts)} → Topics: {len(final_payload)} | "
        f"Total Time: {total_duration}s"
    )
    logger.info("=" * 60)

    return final_payload


# ─── Entrypoint ────────────────────────────────────────────────

if __name__ == "__main__":
    # TODO: integrate topic history tracking — load previous cluster states for longitudinal trend linking
    # TODO: persist results to DB — save final_clusters via SQLAlchemy session after pipeline completes
    # TODO: schedule daily execution — wrap run_pipeline() with APScheduler or a cron job
    try:
        results = run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        results = []
        
    if not results:
        logger.warning("No actionable clusters generated this run.")
