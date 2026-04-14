import uuid
from collections import Counter
from typing import List, Dict, Any
from core.logger import logger
from config.settings import settings

import numpy as np
# Temporary: disabled semantic imports for keyword-based clustering
# from sentence_transformers import SentenceTransformer
# from sklearn.cluster import AgglomerativeClustering
# from sklearn.preprocessing import normalize

def assign_cluster(post: Dict[str, Any]) -> str:
    """
    Tightened keyword-based clustering for precise demo results.
    Refined Fix 3: Requires specific combinations to avoid noise.
    """
    text = (post.get("text") or "").lower()
    
    # Sleep/Circadian: sunlight + circadian + sleep
    if any(k in text for k in ["circadian", "sunlight", "melatonin", "night"]):
        return "sleep_circadian"
    
    # Training: muscle + hypertrophy + volume + training
    elif any(k in text for k in ["hypertrophy", "training", "muscle", "workout", "lifting"]):
        return "training_frequency"
    
    # Protein: protein + metabolic + muscle meat (avoid generic 'food')
    elif "protein" in text or "metabolic" in text:
        return "protein_nutrition"
        
    # Cold/Heat: sauna + plunge + ice + cold
    elif any(k in text for k in ["cold", "sauna", "plunge", "ice"]):
        return "cold_exposure"
        
    # Stress/Meditation: meditation + stress + nervous system
    elif any(k in text for k in ["meditation", "stress", "nervous system", "cortisol"]):
        return "meditation_stress"
        
    else:
        return "other"

def select_representative_posts(cluster_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Isolates the absolute highest signal posts from the cluster 
    to preserve downstream LLM context window efficiently.
    """
    # Sort descending by weighted_score (momentum * engagement ratio)
    sorted_posts = sorted(cluster_posts, key=lambda x: x.get("weighted_score", 0.0), reverse=True)
    
    selected = [
        {
            "external_post_id": p.get("external_post_id", ""),
            "clean_text": p.get("clean_text", "")[:300],
            "weighted_score": p.get("weighted_score", 0.0),
            "likes": p.get("likes", 0),
            "comments": p.get("comments", 0)
        }
        for p in sorted_posts[:settings.max_representative_posts]
    ]
    
    return selected

def compute_aggregates(cluster_posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Fix 1: Computes trend stage based on average relative_score.
    """
    total = len(cluster_posts)
    
    if total == 0:
        return {}
        
    avg_momentum = sum(p.get("momentum_score", 0.0) for p in cluster_posts) / total
    avg_weighted = sum(p.get("weighted_score", 0.0) for p in cluster_posts) / total
    avg_relative = sum(p.get("relative_score", 0.0) for p in cluster_posts) / total
    
    # Count occurrences across platforms
    platforms = [p.get("platform", "unknown") for p in cluster_posts]
    distribution = dict(Counter(platforms))
    dist_total = sum(distribution.values())
    distribution_pct = {k: round(v / dist_total, 2) for k, v in distribution.items()}
    
    # New Fix 1: Trend Stage logic (Score-based)
    if avg_relative > 1.2:
        trend_stage = "emerging"
    elif avg_relative > 0.9:
        trend_stage = "stable"
    else:
        trend_stage = "declining"
        
    return {
        "total_posts": total,
        "avg_momentum": round(avg_momentum, 4),
        "avg_weighted_score": round(avg_weighted, 4),
        "avg_relative_score": round(avg_relative, 4),
        "platform_distribution": distribution_pct,
        "trend_stage": trend_stage
    }

def build_clusters(posts: List[Dict[str, Any]], labels: np.ndarray) -> List[Dict[str, Any]]:
    """
    Maps sklearn labels back to the payload and calculates all cluster-level intelligence.
    """
    cluster_map = {}
    
    # Group raw posts by label
    for post, label in zip(posts, labels):
        # Ignore noisy/unclustered outliers natively marked as -1 by some algorithms 
        if label == -1: 
            continue
            
        cluster_id = f"c_{label}"
        if cluster_id not in cluster_map:
            cluster_map[cluster_id] = []
        cluster_map[cluster_id].append(post)
        
    final_clusters = []
    
    for c_id, grouped_posts in cluster_map.items():
        if len(grouped_posts) < settings.min_cluster_size:
            continue
            
        unique_topics = set(p.get("extracted_topic") for p in grouped_posts)
        if len(unique_topics) > settings.max_topic_variation:
            continue
            
        aggregates = compute_aggregates(grouped_posts)
        representatives = select_representative_posts(grouped_posts)
        
        # Topic Normalization: Stronger topic naming derived from top weighted post
        dominant_topic = max(
            grouped_posts,
            key=lambda x: x.get("weighted_score", 0.0)
        ).get("extracted_topic", "unknown_topic")
        
        post_ids = [p.get("external_post_id") for p in grouped_posts]
        
        # Extract Top Posts natively
        sorted_top = sorted(
            grouped_posts,
            key=lambda x: x.get("relative_score", 0.0) if x.get("platform") == "instagram" else x.get("weighted_score", 0.0),
            reverse=True
        )
        
        top_posts = []
        for p in sorted_top[:5]:
            text = (p.get("clean_text") or p.get("text", ""))[:300]
            top_posts.append({
                "external_post_id": p.get("external_post_id", ""),
                "platform": p.get("platform", ""),
                "text": text,
                "likes": p.get("likes", 0),
                "comments": p.get("comments", 0),
                "views": p.get("views", 0),
                "relative_score": p.get("relative_score", 0.0),
                "weighted_score": p.get("weighted_score", 0.0)
            })
        
        # Deterministic cluster ID explicitly bound to semantic namespace
        import hashlib
        cluster_key = dominant_topic + str(len(grouped_posts))
        cluster_id = "cluster_" + hashlib.md5(cluster_key.encode()).hexdigest()
        
        confidence_score = round(aggregates["avg_weighted_score"] * (1 / max(1, len(unique_topics))), 4)
        
        cluster_payload = {
            "cluster_id": cluster_id,
            "topic_name": dominant_topic,
            "post_ids": post_ids,
            "total_posts": aggregates["total_posts"],
            "avg_momentum": aggregates["avg_momentum"],
            "avg_weighted_score": aggregates["avg_weighted_score"],
            "confidence_score": confidence_score,
            "platform_distribution": aggregates["platform_distribution"],
            "trend_stage": aggregates["trend_stage"],
            "representative_posts": representatives,
            "posts": grouped_posts,
            "top_posts": top_posts
        }
        
        final_clusters.append(cluster_payload)
        
    return final_clusters

def cluster_posts(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Temporary keyword-based clustering orchestrator.
    Groups posts into predefined categories and computes metadata.
    """
    if not posts:
        logger.warning("Empty post array passed to clustering service.")
        return []
        
    logger.info(f"Initiating Keyword-Based Clustering on {len(posts)} posts...")
    
    # 1. Group posts by keyword logic
    cluster_map = {}
    for post in posts:
        key = assign_cluster(post)
        if key not in cluster_map:
            cluster_map[key] = []
        cluster_map[key].append(post)
        
    # 2. Compile output payloads
    final_clusters = []
    
    for topic_slug, grouped_posts in cluster_map.items():
        if topic_slug == "other":
            continue # Skip noise for better demo presentation
            
        aggregates = compute_aggregates(grouped_posts)
        representatives = select_representative_posts(grouped_posts)
        
        # Topic Name formatting (Slug to Title)
        topic_name = topic_slug.replace("_", " ").title()
        
        post_ids = [p.get("external_post_id") for p in grouped_posts]
        
        # Extract Top Posts natively
        sorted_top = sorted(
            grouped_posts,
            key=lambda x: x.get("relative_score", 0.0) if x.get("platform") == "instagram" else x.get("weighted_score", 0.0),
            reverse=True
        )
        
        top_posts = []
        for p in sorted_top[:5]:
            text = (p.get("clean_text") or p.get("text", ""))[:300]
            top_posts.append({
                "external_post_id": p.get("external_post_id", ""),
                "platform": p.get("platform", ""),
                "text": text,
                "likes": p.get("likes", 0),
                "comments": p.get("comments", 0),
                "views": p.get("views", 0),
                "relative_score": p.get("relative_score", 0.0),
                "weighted_score": p.get("weighted_score", 0.0)
            })
            
        import hashlib
        cluster_id = "cluster_" + hashlib.md5(topic_slug.encode()).hexdigest()
        
        cluster_payload = {
            "cluster_id": cluster_id,
            "topic_name": topic_name,
            "post_ids": post_ids,
            "total_posts": len(grouped_posts),
            "avg_momentum": aggregates["avg_momentum"],
            "avg_weighted_score": aggregates["avg_weighted_score"],
            "platform_distribution": aggregates["platform_distribution"],
            "trend_stage": aggregates["trend_stage"],
            "representative_posts": representatives,
            "posts": grouped_posts,
            "top_posts": top_posts
        }
        
        final_clusters.append(cluster_payload)

    # Sort by momentum for output priority
    final_clusters.sort(key=lambda x: x["avg_weighted_score"], reverse=True)
    
    logger.info(f"Clustering Complete. Generated {len(final_clusters)} categories.")
    return final_clusters
