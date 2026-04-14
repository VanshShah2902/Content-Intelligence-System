from datetime import datetime, timezone
from typing import List, Dict, Any
from core.logger import logger
from config.settings import settings

def calculate_time_decay_factor(timestamp_str: str) -> float:
    try:
        post_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        now = datetime.now(timezone.utc)

        hours_elapsed = max((now - post_time).total_seconds() / 3600.0, 0)

        # smoother decay curve
        decay = (hours_elapsed + 2) ** 1.3
        return float(max(1.0, decay))

    except Exception as e:
        logger.warning(f"Bad timestamp {timestamp_str}, fallback decay. Err: {e}")
        return 9999.0

def compute_engagement_ratio(likes: int, comments: int) -> float:
    """
    Balanced engagement signal.
    Avoids inflation of small posts.
    """
    total = likes + comments
    if total == 0:
        return 0.0

    return (comments + 0.5 * likes) / total

def compute_creator_baselines(posts: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """
    Computes absolute baseline engagement and views averages for creators.
    Used to calculate relative performance overrides.
    """
    creator_data = {}

    for post in posts:
        creator = post.get("creator_id", "unknown")

        if creator not in creator_data:
            creator_data[creator] = {
                "engagement": [],
                "views": []
            }

        likes = post.get("likes", 0)
        comments = post.get("comments", 0)
        views = post.get("views", 0)

        creator_data[creator]["engagement"].append(likes + comments)
        creator_data[creator]["views"].append(views)

    baselines = {}

    for creator, data in creator_data.items():
        baselines[creator] = {
            "engagement": sum(data["engagement"]) / max(len(data["engagement"]), 1),
            "views": sum(data["views"]) / max(len(data["views"]), 1)
        }

    return baselines

def apply_relative_scoring(posts: List[Dict[str, Any]], baselines: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    """
    Calculates relative score based on weighted engagement and view signal vs baselines.
    Weights: 70% Engagement, 30% Views.
    """
    for post in posts:
        creator = post.get("creator_id", "unknown")

        baseline = baselines.get(creator, {
            "engagement": 1.0,
            "views": 1.0
        })

        likes = post.get("likes", 0)
        comments = post.get("comments", 0)
        views = post.get("views", 0)
        engagement = likes + comments

        # Calculate ratios relative to baseline
        engagement_score = engagement / max(baseline["engagement"], 1.0)
        view_score = views / max(baseline["views"], 1.0)

        # 70/30 Hybrid Relative Score
        relative_score = (
            engagement_score * 0.7 +
            view_score * 0.3
        )

        post["baseline_engagement"] = round(baseline["engagement"], 3)
        post["baseline_views"] = round(baseline["views"], 3)
        post["relative_score"] = round(relative_score, 3)
        post["is_outlier"] = relative_score >= 1.5

    return posts



def process_momentum(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # 1. Compute and Apply Relative Scoring
    baselines = compute_creator_baselines(posts)
    posts = apply_relative_scoring(posts, baselines)
    
    scored_posts = []
    outlier_count = 0
    total_relative_score = 0.0
    relative_count = 0

    for post in posts:
        enriched_post = post.copy()
        
        platform = enriched_post.get("platform", "reddit")
        likes = post.get("likes", 0)
        comments = post.get("comments", 0)
        
        # 2. Existing Absolute Momentum calculations (preserved for hybrid logic)
        decay_factor = calculate_time_decay_factor(post.get("timestamp", ""))
        momentum_score = (likes + comments) / decay_factor
        engagement_ratio = compute_engagement_ratio(likes, comments)
        weighted_score = momentum_score * engagement_ratio

        enriched_post["momentum_score"] = round(momentum_score, 4)
        enriched_post["weighted_score"] = round(weighted_score, 4)
        
        # 3. Finalize hybrid priority sorting values
        relative_score = enriched_post.get("relative_score", 0.0)
        is_outlier = enriched_post.get("is_outlier", False)
        
        if is_outlier:
            outlier_count += 1
            
        if platform == "instagram":
            # For IG, we prioritize relative performance (outperfomers)
            sort_val = relative_score
        else:
            # For Reddit or others, we use normalized absolute momentum
            sort_val = weighted_score / 100.0
            
        total_relative_score += relative_score
        relative_count += 1

        # Append temporal hybrid sorting bounds to the dict
        enriched_post["_sort_value"] = sort_val
        # Check pass_filter against global settings threshold
        enriched_post["_pass_filter"] = weighted_score >= settings.min_weighted_score_threshold or is_outlier
        
        scored_posts.append(enriched_post)

    # 4. Hybrid platform sorting priority
    scored_posts.sort(key=lambda x: x["_sort_value"], reverse=True)
    
    # Extract only filter-passing posts (DISABLED FOR DEMO)
    filtered_posts = scored_posts  # Disable filtering for demo

    # Cleanup temp algorithmic state vars
    for p in scored_posts:
        p.pop("_sort_value", None)
        p.pop("_pass_filter", None)

    # 6. Final return of all posts
    avg_rel = total_relative_score / max(1, relative_count)
    
    # 7. Additive Logging
    logger.info(f"Posts before filtering: {len(posts)}")
    logger.info(f"Posts after filtering: {len(filtered_posts)}")
    logger.info(f"Momentum Processing | Selected: {len(filtered_posts)}/{len(posts)} | Outliers: {outlier_count} | Avg Relative Score: {avg_rel:.2f}")

    return filtered_posts
