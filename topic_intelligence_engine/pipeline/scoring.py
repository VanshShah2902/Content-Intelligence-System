import math
from typing import List, Dict, Any

def compute_opportunity_score(cluster: Dict[str, Any]) -> float:
    """
    Calculates a production-grade opportunity score for a topic cluster.
    Factors in momentum, volume, logarithmic reach, and consistent outperformance.
    Penalizes based on risk/validation status.
    """
    posts = cluster.get("posts", [])

    if not posts:
        return 0.0

    # --- Momentum: average relative score of posts in cluster ---
    momentum = sum(p.get("relative_score", 0.0) for p in posts) / len(posts)

    # --- Volume: raw number of posts ---
    volume = len(posts)

    # --- Reach: Log scaling of average view counts ---
    avg_views = sum(p.get("views", 0) for p in posts) / len(posts)
    reach_score = math.log10(avg_views + 1) # Use log10 for better manual intuition scaling

    # --- Consistency: Proportion of posts that outperform their creator's baseline (> 1.0) ---
    consistent_posts = [p for p in posts if p.get("relative_score", 0.0) > 1.0]
    consistency = len(consistent_posts) / len(posts)

    # --- Risk Penalty: Based on validation status ---
    validation = cluster.get("validation_status", "uncertain")

    if validation == "misinformation":
        risk_penalty = 0.3
    elif validation == "uncertain":
        risk_penalty = 0.7
    else:
        # "verified" or "safe"
        risk_penalty = 1.0

    # --- Final Score Equation ---
    # Weights: Momentum (40%), Volume (20%), Reach (20%), Consistency (20%)
    score = (
        momentum * 0.4 +
        math.log(volume + 1) * 0.2 +
        reach_score * 0.2 +
        consistency * 0.2
    ) * risk_penalty

    return round(float(score), 3)

def apply_batch_scoring(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Applies opportunity score calculation to a list of analyzed clusters.
    """
    for cluster in clusters:
        opp_score = compute_opportunity_score(cluster)
        cluster["opportunity_score"] = opp_score
        
        # New FIX 4: Strategy Confidence
        # Standardized normalization against a theoretical max_score of 10.0
        cluster["strategy_confidence"] = round(min(opp_score / 10.0, 1.0), 3)
        
    return clusters
