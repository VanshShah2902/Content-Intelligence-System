import json
import re
from typing import List, Dict, Any
from core.logger import logger
from ai.llm_client import call_llm_json

# ─────────────────────────────────────────────
# DETERMINISTIC CODE LAYER
# ─────────────────────────────────────────────

# Stage weights used in opportunity scoring
STAGE_WEIGHTS = {
    "emerging": 1.0,
    "peaking": 0.6,
    "declining": 0.2
}

# Growth multipliers applied on top of stage weights
GROWTH_MULTIPLIERS = {
    "emerging": 1.2,
    "peaking": 1.0,
    "declining": 0.6
}

# Controversy thresholds for risk classification
RISK_THRESHOLDS = {
    "high": 0.65,
    "medium": 0.35
}


def compute_saturation(cluster: Dict[str, Any]) -> float:
    """
    Estimates the content crowding level based on post volume and platform diversity.
    Platform diversity lowers saturation by implying the topic is spread thin.
    Normalizes to 0.0–1.0 with a soft-cap of 100 posts.
    """
    total_posts = cluster.get("total_posts", 0)
    
    # Platform diversity factor: more platforms = lower apparent saturation per platform
    platform_factor = len(cluster.get("platform_distribution", {})) / 2
    
    base_saturation = min(total_posts / 100.0, 1.0)
    saturation = max(base_saturation - (platform_factor * 0.1), 0.0)
    return round(saturation, 4)


def compute_opportunity(cluster: Dict[str, Any], saturation: float) -> float:
    """
    Core opportunity metric. Estimates how open this trend window is for new content.
    
    Formula: (avg_momentum * stage_weight * growth_multiplier) / (saturation + 1)
    
    Emerging trend with low saturation → high opportunity.
    Declining trend with high saturation → near-zero opportunity.
    """
    avg_momentum = cluster.get("avg_momentum", 0.0)
    trend_stage = cluster.get("trend_stage", "declining")
    
    stage_weight = STAGE_WEIGHTS.get(trend_stage, 0.2)
    growth_multiplier = GROWTH_MULTIPLIERS.get(trend_stage, 0.6)
    
    raw_score = (avg_momentum * stage_weight * growth_multiplier) / (saturation + 1.0)
    
    # Normalize: Cap is 100 (avg_momentum) * 1.0 (stage) * 1.2 (growth) → 120.
    normalized = min(raw_score / 120.0, 1.0)
    return round(normalized, 4)


def compute_risk(cluster: Dict[str, Any]) -> str:
    """
    Classifies risk based on controversy level and trend direction.
    Declining controversial topics are especially high-risk (shrinking ROI + backlash potential).
    """
    controversy = cluster.get("controversy_level", 0.0)
    trend_stage = cluster.get("trend_stage", "declining")
    
    if controversy >= RISK_THRESHOLDS["high"] or (trend_stage == "declining" and controversy >= RISK_THRESHOLDS["medium"]):
        return "high"
    elif controversy >= RISK_THRESHOLDS["medium"]:
        return "medium"
    else:
        return "low"


# ─────────────────────────────────────────────
# LLM INFERENCE LAYER
# ─────────────────────────────────────────────

def build_strategy_prompt(cluster: Dict[str, Any], saturation: float, opportunity: float, risk: str) -> str:
    """
    Builds a tightly scoped prompt combining all computed and LLM-extracted signals
    to force the model to produce business-grade strategic recommendations.
    """
    return f"""You are a senior content strategist for a health and wellness brand.
You have been given structured intelligence about a trending social media topic.
Your task: decide whether and how to enter this topic with content.

Do NOT give generic answers. Be precise and business-specific.

─── TOPIC INTELLIGENCE ───
Topic: "{cluster.get('topic_name', 'Unknown')}"
Why Trending: {cluster.get('why_trending', 'Unknown')}
Trigger Event: {cluster.get('trigger_event', 'Unknown')}
Audience Psychology: {cluster.get('audience_psychology', 'Unknown')}
Content Pattern: {cluster.get('content_pattern', 'Unknown')}
Format Pattern: {cluster.get('format_pattern', 'Unknown')}

─── COMPUTED SIGNALS ───
Opportunity Score: {opportunity} (0=no opportunity, 1=wide open)
Content Saturation: {saturation} (0=uncrowded, 1=extremely saturated)
Risk Factor: {risk}
Trend Stage: {cluster.get('trend_stage', 'unknown')}
Avg Momentum: {cluster.get('avg_momentum', 0.0)}
Controversy Level: {cluster.get('controversy_level', 0.0)}

─── TASK ───
Based solely on the above, provide:
1. "competition_insight": What angle are others taking? What is missing from the current conversation?
2. "recommended_action": Exactly ONE of: "enter aggressively", "enter with differentiation", "monitor only", "avoid"
3. "content_strategy": Exactly ONE of: "myth-busting", "educational", "contrarian", "story-driven", "listicle", "opinion"
4. "target_audience": Who exactly should this content be written for? (Be specific: age, intent, experience level)

OUTPUT FORMAT:
Return ONLY a valid JSON object:
{{
    "competition_insight": "...",
    "recommended_action": "...",
    "content_strategy": "...",
    "target_audience": "..."
}}
"""


def extract_safe_json(raw_text: str, cluster_id: str) -> Dict[str, str]:
    """Multi-match JSON extractor with fallback."""
    try:
        matches = re.findall(r'\{.*?\}', str(raw_text), re.DOTALL)
        if matches:
            return json.loads(matches[0])
        return json.loads(raw_text)
    except Exception as e:
        logger.error(f"Strategy JSON parse error for {cluster_id}: {e}")
        return {}


def generate_strategy(cluster: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hybrid engine: deterministic scoring + LLM strategic reasoning.
    Additively enriches the analyzed cluster with full decision layer output.
    """
    enriched = cluster.copy()
    c_id = enriched.get("cluster_id", "UNKNOWN")
    
    # ── 1. Deterministic Metrics ──
    saturation = compute_saturation(cluster)
    opportunity = compute_opportunity(cluster, saturation)
    risk = compute_risk(cluster)
    
    enriched["content_saturation_score"] = saturation
    enriched["opportunity_score"] = opportunity
    enriched["risk_factor"] = risk
    
    # Strategy confidence: combined signal strength
    analysis_confidence = cluster.get("analysis_confidence", 0.0)
    strategy_confidence = round(opportunity * analysis_confidence, 4)
    enriched["strategy_confidence"] = strategy_confidence
    
    logger.debug(f"[{c_id}] sat={saturation}, opp={opportunity}, risk={risk}, strat_conf={strategy_confidence}")
    
    # ── 2. LLM Fallbacks ──
    fallback = {
        "competition_insight": "Insufficient data to assess competition.",
        "recommended_action": "monitor only",
        "content_strategy": "educational",
        "target_audience": "General health-conscious adults.",
        "content_strategy": "educational",
        "target_audience": "General health-conscious adults."
    }
    
    valid_actions = {"enter aggressively", "enter with differentiation", "monitor only", "avoid"}
    valid_strategies = {"myth-busting", "educational", "contrarian", "story-driven", "listicle", "opinion"}
    
    # ── 3. LLM Inference ──
    prompt = build_strategy_prompt(cluster, saturation, opportunity, risk)
    
    try:
        raw = call_llm_json(prompt)
        parsed = extract_safe_json(raw, c_id)
        
        for key in fallback.keys():
            value = str(parsed.get(key, fallback[key])).strip()
            
            # Minimum content guard
            if len(value.split()) < 3:
                value = fallback[key]
            
            enriched[key] = value
        
            enriched[key] = value
        
        # Strict enum enforcement for action and strategy
        if enriched.get("recommended_action") not in valid_actions:
            logger.warning(f"[{c_id}] Invalid recommended_action. Defaulting to 'monitor only'.")
            enriched["recommended_action"] = "monitor only"
        
        # Low analysis confidence → force conservative action regardless of LLM suggestion
        if cluster.get("analysis_confidence", 0) < 0.2:
            logger.warning(f"[{c_id}] Low analysis confidence. Overriding action to 'monitor only'.")
            enriched["recommended_action"] = "monitor only"
        
        if enriched.get("content_strategy") not in valid_strategies:
            logger.warning(f"[{c_id}] Invalid content_strategy. Defaulting to 'educational'.")
            enriched["content_strategy"] = "educational"
    
    except Exception as e:
        logger.error(f"Strategy generation failed for {c_id}: {e}")
        enriched.update(fallback)
    
    logger.info(f"[{c_id}] Strategy: action='{enriched.get('recommended_action')}' | risk='{risk}' | opp={opportunity}")
    return enriched


def generate_batch(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Processes all analyzed clusters through the strategy layer sequentially.
    Returns fully enriched cluster objects ready for database persistence.
    """
    import time
    logger.info(f"Generating strategies for {len(clusters)} clusters...")
    results = []
    
    for i, cluster in enumerate(clusters):
        logger.debug(f"Strategy {i+1}/{len(clusters)} | Topic: {cluster.get('topic_name')}")
        result = generate_strategy(cluster)
        results.append(result)
        
        # Tactical delay to avoid rate limits
        if i < len(clusters) - 1:
            time.sleep(1)
    
    # Sort by opportunity score descending — highest alpha first
    results.sort(key=lambda x: x.get("opportunity_score", 0.0), reverse=True)
    
    logger.info(f"Strategy generation complete. {len(results)} actionable clusters ready.")
    return results
