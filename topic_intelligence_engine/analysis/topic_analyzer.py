import json
import re
from typing import List, Dict, Any
from core.logger import logger
from ai.llm_client import call_llm_json

def compute_controversy(cluster: Dict[str, Any]) -> float:
    """
    Computes a deterministic controversy proxy based on the comment-to-like ratio.
    Higher comments relative to likes = higher controversy/debate.
    Normalizes between 0.0 and 1.0.
    """
    reps = cluster.get("representative_posts", [])
    if not reps:
        return 0.0
        
    total_comments = sum(p.get("comments", 0) for p in reps)
    total_likes = sum(p.get("likes", 0) for p in reps)
    
    if total_likes + total_comments < 10:
        return 0.0
        
    # Proxy equation: Comments usually represent debate. 
    # A standard post might have 5% comments to likes. 
    # If comments are 50% or more of likes, it's highly controversial.
    ratio = total_comments / max(total_likes, 1)
    
    # Normalize: ratio of 0.5+ becomes 1.0 controversy.
    controversy = min(ratio * 2.0, 1.0)
    
    return round(float(controversy), 4)

def build_analysis_prompt(cluster: Dict[str, Any]) -> str:
    """
    Constructs a rigid, deeply contextual prompt that forces the LLM to extract
    actionable, strategic psychological triggers instead of generic observations.
    """
    topic_name = cluster.get("topic_name", "Unknown Topic")
    reps = cluster.get("representative_posts", [])
    total_posts = cluster.get("total_posts", 0)
    avg_momentum = cluster.get("avg_momentum", 0.0)
    trend_stage = cluster.get("trend_stage", "unknown")
    controversy = cluster.get("controversy_level", 0.0)
    
    # Compile the 300-char truncated contexts
    text_context = "\n\n".join([f"[Score: {round(p.get('weighted_score', 0), 1)}] {p.get('clean_text', '')}" for p in reps])
    
    prompt = f"""You are a senior behavioral data scientist.
Analyze the following social media trend cluster to uncover deep psychological drivers and strategic intelligence.
Do NOT use vague phrases like "people like this" or "it is popular". Be hyper-specific and grounded in the provided text.

CLUSTER TOPIC: "{topic_name}"

CLUSTER METRICS:

total_posts: {total_posts}
avg_momentum: {avg_momentum}
trend_intensity: {round(avg_momentum * total_posts, 2)}
trend_stage: {trend_stage}
controversy_level: {controversy}

REPRESENTATIVE POSTS (Context):
{text_context}

Provide insights based EXACTLY on the texts above. Identify the following fields:
1. "why_trending": Explain WHY this is trending, not WHAT it is. Identify what people are reacting to, what problem is being discussed, and what insight is driving engagement.
   BAD: "Trending based on discussions like sunlight exposure..."
   GOOD: "Growing interest in optimizing circadian rhythm and mental health through natural sunlight exposure."
2. "trigger_event": What is the likely catalyst or catalyst-content causing this? (e.g., "Spike in anecdotal horror stories")
3. "audience_psychology": What exact emotional driver is present? (Fear, aspiration, validation, confusion?)
4. "content_pattern": What exact content style is winning here? (e.g., "Personal experience posts questioning popular advice")
5. "format_pattern": What is the structural format? (e.g., "Story-driven posts with strong emotional hooks")

OUTPUT FORMAT:
Return ONLY a valid JSON object matching this exact schema:
{{
    "why_trending": "...",
    "trigger_event": "...",
    "audience_psychology": "...",
    "content_pattern": "...",
    "format_pattern": "...",
    "validation_status": "verified | uncertain | misinformation"
}}
"""
    return prompt


def analyze_cluster(cluster: Dict[str, Any]) -> Dict[str, Any]:
    """
    Runs hybrid intelligence combining deterministic math (controversy) 
    with LLM semantic inference. Additively enriches the cluster object.
    """
    enriched_cluster = cluster.copy()
    c_id = enriched_cluster.get("cluster_id", "UNKNOWN_ID")
    
    # 1. Deterministic Code Path
    controversy = compute_controversy(cluster)
    confidence_score = round(
        cluster.get("avg_weighted_score", 0.0) * len(cluster.get("representative_posts", [])),
        4
    )
    enriched_cluster["analysis_confidence"] = confidence_score
    
    threshold = 250.0 # Set arbitrary base heuristic threshold for structural sorting
    enriched_cluster["analysis_type"] = (
        "high_confidence" if confidence_score > threshold else "low_confidence"
    )
    
    # Dynamic Fallback for why_trending
    reps = cluster.get("representative_posts", [])
    top_2_texts = [p.get("clean_text", "")[:60].strip() for p in reps[:2] if p.get("clean_text")]
    if top_2_texts:
        dynamic_why_trending = f"Driven by high engagement and clustering around '{cluster.get('topic_name', 'this topic')}'-related terminology."
    else:
        dynamic_why_trending = "Driven by an intense velocity spike in contextual keywords."

    # Fallback default struct
    fallback = {
        "why_trending": dynamic_why_trending,
        "trigger_event": "Unknown trigger.",
        "audience_psychology": "Unknown psychology.",
        "content_pattern": "Unknown.",
        "format_pattern": "Unknown."
    }
    
    # 2. LLM Inference Path
    if not reps:
        logger.debug(f"Skipping LLM analysis for {c_id}: No representative posts.")
        enriched_cluster.update(fallback)
        return enriched_cluster
        
    prompt = build_analysis_prompt(enriched_cluster)
    
    topic_name = enriched_cluster.get("topic_name", "Unknown Topic")

    try:
        # call_llm_json natively handles tenacity @retry and temperature=0 determinism
        raw_response = call_llm_json(prompt)
        
        try:
            if isinstance(raw_response, dict):
                parsed = raw_response
            else:
                # Still strip common LLM wrapper as it fails json.loads trivially
                clean_response = str(raw_response).strip()
                if clean_response.startswith("```json"):
                    clean_response = clean_response.replace("```json", "").replace("```", "").strip()
                parsed = json.loads(clean_response)
        except Exception:
            logger.error(f"Analysis parsing failed: {raw_response}")
            # HARD FALLBACK
            parsed = {
                "why_trending": f"Rising discussion around {topic_name}",
                "audience_psychology": "curiosity",
                "content_pattern": "educational",
                "recommended_action": "explore content"
            }
            
        # Guard: ensure critical fields are never empty
        if not parsed.get("why_trending"):
            parsed["why_trending"] = f"Increasing discussion and interest around {topic_name}"
            
        if not parsed.get("audience_psychology"):
            parsed["audience_psychology"] = "curiosity"
            
        if not parsed.get("validation_status"):
            parsed["validation_status"] = "uncertain"
            
        enriched_cluster.update(parsed)
        logger.info(f"ANALYSIS OUTPUT: {parsed}")
            
    except Exception as e:
        logger.error(f"Topic Analysis failed for {c_id}: {str(e)}")
        # Complete failure fallback
        parsed = {
            "why_trending": f"Rising discussion around {topic_name}",
            "audience_psychology": "curiosity",
            "content_pattern": "educational",
            "recommended_action": "explore content"
        }
        enriched_cluster.update(parsed)
        logger.info(f"ANALYSIS OUTPUT (fallback): {parsed}")
        
    return enriched_cluster

def analyze_batch(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Iterates through the clustered arrays sequentially to enrich them with strategic intelligence.
    """
    logger.info(f"Starting Strategic Analysis on {len(clusters)} clusters...")
    analyzed_batch = []
    
    for i, cluster in enumerate(clusters):
        logger.debug(f"Analyzing Cluster {i+1}/{len(clusters)} | Topic: {cluster.get('topic_name')}")
        analyzed = analyze_cluster(cluster)
        analyzed_batch.append(analyzed)
        
    logger.info(f"Successfully completed analysis for {len(analyzed_batch)} clusters.")
    return analyzed_batch
