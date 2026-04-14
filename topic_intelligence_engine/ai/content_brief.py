import json
from typing import List, Dict, Any
from core.logger import logger
from ai.llm_client import call_llm_json

def build_brief_prompt(topic_name: str, top_posts_text: str, why_trending: str) -> str:
    """
    Constructs the high-quality content brief prompt using the specialist creative template.
    """
    return f"""You are generating a high-quality content brief for creators based on trending topic data.

INPUT:

Topic: {topic_name}

Top Posts (Context):
{top_posts_text}

Why Trending (Psychological Driver):
{why_trending}

TASK:

Generate a structured content brief.

OUTPUT FORMAT (STRICT JSON):

{{
"angles": [
"3-5 distinct content angles"
],
"hooks": [
"5 short, high-impact hooks (1 line each)"
],
"formats": [
"3 content formats (reel, carousel, talking head, etc.)"
],
"talking_points": [
"4-6 key points creators should cover"
]
}}

RULES:

* Hooks must be punchy and scroll-stopping
* Angles must be meaningfully different
* Talking points must be specific, not generic
* Avoid vague phrases like "improve health"
* Use language similar to social media tone

Return ONLY valid JSON.
"""

def generate_content_brief(cluster: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preps input and calls the LLM to generate a specialized content brief.
    """
    topic_name = cluster.get("topic_name", "Unknown Topic")
    why_trending = cluster.get("why_trending", "Driven by recent viral interest.")
    
    # Prep top posts (top 3 for context)
    posts = cluster.get("posts", [])
    top_posts_text = "\n---\n".join([p.get("text", "")[:300] for p in posts[:3]])
    
    prompt = build_brief_prompt(topic_name, top_posts_text, why_trending)
    
    fallback = {
        "angles": ["Educational overview", "Personal experience share"],
        "hooks": [f"Why everyone is talking about {topic_name} right now."],
        "formats": ["Talking head video", "Educational carousel"],
        "talking_points": ["Understand the basic science", "Common myths to avoid"]
    }
    
    try:
        raw_response = call_llm_json(prompt)
        if isinstance(raw_response, dict):
            return raw_response
        return json.loads(str(raw_response))
    except Exception as e:
        logger.error(f"High-quality brief generation failed for {topic_name}: {e}")
        return fallback

def generate_brief_batch(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyzes a batch of clusters to attach creative content briefs.
    """
    import time
    logger.info(f"Generating high-quality content briefs for {len(clusters)} clusters...")
    enriched_clusters = []
    
    for i, cluster in enumerate(clusters):
        brief = generate_content_brief(cluster)
        cluster["content_brief"] = brief
        enriched_clusters.append(cluster)
        
        # Tactical delay to avoid rate limits
        if i < len(clusters) - 1:
            time.sleep(1)
            
    return enriched_clusters
