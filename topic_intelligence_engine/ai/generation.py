import json
from typing import Dict, Any, List
from core.logger import logger
from ai.llm_client import call_llm_json


def build_generation_prompt(topic_name: str, cluster: Dict[str, Any]) -> str:
    why_trending = cluster.get("why_trending", "")
    audience = cluster.get("audience_psychology", "")
    content_pattern = cluster.get("content_pattern", "")

    prompt = f"""You are a senior content strategist.
Generate a content brief for creators based on the following trend intelligence.

TOPIC: {topic_name}
WHY TRENDING: {why_trending}
AUDIENCE PSYCHOLOGY: {audience}
CONTENT PATTERN: {content_pattern}

Return ONLY a valid JSON object. No markdown, no extra text.
{{
    "angles": ["Angle 1", "Angle 2", "Angle 3"],
    "hooks": ["Hook 1", "Hook 2", "Hook 3"],
    "talking_points": ["Point 1", "Point 2", "Point 3"],
    "tone_guidance": "educational"
}}
"""
    return prompt


def generate_content_brief(cluster: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generates a content brief for a cluster.
    Always returns a non-empty content_brief — never fails.
    """
    enriched = cluster.copy()
    topic_name = cluster.get("topic_name", "this topic")

    # Dynamic fallback using topic_name
    fallback_brief = {
        "angles": [
            f"Why {topic_name} matters",
            f"Mistakes people make about {topic_name}"
        ],
        "hooks": [
            f"This is why {topic_name} is trending",
            f"You are doing this wrong: {topic_name}"
        ],
        "talking_points": [
            f"Explain {topic_name} simply",
            "Common misconceptions"
        ],
        "tone_guidance": "educational"
    }

    prompt = build_generation_prompt(topic_name, cluster)

    try:
        response = call_llm_json(prompt)

        try:
            if isinstance(response, dict):
                parsed = response
            else:
                parsed = json.loads(response)
        except Exception:
            logger.warning("Primary parse failed. Attempting cleanup")
            try:
                start = response.find("{")
                end = response.rfind("}") + 1
                parsed = json.loads(response[start:end])
            except Exception:
                logger.error(f"Final parse failed: {response}")
                parsed = {
                    "angles": [f"Why {topic_name} matters"],
                    "hooks": [f"This is trending: {topic_name}"],
                    "talking_points": [f"Explain {topic_name} simply"],
                    "tone_guidance": "educational"
                }

        # Guard: ensure no field is empty or missing
        for key, fallback_val in fallback_brief.items():
            if not parsed.get(key):
                parsed[key] = fallback_val

        enriched["content_brief"] = parsed
        logger.info(f"GENERATION OUTPUT [{topic_name}]: {parsed}")

    except Exception as e:
        logger.error(f"Generation stage failed for '{topic_name}': {str(e)}")
        enriched["content_brief"] = fallback_brief

    return enriched


def generate_batch(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Runs content brief generation across a batch of clusters.
    """
    logger.info(f"Generating content briefs for {len(clusters)} clusters.")
    results = []
    for i, cluster in enumerate(clusters):
        logger.debug(f"Generating brief {i+1}/{len(clusters)} | Topic: {cluster.get('topic_name')}")
        result = generate_content_brief(cluster)
        results.append(result)
    return results
