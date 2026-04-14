import json
import re
from typing import List, Dict, Any
from core.logger import logger
from ai.llm_client import call_llm_json

# Valid output statuses
VALID_STATUSES = {"valid", "misinformation", "uncertain"}

# Fallback used when LLM fails or returns malformed data
FALLBACK_CLAIM = {
    "status": "uncertain",
    "confidence": 0.3,
    "reasoning": "Unable to evaluate claim due to LLM failure.",
    "source_hint": "No source available"
}


def build_validation_prompt(claim: str, topic_context: str = "") -> str:
    """
    Builds a strict, grounded validation prompt for a single health claim.
    Forces the LLM to use general scientific consensus rather than hallucinating specific studies.
    """
    context_str = f"\nTOPIC CONTEXT: {topic_context}\n" if topic_context else ""
    return f"""You are a senior biomedical fact-checker.
Evaluate the following health claim for scientific accuracy.
{context_str}
CLAIM: "{claim}"

INSTRUCTIONS:
- Base your evaluation ONLY on general scientific consensus from authoritative bodies: NIH, WHO, Mayo Clinic, PubMed.
- Do NOT invent specific study names, trial numbers, or paper authors.
- Do NOT evaluate the claim based on popularity or social media trends.
- Be concise but precise in your reasoning.

CLASSIFICATION RULES:
- "valid": claim is supported by scientific evidence or general medical consensus.
- "misinformation": claim is false, misleading, or relies on pseudoscience.
- "uncertain": claim has mixed evidence, insufficient research, or is genuinely controversial.

CONFIDENCE SCORE:
- 0.0 to 1.0 — reflects the strength and clarity of evidence, not certainty of the model.

SOURCE HINT (do NOT fabricate specific studies):
- Use authority names only: "NIH consensus", "WHO guidance", "Mayo Clinic guidance", "No clinical evidence found", etc.

OUTPUT FORMAT:
Return ONLY a valid JSON object in this exact structure:
{{
    "status": "valid" | "misinformation" | "uncertain",
    "confidence": 0.0,
    "reasoning": "...",
    "source_hint": "..."
}}
"""


def _extract_json(raw_text: str, claim: str) -> Dict[str, Any]:
    """
    Multi-match regex JSON extractor with clean fallback.
    """
    try:
        matches = re.findall(r'\{.*?\}', str(raw_text), re.DOTALL)
        if matches:
            return json.loads(matches[0])
        return json.loads(raw_text)
    except Exception as e:
        logger.error(f"Claim validation JSON parse error for '{claim[:50]}...': {e}")
        return {}


def validate_claim(claim: str, topic_context: str = "") -> Dict[str, Any]:
    """
    Validates a single health claim using the LLM.
    Returns a structured result with status, confidence, reasoning, and source_hint.
    """
    prompt = build_validation_prompt(claim, topic_context)
    result = {"claim": claim}

    try:
        raw = call_llm_json(prompt)
        parsed = _extract_json(raw, claim)

        # Validate LLM output fields before accepting
        status = str(parsed.get("status", "uncertain")).strip().lower()
        if status not in VALID_STATUSES:
            logger.warning(f"Invalid status '{status}' for claim: '{claim[:50]}'. Defaulting to 'uncertain'.")
            status = "uncertain"

        confidence = float(parsed.get("confidence", 0.3))
        confidence = max(0.0, min(1.0, confidence))  # Clamp 0–1

        reasoning = str(parsed.get("reasoning", FALLBACK_CLAIM["reasoning"])).strip()
        source_hint = str(parsed.get("source_hint", FALLBACK_CLAIM["source_hint"])).strip()

        # Guard against too-short or generic responses
        if len(reasoning.split()) < 4:
            reasoning = FALLBACK_CLAIM["reasoning"]

        result.update({
            "status": status,
            "confidence": round(confidence, 4),
            "reasoning": reasoning,
            "source_hint": source_hint
        })

    except Exception as e:
        logger.error(f"Claim validation failed for '{claim[:50]}': {e}")
        result.update(FALLBACK_CLAIM)

    return result


def validate_cluster(cluster: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validates all health claims in a single cluster.
    Deduplicates claims before validation to avoid redundant LLM calls.
    Additively enriches the cluster with 'validated_claims'.
    """
    enriched = cluster.copy()
    c_id = enriched.get("cluster_id", "UNKNOWN")
    raw_claims = enriched.get("health_claims", [])

    if not raw_claims:
        logger.debug(f"[{c_id}] No health claims to validate.")
        enriched["validated_claims"] = []
        return enriched

    # Deduplication: lowercase stripped set preserves order via dict
    seen = {}
    for c in raw_claims:
        key = c.strip().lower()
        if key and key not in seen:
            seen[key] = c.strip()  # preserve original casing

    unique_claims = list(seen.values())
    logger.info(f"[{c_id}] Validating {len(unique_claims)} unique claims (raw: {len(raw_claims)}).")

    validated = []
    valid_count = 0
    misinfo_count = 0
    uncertain_count = 0
    total_conf = 0.0

    for claim in unique_claims:
        result = validate_claim(claim, topic_context=enriched.get("topic_name", ""))
        validated.append(result)
        
        status = result.get("status")
        if status == "valid": valid_count += 1
        elif status == "misinformation": misinfo_count += 1
        elif status == "uncertain": uncertain_count += 1
        
        total_conf += result.get("confidence", 0.0)

        logger.debug(f"[{c_id}] Claim: '{claim[:50]}' → {result['status']} ({result['confidence']})")

    avg_conf = total_conf / len(unique_claims) if unique_claims else 0.0

    enriched["validated_claims"] = validated
    
    enriched["validation_summary"] = {
        "valid": valid_count,
        "misinformation": misinfo_count,
        "uncertain": uncertain_count,
        "total": len(unique_claims)
    }
    enriched["is_myth_opportunity"] = misinfo_count > 0
    enriched["validation_confidence"] = round(avg_conf, 4)

    return enriched


def validate_batch(clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Runs claim validation across all clusters sequentially.
    Additively enriches each cluster with validated_claims.
    """
    logger.info(f"Claim validation started for {len(clusters)} clusters.")
    results = []

    for i, cluster in enumerate(clusters):
        logger.debug(f"Validating cluster {i+1}/{len(clusters)} | Topic: {cluster.get('topic_name')}")
        validated = validate_cluster(cluster)
        results.append(validated)

    misinformation_count = sum(
        1 for c in results
        for v in c.get("validated_claims", [])
        if v.get("status") == "misinformation"
    )
    logger.info(
        f"Claim validation complete. "
        f"Clusters: {len(results)} | Misinformation flags: {misinformation_count}"
    )
    return results
