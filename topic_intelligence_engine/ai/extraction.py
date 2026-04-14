import json
import re
from typing import List, Dict, Any
from core.logger import logger
from ai.llm_client import call_llm_json

def build_prompt(post: Dict[str, Any]) -> str:
    """
    Constructs a highly constrained, strict prompt to prevent hallucinations.
    Forces deterministic formatting and explicitly forbids vague outputs.
    """
    clean_text = post.get("clean_text") or post.get("text", "")
    
    prompt = f"""You are extracting a topic from a social media post.

Your task:
Return a SHORT topic describing the main idea.

RULES:

* 3 to 5 words only
* Must be human-readable
* Do NOT copy the sentence
* Do NOT include filler words like "comment", "dm", "link"
* Focus on the core idea only

---

INPUT:
{clean_text}

---

OUTPUT FORMAT (STRICT):

{{
"extracted_topic": "..."
}}
"""
    return prompt

def extract_json_safely(response: Any, post_id: Any) -> Dict[str, Any]:
    """
    Safely extract JSON from the LLM response, handling both raw dicts and strings.
    Strips out conversational LLM text before/after the actual JSON block using non-greedy regex.
    """
    try:
        if isinstance(response, dict):
            parsed = response
        elif isinstance(response, str):
            try:
                parsed = json.loads(response)
            except json.JSONDecodeError:
                match = re.search(r'\{.*\}', response, re.DOTALL)
                if match:
                    parsed = json.loads(match.group())
                else:
                    raise ValueError("Could not parse JSON from string")
        else:
            raise ValueError(f"Invalid LLM response type: {type(response)}")

        # Ensure extracted_topic is always present
        parsed["extracted_topic"] = parsed.get("extracted_topic", "unknown topic")
        return parsed
            
    except Exception as e:
        # 6. Improve Error Logging
        logger.error(f"Extraction parsing failed | post_id={post_id} | error={str(e)} | raw={response}")
        
        # 2. Safe Fallback Structure
        return {
            "extracted_topic": "unknown_topic",
            "health_claims": [],
            "hook_type": "unknown",
            "content_format": "unknown",
            "extraction_status": "failed" # 5. Add Extraction Status Flag
        }

def normalize_extraction(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalizes the returned JSON payload, coercing invalid enums and truncating words.
    """
    # 4. Strict Topic Cleaning -> Lowercase, stripped, remove prefixes
    topic = str(raw_data.get("extracted_topic", "unknown_topic")).lower().strip()
    
    for prefix in ["this post talks about", "discussion on", "the topic is"]:
        if topic.startswith(prefix):
            topic = topic.replace(prefix, "").strip()
            
    topic = re.sub(r'\b(issue|problem|discussion|thing)\b', '', topic)
    
    # 4. Remove fillers and usernames
    topic = re.sub(r'\b(comment|dm|link|episode)\b', '', topic)
    topic = re.sub(r'@[a-zA-Z0-9_.]+', '', topic) # Remove usernames
    
    topic = re.sub(r'\s+', ' ', topic).strip()
            
    words = topic.split()
    if len(words) < 2:
        topic = "unknown_topic"
    else:
        topic = " ".join(words[:6])

    # 3. Deterministic Claims Deduplication -> Ordered deduplication
    raw_claims = raw_data.get("health_claims", [])
    if not isinstance(raw_claims, list):
        raw_claims = []
    
    clean_claims = []
    seen = set()
    for c in raw_claims:
        if isinstance(c, str):
            c_clean = c.strip()
            if c_clean and c_clean not in seen:
                seen.add(c_clean)
                clean_claims.append(c_clean)
                
    clean_claims = [c[:120] for c in clean_claims]
    
    # Enum Validation
    valid_hooks = {"fear", "curiosity", "authority", "story", "contrarian"}
    valid_formats = {"educational", "myth_busting", "personal_story", "listicle", "opinion"}
    
    hook = str(raw_data.get("hook_type", "unknown")).lower().strip()
    fmt = str(raw_data.get("content_format", "unknown")).lower().strip()
    
    return {
        "extracted_topic": topic,
        "health_claims": clean_claims,
        "hook_type": hook if hook in valid_hooks else "unknown",
        "content_format": fmt if fmt in valid_formats else "unknown"
    }

def extract_post(post: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes a single post through the hardened pipeline.
    """
    enriched_post = post.copy()
    post_id = enriched_post.get("external_post_id", "UNKNOWN")
    
    prompt = build_prompt(post)
    
    try:
        # call_llm_json handles retry and temperature=0 determinism
        llm_response_raw = call_llm_json(prompt)
        
        # 1. Safely extract JSON from conversational wrappers
        extracted_dict = extract_json_safely(llm_response_raw, post_id)
        
        if extracted_dict.get("extraction_status") == "failed":
            final_data = extracted_dict
        else:
            final_data = normalize_extraction(extracted_dict)
            final_data["extraction_status"] = "success" # 5. Add Extraction Status Flag
            
        # Extract clean text for fallbacks
        clean_text = str(post.get("clean_text") or post.get("text", ""))
        
        # Append to enriched packet first
        enriched_post.update(final_data)
        
        # --- FINAL TOPIC CLEANING (runs AFTER LLM response, BEFORE final write) ---
        # Must override enriched_post directly to guarantee no bypass via any code path
        topic = enriched_post.get("extracted_topic", "")
        
        topic = topic.lower().strip()

        # Remove punctuation
        topic = re.sub(r'[^\w\s]', '', topic)

        # Remove filler + weak words
        remove_words = [
            "the", "a", "an", "and", "or", "for", "with", "to", "of", "in",
            "is", "are", "was", "were", "be", "being", "been",
            "improves", "improve", "improving", "reduces", "reduce", "reducing",
            "enhances", "enhance", "enhancing", "helps", "help", "helping",
            "makes", "make", "making"
        ]
        words = [w for w in topic.split() if w not in remove_words]

        # Keep only first 3–4 meaningful words
        topic = " ".join(words[:4])

        if len(topic.split()) < 2:
            topic = clean_text.split()[:3]
            topic = " ".join(topic)
            
        enriched_post["extracted_topic"] = topic
            
        # 2. ADD DEBUG LOG
        logger.info(f"EXTRACTED TOPIC: {topic}")
        
    except Exception as e:
        logger.error(f"Extraction failed | post_id={post_id} | error={str(e)}")
        raise e  # Let extract_batch handle the skipping
        
    return enriched_post

def extract_batch(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Iterates sequentially over posts. 
    """
    logger.info(f"Starting AI Intelligence Extraction for {len(posts)} posts...")
    enriched_batch = []
    
    for i, post in enumerate(posts):
        try:
            logger.debug(f"Extracting post {i+1}/{len(posts)}")
            enriched = extract_post(post)
            enriched_batch.append(enriched)
        except Exception as e:
            logger.error(f"Extraction failed for post: {e}")
            continue  # skip failed post but DO NOT crash entire pipeline
            
    logger.info(f"Successfully completed AI Extraction for {len(enriched_batch)} posts.")
    return enriched_batch
