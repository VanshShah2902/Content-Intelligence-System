from typing import List, Dict, Any
from core.logger import logger
from config.settings import settings

def validate_posts(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Ensures only valid, usable data enters the pipeline.
    Removes empty text, low engagement, malformed objects,
    and normalizes numeric fields.
    """
    valid_posts = []
    dropped_counts = {"empty_text": 0, "low_engagement": 0, "missing_fields": 0}

    required_keys = {"external_post_id", "platform", "creator_id", "text", "likes", "comments", "timestamp"}

    for post in posts:
        # 1. Check required fields
        if not required_keys.issubset(post.keys()):
            dropped_counts["missing_fields"] += 1
            continue

        # 2. Normalize numeric fields (critical)
        try:
            post["likes"] = int(post.get("likes", 0) or 0)
            post["comments"] = int(post.get("comments", 0) or 0)
        except Exception:
            dropped_counts["missing_fields"] += 1
            continue

        # 3. Validate text
        if not post["text"] or not str(post["text"]).strip():
            dropped_counts["empty_text"] += 1
            continue

        # 4. Engagement threshold
        total_engagement = post["likes"] + post["comments"]
        if total_engagement < settings.min_engagement_threshold:
            dropped_counts["low_engagement"] += 1
            continue

        valid_posts.append(post)

    logger.info(
        f"Validation Complete | Passed: {len(valid_posts)} | "
        f"Dropped (Empty: {dropped_counts['empty_text']}, "
        f"Low Eng: {dropped_counts['low_engagement']}, "
        f"Malformed: {dropped_counts['missing_fields']})"
    )

    return valid_posts
