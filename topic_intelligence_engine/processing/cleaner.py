import re
from typing import List, Dict, Any

def clean_text(text: str) -> str:
    """
    Normalize text while preserving meaning.
    """
    if not isinstance(text, str):
        return ""

    cleaned = text.lower()

    # Remove URLs
    cleaned = re.sub(r'http\S+|www\.\S+', '', cleaned)

    # Remove weird unicode/control chars
    cleaned = re.sub(r'[^\x00-\x7F]+', ' ', cleaned)

    # Normalize punctuation (!!! -> !)
    cleaned = re.sub(r'([!?.]){2,}', r'\1', cleaned)

    # Normalize whitespace
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)

    return cleaned.strip()

def apply_cleaning(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    cleaned_posts = []

    for post in posts:
        enriched_post = post.copy()
        enriched_post["clean_text"] = clean_text(post.get("text", ""))
        cleaned_posts.append(enriched_post)

    return cleaned_posts
