import praw
from praw.exceptions import PRAWException
from datetime import datetime
from typing import List, Dict, Any, Optional

from config.settings import settings
from core.logger import logger

def get_reddit_client() -> praw.Reddit:
    """Initializes and returns the PRAW Reddit client."""
    try:
        return praw.Reddit(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent
        )
    except Exception as e:
        logger.error(f"Failed to initialize Reddit client: {str(e)}")
        raise

def normalize_post(submission: Any) -> Optional[Dict[str, Any]]:
    """
    Normalizes a PRAW submission object into the strict target dictionary format.
    Combines title and selftext for full context. 
    """
    try:
        # Avoid deleted authors
        creator_id = submission.author.name if submission.author else "deleted_user"
        
        # Combine title and body text safely
        title = submission.title or ""
        body = submission.selftext or ""
        text = f"{title}\n\n{body}".strip()
        
        # Convert timestamp to ISO 8601 string
        dt = datetime.utcfromtimestamp(submission.created_utc)
        iso_timestamp = dt.isoformat() + "Z"

        return {
            "external_post_id": submission.id,
            "platform": "reddit",
            "creator_id": creator_id,
            "text": text,
            "likes": submission.score,  # Reddit uses 'score' for net upvotes
            "comments": submission.num_comments,
            "timestamp": iso_timestamp
        }
    except Exception as e:
        logger.warning(f"Failed to normalize Reddit post {getattr(submission, 'id', 'UNKNOWN')}: {str(e)}")
        return None

def fetch_posts(reddit: praw.Reddit, subreddit_name: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Fetches and normalizes 'hot' posts from a specific subreddit."""
    logger.info(f"Fetching top {limit} hot posts from r/{subreddit_name}")
    normalized_posts = []
    
    try:
        subreddit = reddit.subreddit(subreddit_name)
        # We target 'hot' to capture current momentum, but 'top' (daily) is also viable
        for submission in subreddit.hot(limit=limit):
            # Skip megathreads/stickied posts if desired, but here we ingest all
            doc = normalize_post(submission)
            if doc:
                normalized_posts.append(doc)
                
        logger.info(f"Successfully fetched {len(normalized_posts)} posts from r/{subreddit_name}")
        
    except PRAWException as e:
        logger.error(f"PRAW API Error while fetching from r/{subreddit_name}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error while fetching from r/{subreddit_name}: {str(e)}")
        
    return normalized_posts

def run_ingestion() -> List[Dict[str, Any]]:
    """
    Main entry point for Reddit ingestion.
    Iterates through configured targets, avoids duplicates, and returns unified list.
    """
    logger.info("Starting Reddit Ingestion Pipeline...")
    all_posts = []
    seen_ids = set() # To strictly avoid duplicates across subreddits
    
    try:
        reddit = get_reddit_client()
        
        for subreddit in settings.target_subreddits:
            sub_posts = fetch_posts(reddit, subreddit, limit=settings.reddit_fetch_limit)
            
            for post in sub_posts:
                if post["external_post_id"] not in seen_ids:
                    all_posts.append(post)
                    seen_ids.add(post["external_post_id"])
                else:
                    logger.debug(f"Skipping duplicate post {post['external_post_id']}")
                    
    except Exception as e:
        logger.error(f"Fatal error during Reddit ingestion run: {str(e)}")
        
    logger.info(f"Reddit ingestion complete. Total unique posts retrieved: {len(all_posts)}")
    return all_posts

if __name__ == "__main__":
    # Example local runtime for testing
    import json
    
    print("Testing Reddit Ingestion... (Ensure credentials are set in .env)")
    results = run_ingestion()
    if results:
        print("\nExample Output:")
        print(json.dumps(results[0], indent=2))
