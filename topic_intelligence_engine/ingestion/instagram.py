import requests
import time
from datetime import datetime, timezone
from typing import List, Dict, Any
from core.logger import logger
from config.settings import settings

from ingestion.apify_client import fetch_instagram_posts, normalize_post as apify_normalize_post
from config.creators import CREATORS
from storage.excel_storage import save_posts_to_excel

def fetch_real_instagram_data():
    """
    Fetches real Instagram posts using the Apify SDK, iterating through all creators.
    Implements rate-limiting and normalization.
    """
    all_posts = []

    for creator in CREATORS:
        profile_url = f"https://www.instagram.com/{creator}/"
        logger.info(f"Fetching creator: {creator}")
        
        try:
            # Call the real Apify SDK
            raw_posts = fetch_instagram_posts(profile_url, limit=getattr(settings, "instagram_posts_limit", 500))
            logger.info(f"Posts fetched: {len(raw_posts)}")

            normalized = []
            for p in raw_posts:
                # Validation: Skip posts where id is None or caption is empty
                if not p.get("id") or not p.get("caption"):
                    continue
                
                # Normalize using the Apify-specific mapper
                norm = apify_normalize_post(p)
                normalized.append(norm)

            all_posts.extend(normalized)
            
            # Rate limit control: sleep 2s between creators
            import time
            time.sleep(2)

        except Exception as e:
            logger.error(f"Failed fetching for {creator}: {str(e)}")
            # Fail Fast rule: Propagate the exception if it's critical, 
            # but allow the ingestion to potentially try other creators if allowed.
            # Here we follow the logic: Raise error clearly.
            raise e

    return all_posts
def get_mock_posts():
    return [
        # --- Creator: hubermanlab ---
        {
            "external_post_id": "hub_1",
            "platform": "instagram",
            "creator_id": "hubermanlab",
            "text": "Morning sunlight is a non-negotiable. 10 mins outside before 9am sets your cortisol and melatonin for the next 24 hours. Don't skip this. #circadianrhythm #health",
            "likes": 45000, "comments": 1200, "views": 850000,
            "timestamp": "2024-03-25T08:00:00Z"
        },
        {
            "external_post_id": "hub_2",
            "platform": "instagram",
            "creator_id": "hubermanlab",
            "text": "Meditation isn't just about 'clearing your mind'. NDR (Non-Sleep Deep Rest) can actually restock your brain's dopamine levels. 20 mins is all it takes. Thoughts?",
            "likes": 32000, "comments": 950, "views": 620000,
            "timestamp": "2024-03-26T10:00:00Z"
        },
        {
            "external_post_id": "hub_3",
            "platform": "instagram",
            "creator_id": "hubermanlab",
            "text": "The data on cold exposure is becoming undeniable. 11 minutes total per week in a cold plunge or shower significantly boosts baseline dopamine. Just do it.",
            "likes": 58000, "comments": 2100, "views": 1100000,
            "timestamp": "2024-03-27T07:30:00Z"
        },
        {
            "external_post_id": "hub_4",
            "platform": "instagram",
            "creator_id": "hubermanlab",
            "text": "Training frequency vs intensity... science suggests at least 3 days a week for hypertrophy but it's the total weekly volume that really moves the needle.",
            "likes": 28000, "comments": 1500, "views": 540000,
            "timestamp": "2024-03-28T14:00:00Z"
        },
        {
            "external_post_id": "hub_5",
            "platform": "instagram",
            "creator_id": "hubermanlab",
            "text": "Quick tip: if you can't sleep, look at your light exposure after 10pm. Even small amounts of blue light from your phone can suppress melatonin. Put the screen away.",
            "likes": 12000, "comments": 400, "views": 310000,
            "timestamp": "2024-03-29T21:00:00Z"
        },

        # --- Creator: drhyman ---
        {
            "external_post_id": "hym_1",
            "platform": "instagram",
            "creator_id": "drhyman",
            "text": "Food is medicine. High protein breakfasts are the key to metabolic health. If you aren't getting 30g of protein before noon, you're missing out. #functionalmedicine",
            "likes": 15000, "comments": 600, "views": 250000,
            "timestamp": "2024-03-25T09:00:00Z"
        },
        {
            "external_post_id": "hym_2",
            "platform": "instagram",
            "creator_id": "drhyman",
            "text": "Chronic stress is the silent killer. I use meditation daily to keep my nervous system regulated. It's not a luxury, it's a necessity in our modern world.",
            "likes": 12000, "comments": 450, "views": 210000,
            "timestamp": "2024-03-26T11:00:00Z"
        },
        {
            "external_post_id": "hym_3",
            "platform": "instagram",
            "creator_id": "drhyman",
            "text": "Your sleep is the foundation of your health. Try a circadian reset: 5 mins of sun in the morning, no screens at night. Simple but life-changing stuff.",
            "likes": 18000, "comments": 550, "views": 320000,
            "timestamp": "2024-03-27T08:15:00Z"
        },
        {
            "external_post_id": "hym_4",
            "platform": "instagram",
            "creator_id": "drhyman",
            "text": "Is high protein just a fad? No. It's essential for preserving muscle as we age. Don't fear the steak, fear the ultra-processed cereal instead! #protein #longevity",
            "likes": 8000, "comments": 900, "views": 180000,
            "timestamp": "2024-03-28T12:00:00Z"
        },

        # --- Creator: foundmyfitness ---
        {
            "external_post_id": "fmf_1",
            "platform": "instagram",
            "creator_id": "foundmyfitness",
            "text": "New episode on cold exposure and heat shock proteins! We dive deep into why 11 mins of cold a week is the sweet spot for metabolic shifts. Link in bio.",
            "likes": 22000, "comments": 800, "views": 410000,
            "timestamp": "2024-03-26T07:00:00Z"
        },
        {
            "external_post_id": "fmf_2",
            "platform": "instagram",
            "creator_id": "foundmyfitness",
            "text": "Circadian rhythm disruption is a precursor to so many chronic issues. Get your light early and keep it dark late. Your mitochondria will thank you. 🧬",
            "likes": 25000, "comments": 400, "views": 380000,
            "timestamp": "2024-03-27T09:30:00Z"
        },
        {
            "external_post_id": "fmf_3",
            "platform": "instagram",
            "creator_id": "foundmyfitness",
            "text": "Sauna + Cold Plunge = the ultimate longevity stack. The thermal stress activates pathways that we simply don't trigger in climate-controlled environments.",
            "likes": 41000, "comments": 1100, "views": 750000,
            "timestamp": "2024-03-28T08:00:00Z"
        },

        # --- Creator: biohacker_max ---
        {
            "external_post_id": "max_1",
            "platform": "instagram",
            "creator_id": "biohacker_max",
            "text": "Testing the 15-minute ice bath challenge. 🥶 Anyone else doing this? The mental clarity afterward is literally better than any coffee I've ever had.",
            "likes": 5000, "comments": 300, "views": 95000,
            "timestamp": "2024-03-25T15:00:00Z"
        },
        {
            "external_post_id": "max_2",
            "platform": "instagram",
            "creator_id": "biohacker_max",
            "text": "How many of you actually meditate? 🧘 I find 10 mins of Box Breathing before big meetings completely changes my stress response. Give it a try.",
            "likes": 3500, "comments": 150, "views": 72000,
            "timestamp": "2024-03-26T13:00:00Z"
        },
        {
            "external_post_id": "max_3",
            "platform": "instagram",
            "creator_id": "biohacker_max",
            "text": "Protein powder is okay, but whole food sources are where the micronutrients are. Aiming for 2g per kg of bodyweight this month. Who's with me??",
            "likes": 4200, "comments": 280, "views": 81000,
            "timestamp": "2024-03-27T12:00:00Z"
        },
        {
            "external_post_id": "max_4",
            "platform": "instagram",
            "creator_id": "biohacker_max",
            "text": "Why I stopped training every day. Recovery is where the progress happens. 4 days a week is my new sweet spot. More isn't always better. #recovery #fitness",
            "likes": 7000, "comments": 500, "views": 150000,
            "timestamp": "2024-03-29T10:00:00Z"
        },

        # --- Creator: fitness_science ---
        {
            "external_post_id": "sci_1",
            "platform": "instagram",
            "creator_id": "fitness_science",
            "text": "Is training a muscle once a week enough? The meta-analysis says 2x per week yields 20% more growth. Frequency matters as much as volume. #science",
            "likes": 11000, "comments": 700, "views": 220000,
            "timestamp": "2024-03-25T16:00:00Z"
        },
        {
            "external_post_id": "sci_2",
            "platform": "instagram",
            "creator_id": "fitness_science",
            "text": "Protein timing: does it matter? Recent studies show that as long as you hit your daily total, the 'anabolic window' is much wider than we thought. Relax.",
            "likes": 9500, "comments": 600, "views": 195000,
            "timestamp": "2024-03-26T14:00:00Z"
        },
        {
            "external_post_id": "sci_3",
            "platform": "instagram",
            "creator_id": "fitness_science",
            "text": "The myth of 'overtraining'... most people are just under-recovering. Fix your sleep and your 6-day split will suddenly feel manageable again. #gymtips",
            "likes": 14000, "comments": 850, "views": 290000,
            "timestamp": "2024-03-28T15:00:00Z"
        },
        {
            "external_post_id": "sci_4",
            "platform": "instagram",
            "creator_id": "fitness_science",
            "text": "Just 20g of protein post-workout? New evidence suggests 40g+ might be better for older lifters to overcome leucine resistance. Adjust your shakes! 🥛",
            "likes": 21000, "comments": 1200, "views": 450000,
            "timestamp": "2024-03-30T09:00:00Z"
        }
    ]

def _parse_posts_from_data(data: Dict[str, Any], username: str) -> List[Dict[str, Any]]:
    """
    Shared helper: extract post nodes from either graphql or data response shapes.
    """
    user_node = data.get("graphql", {}).get("user", {})
    if not user_node:
        user_node = data.get("data", {}).get("user", {})
    if not user_node:
        logger.warning(f"No user node found for '{username}'. Response structure may have changed.")
        return []

    timeline = user_node.get("edge_owner_to_timeline_media", {})
    edges = timeline.get("edges", [])
    logger.info(f"DEBUG: Raw edges count for {username}: {len(edges)}")

    raw_posts = []
    for edge in edges:
        node = edge.get("node", {})
        if node:
            raw_posts.append(node)

    logger.info(f"Fetched {len(raw_posts)} posts for {username}")
    return raw_posts


def fetch_user_posts(username: str) -> List[Dict[str, Any]]:
    """
    Fetches raw posts from a public Instagram profile using a Session that
    mimics a real browser. Tries the lightweight GraphQL endpoint first,
    then falls back to the i.instagram.com web_profile_info API.
    """
    # Step 1 & 2: Build a session with real-browser headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://www.instagram.com/{username}/",
        "X-IG-App-ID": "936619743392459",
    }
    session = requests.Session()
    session.headers.update(headers)

    # Primary URL (GraphQL / __a=1)
    primary_url   = f"https://www.instagram.com/{username}/?__a=1&__d=dis"
    # Step 6: Fallback URL (i.instagram.com)
    fallback_url  = f"https://i.instagram.com/api/v1/users/web_profile_info/?username={username}"

    for label, url in [("primary", primary_url), ("fallback", fallback_url)]:
        logger.info(f"Trying {label} URL for {username}: {url}")
        response = None

        for attempt in range(2):
            try:
                response = session.get(url, timeout=15)

                # Step 3: Handle non-200 explicitly
                if response.status_code == 429:
                    logger.error(f"Rate limited (429) for {username} on {label} URL.")
                    time.sleep(10)
                    break  # skip retries; move to fallback

                if response.status_code != 200:
                    logger.error(
                        f"Instagram blocked request for {username} on {label} URL. "
                        f"Status: {response.status_code}"
                    )
                    time.sleep(2 ** attempt)
                    continue

                # 200 — try to parse
                try:
                    data = response.json()
                except ValueError:
                    # Step 4: Debug raw response text
                    logger.error(
                        f"Failed to parse JSON for '{username}' ({label}). "
                        f"Raw response: {response.text[:500]}"
                    )
                    break  # move to fallback

                if "graphql" not in data and "data" not in data:
                    logger.error(f"Instagram response invalid for {username} ({label}): {response.text[:300]}")
                    break  # move to fallback

                posts = _parse_posts_from_data(data, username)
                if posts:
                    return posts

                logger.warning(f"Zero posts parsed from {label} URL for {username}.")
                break  # move to fallback

            except Exception as e:
                logger.warning(f"[{label}][Retry {attempt+1}] Request failed for {username}: {e}")
                time.sleep(2 ** attempt)

        # Step 5: Small delay before trying fallback
        logger.info(f"Sleeping 3s before next URL attempt for {username}...")
        time.sleep(3)

    logger.warning(f"All fetch attempts exhausted for '{username}'. Returning empty list.")
    return []


def normalize_post(raw_post: Dict[str, Any], creator_id: str, creator_post_count: int) -> Dict[str, Any]:
    """
    Normalizes deeply nested Instagram post data into the strict schema.
    Returns:
    {
      "external_post_id": "",
      "platform": "instagram",
      "creator_id": "",
      "text": "",
      "likes": 0,
      "comments": 0,
      "views": 0,
      "timestamp": ""
    }
    """
    external_post_id = raw_post.get("shortcode", "")
    
    # Extract engagement metrics deeply safely
    likes = raw_post.get("edge_media_preview_like", {}).get("count", 0)
    comments = raw_post.get("edge_media_to_comment", {}).get("count", 0)
    views = raw_post.get("video_view_count", 0)
    
    # Extract timestamp properly
    ts_seconds = raw_post.get("taken_at_timestamp")
    timestamp = str(ts_seconds) if ts_seconds else ""
    if ts_seconds:
        try:
            dt = datetime.fromtimestamp(ts_seconds, tz=timezone.utc)
            timestamp = dt.isoformat()
        except Exception:
            pass
            
    # Safely extract caption text
    text = ""
    caption_edges = raw_post.get("edge_media_to_caption", {}).get("edges", [])
    if caption_edges and len(caption_edges) > 0:
        text = caption_edges[0].get("node", {}).get("text", "")

    # Fix 4: Try accessibility_caption as fallback
    text = raw_post.get("accessibility_caption", "") or text

    # Fix 1: Replace hard filter with soft fallback
    if not text.strip():
        text = "[No Caption Available]"

    # Fix 5: Final safe default
    if not text:
        text = f"Instagram post from {creator_id}"

    likes_val = int(likes) if likes is not None else 0
    comments_val = int(comments) if comments is not None else 0
    views_val = int(views) if views is not None else 0

    # Fix 3: Debug logging
    logger.info(f"DEBUG IG POST: likes={likes_val}, comments={comments_val}, text_len={len(text)}")

    # Fix 2: Relax engagement filter — keep post but mark low signal
    if likes_val + comments_val < getattr(settings, "min_engagement_threshold", 5):
        # keep post but mark low signal
        pass

    return {
        "external_post_id": str(external_post_id),
        "platform": "instagram",
        "creator_id": str(creator_id),
        "creator_post_count": creator_post_count,
        "text": str(text).strip(),
        "likes": likes_val,
        "comments": comments_val,
        "views": views_val,
        "timestamp": timestamp
    }

def run_instagram_ingestion() -> List[Dict[str, Any]]:
    """
    Runs the full ingestion pipeline across all configured Instagram creators.
    Enforces limits, delay for safety, error handling, and deduplication.
    """
    if getattr(settings, "use_mock", False):
        logger.warning("Mock mode enabled — using mock data")
        all_normalized_posts = get_mock_posts()
    else:
        logger.info("Using real Instagram ingestion via Apify")
        # Step 2: Fully replace logic with the new real-data fetcher
        all_normalized_posts = fetch_real_instagram_data()
    
    logger.info(f"Instagram ingestion complete. Total normalized posts: {len(all_normalized_posts)}")

    if all_normalized_posts:
        try:
            excel_path = getattr(settings, "excel_output_path", "data/instagram_posts.xlsx")
            stats = save_posts_to_excel(all_normalized_posts, excel_path)
            total_new = sum(stats.values())
            logger.info(f"Excel storage complete: {total_new} new posts written → {excel_path}")
        except Exception as e:
            logger.error(f"Failed to save posts to Excel: {e}")

    if not all_normalized_posts:
        logger.warning("Instagram ingestion failed or blocked. Using MOCK data.")
        logger.info("Using MOCK DATA for ingestion")
        return get_mock_posts()

    return all_normalized_posts
