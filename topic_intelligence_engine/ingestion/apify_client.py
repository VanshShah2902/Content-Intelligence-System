from apify_client import ApifyClient
import os
from dotenv import load_dotenv

# Ensure environment variables are loaded
load_dotenv()

APIFY_API_KEY = os.getenv("APIFY_API_KEY")

if not APIFY_API_KEY:
    # We allow the import but will fail during client usage if key is missing
    client = ApifyClient("")
else:
    client = ApifyClient(APIFY_API_KEY)

def fetch_instagram_posts(profile_url: str, limit: int = 80):
    """
    Fetches raw posts from a specific Instagram profile using the Apify SDK.
    """
    if not client.token:
        raise Exception("APIFY_API_KEY not set in environment or .env file.")

    run_input = {
        "directUrls": [profile_url],
        "resultsType": "posts",
        "resultsLimit": limit,
        "searchType": "hashtag",
        "searchLimit": 1,
        "addParentData": False,
    }

    # Call the Apify Actor
    run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)
    dataset_id = run["defaultDatasetId"]

    posts = []
    # Iterate through resulting dataset items
    for item in client.dataset(dataset_id).iterate_items():
        posts.append(item)

    return posts

def normalize_post(raw):
    """
    Maps raw Apify output keys to the system's internal post format.
    """
    return {
        "external_post_id": raw.get("id"),
        "platform": "instagram",
        "creator_id": raw.get("ownerUsername"),
        "text": raw.get("caption", ""),
        "likes": raw.get("likesCount", 0),
        "comments": raw.get("commentsCount", 0),
        "views": raw.get("videoViewCount", 0),
        "timestamp": raw.get("timestamp"),
        "post_url": raw.get("url"),
        "image_url": raw.get("displayUrl"),
        "video_url": raw.get("videoUrl"),
        "audio_url": raw.get("musicInfo", {}).get("artistUrl") if isinstance(raw.get("musicInfo"), dict) else None,
        "video_duration": raw.get("videoDuration"),
    }
