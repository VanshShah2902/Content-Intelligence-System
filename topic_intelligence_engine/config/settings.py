from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List

class Settings(BaseSettings):
    # Reddit API Credentials
    reddit_client_id: str = "YOUR_CLIENT_ID"
    reddit_client_secret: str = "YOUR_CLIENT_SECRET"
    reddit_user_agent: str = "TopicIntelligenceEngine/1.0 by Individual"
    
    # LLM Configuration (Groq)
    groq_api_key: str = ""
    llm_api_url: str = "https://api.groq.com/openai/v1/chat/completions"
    llm_model: str = "llama-3.1-8b-instant"
    
    # Ingestion Configuration
    target_subreddits: List[str] = ["nutrition", "fitness", "Supplements", "HealthyFood"]
    reddit_fetch_limit: int = 100
    instagram_usernames: List[str] = ["drhyman", "hubermanlab"]
    max_posts_per_user: int = 10
    
    # Processing Configuration
    min_engagement_threshold: int = 0
    momentum_time_decay_hours: float = 24.0
    top_x_weighted_posts: int = 50
    min_weighted_score_threshold: float = 0.0
    min_required_posts: int = 1
    
    # Clustering Configuration
    clustering_similarity_threshold: float = 0.65
    max_representative_posts: int = 5
    min_cluster_size: int = 1
    high_momentum_threshold: float = 50.0
    max_topic_variation: int = 3

    # Database Configuration (kept for pipeline/analysis modules that still use it)
    db_url: str = "postgresql://postgres:postgres@localhost:5432/topic_intelligence"
    use_mock: bool = False

    # Excel Storage
    excel_output_path: str = "data/instagram_posts.xlsx"

    # Apify Configuration
    apify_api_key: str = ""
    instagram_posts_limit: int = 500   # max posts to fetch per creator

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

settings = Settings()
