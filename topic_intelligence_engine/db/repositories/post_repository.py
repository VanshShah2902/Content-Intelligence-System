from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from db.models import Post

class PostRepository:
    @staticmethod
    def get_recent_posts(db: Session, hours: int = 24):
        """Fetch posts within the last X hours."""
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return db.query(Post).filter(Post.created_at >= since).all()
        
    @staticmethod
    def get_by_id(db: Session, post_id: str):
        """Retrieve a post by its ID."""
        return db.query(Post).filter(Post.post_id == post_id).first()

    @staticmethod
    def create(db: Session, post_data: dict):
        """Create a new post record."""
        db_post = Post(
            post_id=post_data.get("post_id"),
            creator_id=post_data.get("creator_id"),
            caption=post_data.get("caption"),
            clean_text=post_data.get("clean_text"),
            post_url=post_data.get("post_url"),
            image_url=post_data.get("image_url"),
            created_at=post_data.get("created_at")
        )
        db.add(db_post)
        db.commit()
        db.refresh(db_post)
        return db_post
