from sqlalchemy.orm import Session
from db.models import PostMetrics

class MetricsRepository:
    @staticmethod
    def create(db: Session, metrics_data: dict):
        """Create a new metric record for a post (for time-series)."""
        db_metric = PostMetrics(
            post_id=metrics_data.get("post_id"),
            captured_at=metrics_data.get("captured_at"),
            likes=metrics_data.get("likes"),
            comments=metrics_data.get("comments"),
            views=metrics_data.get("views")
        )
        db.add(db_metric)
        db.commit()
        db.refresh(db_metric)
        return db_metric
