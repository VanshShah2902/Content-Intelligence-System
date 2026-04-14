from sqlalchemy.orm import Session
from db.models import Cluster
from datetime import date

class ClusterRepository:
    @staticmethod
    def create(db: Session, cluster_data: dict):
        """Create a new cluster record for daily snapshot tracking."""
        db_cluster = Cluster(
            topic_id=cluster_data.get("topic_id"),
            date=cluster_data.get("date", date.today()),
            avg_momentum=cluster_data.get("avg_momentum"),
            total_posts=cluster_data.get("total_posts"),
            trend_stage=cluster_data.get("trend_stage"),
            content_saturation_score=cluster_data.get("content_saturation_score"),
            opportunity_score=cluster_data.get("opportunity_score")
        )
        db.add(db_cluster)
        db.commit()
        db.refresh(db_cluster)
        return db_cluster
